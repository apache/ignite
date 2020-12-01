/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase.computeInlineSize;
import static org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase.getAvailableInlineColumns;
import static org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.AbstractInlineIndexColumn.CANT_BE_COMPARE;

/**
 * H2 tree index implementation.
 */
public class H2Tree extends BPlusTree<H2Row, H2Row> {
    /** */
    public static final String IGNITE_THROTTLE_INLINE_SIZE_CALCULATION = "IGNITE_THROTTLE_INLINE_SIZE_CALCULATION";

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Owning table. */
    private final GridH2Table table;

    /** */
    private final int inlineSize;

    /** List of helpers to work with inline values on the page. */
    private final List<InlineIndexColumn> inlineIdxs;

    /** Actual columns that current index is consist from. */
    private final IndexColumn[] cols;

    /**
     * Columns that will be used for inlining.
     * Could differ from actual columns {@link #cols} in case of
     * meta page were upgraded from older version.
     */
    private final IndexColumn[] inlineCols;

    /** */
    private final boolean mvccEnabled;

    /** */
    private final boolean pk;

    /** */
    private final boolean affinityKey;

    /** */
    private final String cacheName;

    /** */
    private final String tblName;

    /** */
    private final String idxName;

    /** */
    private final IoStatisticsHolder stats;

    /** */
    private final Comparator<Value> comp = this::compareValues;

    /** Row cache. */
    private final H2RowCache rowCache;

    /** How often real invocation of inline size calculation will be skipped. */
    private final int inlineSizeThrottleThreshold =
        IgniteSystemProperties.getInteger(IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, 1_000);

    /** Counter of inline size calculation for throttling real invocations. */
    private final ThreadLocal<Long> inlineSizeCalculationCntr = ThreadLocal.withInitial(() -> 0L);

    /** Keep max calculated inline size for current index. */
    private final AtomicInteger maxCalculatedInlineSize;

    /** */
    private final IgniteLogger log;

    /** Whether PK is stored in unwrapped form. */
    private final boolean unwrappedPk;

    /** Whether index was created from scratch during owning node lifecycle. */
    private final boolean created;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param table Owning table.
     * @param name Name of the tree.
     * @param idxName Name of the index.
     * @param cacheName Name of the cache.
     * @param tblName Name of the table.
     * @param reuseList Reuse list.
     * @param grpId Cache group ID.
     * @param grpName Name of the cache group.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param globalRmvId Global remove ID counter.
     * @param metaPageId Meta page ID.
     * @param initNew if {@code true} new tree will be initialized,
     * else meta page info will be read.
     * @param unwrappedCols Unwrapped indexed columns.
     * @param wrappedCols Original indexed columns.
     * @param maxCalculatedInlineSize Keep max calculated inline size
     * for current index.
     * @param pk {@code true} for primary key.
     * @param affinityKey {@code true} for affinity key.
     * @param mvccEnabled Mvcc flag.
     * @param rowCache Row cache.
     * @param failureProcessor if the tree is corrupted.
     * @param log Logger.
     * @param stats Statistics holder.
     * @param factory Inline helper factory.
     * @param configuredInlineSize Size that has been set by user during
     * index creation.
     * @throws IgniteCheckedException If failed.
     */
    public H2Tree(
        GridCacheContext cctx,
        GridH2Table table,
        String name,
        String idxName,
        String cacheName,
        String tblName,
        ReuseList reuseList,
        int grpId,
        String grpName,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        boolean initNew,
        List<IndexColumn> unwrappedCols,
        List<IndexColumn> wrappedCols,
        AtomicInteger maxCalculatedInlineSize,
        boolean pk,
        boolean affinityKey,
        boolean mvccEnabled,
        @Nullable H2RowCache rowCache,
        @Nullable FailureProcessor failureProcessor,
        IgniteLogger log,
        IoStatisticsHolder stats,
        InlineIndexColumnFactory factory,
        int configuredInlineSize
    ) throws IgniteCheckedException {
        super(
            name,
            grpId,
            grpName,
            pageMem,
            wal,
            globalRmvId,
            metaPageId,
            reuseList,
            failureProcessor,
            null
        );

        this.cctx = cctx;
        this.table = table;
        this.stats = stats;
        this.log = log;
        this.rowCache = rowCache;
        this.idxName = idxName;
        this.cacheName = cacheName;
        this.tblName = tblName;
        this.maxCalculatedInlineSize = maxCalculatedInlineSize;
        this.pk = pk;
        this.affinityKey = affinityKey;
        this.mvccEnabled = mvccEnabled;

        if (!initNew) {
            // Page is ready - read meta information.
            MetaPageInfo metaInfo = getMetaInfo();

            unwrappedPk = metaInfo.useUnwrappedPk();

            cols = (unwrappedPk ? unwrappedCols : wrappedCols).toArray(H2Utils.EMPTY_COLUMNS);

            inlineSize = metaInfo.inlineSize();

            List<InlineIndexColumn> inlineIdxs0 = getAvailableInlineColumns(affinityKey, cacheName, idxName, log, pk,
                table, cols, factory, metaInfo.inlineObjectHash());

            boolean inlineObjSupported = inlineSize > 0 && metaInfo.inlineObjectSupported();

            inlineIdxs = inlineObjSupported ? inlineIdxs0 : inlineIdxs0.stream()
                .filter(ih -> ih.type() != Value.JAVA_OBJECT)
                .collect(Collectors.toList());

            inlineCols = new IndexColumn[inlineIdxs.size()];

            for (int i = 0, j = 0; i < cols.length && j < inlineIdxs.size(); i++) {
                if (cols[i].column.getColumnId() == inlineIdxs.get(j).columnIndex())
                    inlineCols[j++] = cols[i];
            }

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);

            setIos(
                H2ExtrasInnerIO.getVersions(inlineSize, mvccEnabled),
                H2ExtrasLeafIO.getVersions(inlineSize, mvccEnabled)
            );
        }
        else {
            unwrappedPk = true;

            cols = unwrappedCols.toArray(H2Utils.EMPTY_COLUMNS);
            inlineCols = cols;

            inlineIdxs = getAvailableInlineColumns(affinityKey, cacheName, idxName, log, pk, table, cols, factory, true);

            inlineSize = computeInlineSize(inlineIdxs, configuredInlineSize, cctx.config().getSqlIndexMaxInlineSize());

            setIos(
                H2ExtrasInnerIO.getVersions(inlineSize, mvccEnabled),
                H2ExtrasLeafIO.getVersions(inlineSize, mvccEnabled)
            );

            initTree(true, inlineSize);
        }

        created = initNew;
    }

    /**
     * Return columns of the index.
     *
     * @return Indexed columns.
     */
    IndexColumn[] cols() {
        return cols;
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public H2Row createRow(long link) throws IgniteCheckedException {
        if (rowCache != null) {
            H2CacheRow row = rowCache.get(link);

            if (row == null) {
                row = createRow0(link);

                rowCache.put(row);
            }

            return row;
        }
        else
            return createRow0(link);
    }

    /**
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    private H2CacheRow createRow0(long link) throws IgniteCheckedException {
        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        row.initFromLink(
            cctx.group(),
            CacheDataRowAdapter.RowData.FULL,
            true
        );

        return table.rowDescriptor().createRow(row);
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @param mvccOpCntr MVCC operation counter.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public H2Row createMvccRow(long link, long mvccCrdVer, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        if (rowCache != null) {
            H2CacheRow row = rowCache.get(link);

            if (row == null) {
                row = createMvccRow0(link, mvccCrdVer, mvccCntr, mvccOpCntr);

                rowCache.put(row);
            }

            return row;
        }
        else
            return createMvccRow0(link, mvccCrdVer, mvccCntr, mvccOpCntr);
    }

    /**
     * @param link Link.
     * @param mvccCrdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return Row.
     */
    private H2CacheRow createMvccRow0(long link, long mvccCrdVer, long mvccCntr, int mvccOpCntr)
        throws IgniteCheckedException {
        int partId = PageIdUtils.partId(PageIdUtils.pageId(link));

        MvccDataRow row = new MvccDataRow(
            cctx.group(),
            0,
            link,
            partId,
            null,
            mvccCrdVer,
            mvccCntr,
            mvccOpCntr,
            true
        );

        return table.rowDescriptor().createRow(row);
    }

    /** {@inheritDoc} */
    @Override public H2Row getRow(BPlusIO<H2Row> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {
        return io.getLookupRow(this, pageAddr, idx);
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    private MetaPageInfo getMetaInfo() throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = readLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return new MetaPageInfo(io, pageAddr);
            }
            finally {
                readUnlock(metaPageId, metaPage, pageAddr);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * Update root meta page if need (previous version not supported features flags
     * and created product version on root meta page).
     *
     * @param inlineObjSupported inline POJO by created tree flag.
     * @throws IgniteCheckedException On error.
     */
    private void upgradeMetaPage(boolean inlineObjSupported) throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = writeLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.upgradePageVersion(pageAddr, inlineObjSupported, false, pageSize());

                if (wal != null)
                    wal.log(new PageSnapshot(new FullPageId(metaPageId, grpId),
                        pageAddr, pageMem.pageSize(), pageMem.realPageSize(grpId)));
            }
            finally {
                writeUnlock(metaPageId, metaPage, pageAddr, true);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override protected int compare(BPlusIO<H2Row> io, long pageAddr, int idx,
        H2Row row) throws IgniteCheckedException {
        try {
            if (inlineSize() == 0)
                return compareRows(getRow(io, pageAddr, idx), row);
            else {
                int off = io.offset(idx);

                int fieldOff = 0;

                int lastIdxUsed = 0;

                for (int i = 0; i < inlineIdxs.size(); i++) {
                    InlineIndexColumn inlineIdx = inlineIdxs.get(i);
                    
                    Value v2 = row.getValue(inlineIdx.columnIndex());

                    if (v2 == null)
                        return 0;

                    int c = inlineIdx.compare(pageAddr, off + fieldOff, inlineSize() - fieldOff, v2, comp);

                    if (c == CANT_BE_COMPARE)
                        break;

                    lastIdxUsed++;

                    if (c != 0)
                        return fixSort(c, inlineCols[i].sortType);

                    fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                    if (fieldOff > inlineSize())
                        break;
                }

                if (lastIdxUsed == cols.length)
                    return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);

                inlineSizeRecomendation(row);

                SearchRow rowData = getRow(io, pageAddr, idx);

                for (int i = lastIdxUsed, len = cols.length; i < len; i++) {
                    IndexColumn col = cols[i];
                    int idx0 = col.column.getColumnId();

                    Value v2 = row.getValue(idx0);

                    if (v2 == null) {
                        // Can't compare further.
                        return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);
                    }

                    Value v1 = rowData.getValue(idx0);

                    int c = compareValues(v1, v2);

                    if (c != 0)
                        return fixSort(c, col.sortType);
                }

                return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);
            }
        }
        catch (DbException ex) {
            throw new IgniteCheckedException("Rows cannot be compared", ex);
        }
    }

    /**
     * Perform sort order correction.
     *
     * @param c Compare result.
     * @param sortType Sort type.
     * @return Fixed compare result.
     */
    private static int fixSort(int c, int sortType) {
        return sortType == SortOrder.ASCENDING ? c : -c;
    }

    /**
     * Compares two H2 rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result: see {@link Comparator#compare(Object, Object)} for values.
     */
    public int compareRows(H2Row r1, H2Row r2) {
        assert !mvccEnabled || r2.indexSearchRow() || MvccUtils.mvccVersionIsValid(r2.mvccCoordinatorVersion(), r2.mvccCounter()) : r2;
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            IndexColumn idxCol = cols[i];

            int idx = idxCol.column.getColumnId();

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null || v2 == null) {
                // Can't compare further.
                return mvccCompare(r1, r2);
            }

            int c = compareValues(v1, v2);

            if (c != 0)
                return fixSort(c, idxCol.sortType);
        }

        return mvccCompare(r1, r2);
    }

    /**
     * Checks both rows are the same. <p/>
     * Primarly used to verify both search rows are the same and we can apply
     * the single row lookup optimization.
     *
     * @param r1 The first row.
     * @param r2 Another row.
     * @return {@code true} in case both rows are efficiently the same, {@code false} otherwise.
     */
    boolean checkRowsTheSame(H2Row r1, H2Row r2) {
        if (r1 == r2)
            return true;

        for (int i = 0, len = cols.length; i < len; i++) {
            IndexColumn idxCol = cols[i];

            int idx = idxCol.column.getColumnId();

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null && v2 == null)
                continue;

            if (!(v1 != null && v2 != null))
                return false;

            if (compareValues(v1, v2) != 0)
                return false;
        }

        return true;
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param r2 Search row.
     * @return Comparison result.
     */
    private int mvccCompare(H2RowLinkIO io, long pageAddr, int idx, H2Row r2) {
        if (!mvccEnabled || r2.indexSearchRow())
            return 0;

        long crd = io.getMvccCoordinatorVersion(pageAddr, idx);
        long cntr = io.getMvccCounter(pageAddr, idx);
        int opCntr = io.getMvccOperationCounter(pageAddr, idx);

        assert MvccUtils.mvccVersionIsValid(crd, cntr, opCntr);

        return -MvccUtils.compare(crd, cntr, opCntr, r2);  // descending order
    }

    /**
     * @param r1 First row.
     * @param r2 Second row.
     * @return Comparison result.
     */
    private int mvccCompare(H2Row r1, H2Row r2) {
        if (!mvccEnabled || r2.indexSearchRow())
            return 0;

        long crdVer1 = r1.mvccCoordinatorVersion();
        long crdVer2 = r2.mvccCoordinatorVersion();

        int c = -Long.compare(crdVer1, crdVer2);

        if (c != 0)
            return c;

        return -Long.compare(r1.mvccCounter(), r2.mvccCounter());
    }

    /**
     * Calculate aggregate inline size for given indexes and log recommendation in case calculated size more than
     * current inline size.
     *
     * @param row Grid H2 row related to given inline indexes.
     */
    @SuppressWarnings({"ConditionalBreakInInfiniteLoop", "IfMayBeConditional"})
    private void inlineSizeRecomendation(SearchRow row) {
        //Do the check only for put operations.
        if (!(row instanceof H2CacheRow))
            return;

        Long invokeCnt = inlineSizeCalculationCntr.get();

        inlineSizeCalculationCntr.set(++invokeCnt);

        boolean throttle = invokeCnt % inlineSizeThrottleThreshold != 0;

        if (throttle)
            return;

        int newSize = 0;

        InlineIndexColumn idx;

        List<String> colNames = new ArrayList<>();

        for (InlineIndexColumn index : inlineIdxs) {
            idx = index;

            newSize += idx.inlineSizeOf(row.getValue(idx.columnIndex()));

            colNames.add(index.columnName());
        }

        if (newSize > inlineSize()) {
            int oldSize;

            while (true) {
                oldSize = maxCalculatedInlineSize.get();

                if (oldSize >= newSize)
                    return;

                if (maxCalculatedInlineSize.compareAndSet(oldSize, newSize))
                    break;
            }

            String cols = colNames.stream().collect(Collectors.joining(", ", "(", ")"));

            String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

            String recommendation;

            if (pk || affinityKey) {
                recommendation = "set system property "
                    + IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE + " with recommended size " +
                    "(be aware it will be used by default for all indexes without explicit inline size)";
            }
            else {
                recommendation = "use INLINE_SIZE option for CREATE INDEX command, " +
                    "QuerySqlField.inlineSize for annotated classes, or QueryIndex.inlineSize for explicit " +
                    "QueryEntity configuration";
            }

            String warn = "Indexed columns of a row cannot be fully inlined into index " +
                "what may lead to slowdown due to additional data page reads, increase index inline size if needed " +
                "(" + recommendation + ") " +
                "[cacheName=" + cacheName +
                ", tableName=" + tblName +
                ", idxName=" + idxName +
                ", idxCols=" + cols +
                ", idxType=" + idxType +
                ", curSize=" + inlineSize() +
                ", recommendedInlineSize=" + newSize + "]";

            U.warn(log, warn);
        }
    }

    /** {@inheritDoc} */
    @Override protected IoStatisticsHolder statisticsHolder() {
        return stats;
    }

    /**
     * @return {@code true} In case use unwrapped columns for PK
     */
    public boolean unwrappedPk() {
        return unwrappedPk;
    }

    /**
     * @return Inline indexes for the segment.
     */
    public List<InlineIndexColumn> inlineIndexes() {
        return inlineIdxs;
    }

    /**
     *
     */
    public static class MetaPageInfo {
        /** */
        int inlineSize;

        /** */
        boolean useUnwrappedPk;

        /** */
        boolean flagsSupported;

        /** */
        boolean inlineObjSupported;

        /** */
        boolean inlineObjHash;

        /** */
        IgniteProductVersion createdVer;

        /**
         * @param io Metapage IO.
         * @param pageAddr Page address.
         */
        public MetaPageInfo(BPlusMetaIO io, long pageAddr) {
            inlineSize = io.getInlineSize(pageAddr);
            useUnwrappedPk = io.unwrappedPk(pageAddr);
            flagsSupported = io.supportFlags();

            if (flagsSupported) {
                inlineObjSupported = io.inlineObjectSupported(pageAddr);
                inlineObjHash = io.inlineObjectHash(pageAddr);
            }

            createdVer = io.createdVersion(pageAddr);
        }

        /**
         * @return Inline size.
         */
        public int inlineSize() {
            return inlineSize;
        }

        /**
         * @return {@code true} In case use unwrapped PK for indexes.
         */
        public boolean useUnwrappedPk() {
            return useUnwrappedPk;
        }

        /**
         * @return {@code true} In case metapage contains flags.
         */
        public boolean flagsSupported() {
            return flagsSupported;
        }

        /**
         * @return {@code true} In case inline object is supported.
         */
        public boolean inlineObjectSupported() {
            return inlineObjSupported;
        }

        /**
         * @return {@code true} In case inline object is supported.
         */
        public boolean inlineObjectHash() {
            return inlineObjHash;
        }
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public int compareValues(Value v1, Value v2) {
        return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);
    }

    /**
     * @return {@code True} if index was created during curren node's lifetime, {@code False} if it was restored from
     * disk.
     */
    public boolean created() {
        return created;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2Tree.class, this, "super", super.toString());
    }

    /**
     * Construct the exception and invoke failure processor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return New CorruptedTreeException instance.
     */
    @Override protected CorruptedTreeException corruptedTreeException(String msg, Throwable cause, int grpId, long... pageIds) {
        CorruptedTreeException e = new CorruptedTreeException(msg, cause, grpId, grpName, cacheName, idxName, pageIds);

        processFailure(FailureType.CRITICAL_ERROR, e);

        return e;
    }

    /** {@inheritDoc} */
    @Override protected void temporaryReleaseLock() {
        cctx.kernalContext().cache().context().database().checkpointReadUnlock();
        cctx.kernalContext().cache().context().database().checkpointReadLock();
    }

    /** {@inheritDoc} */
    @Override protected long maxLockHoldTime() {
        long sysWorkerBlockedTimeout = cctx.kernalContext().workersRegistry().getSystemWorkerBlockedTimeout();

        // Using timeout value reduced by 10 times to increase possibility of lock releasing before timeout.
        return sysWorkerBlockedTimeout == 0 ? Long.MAX_VALUE : (sysWorkerBlockedTimeout / 10);
    }
}

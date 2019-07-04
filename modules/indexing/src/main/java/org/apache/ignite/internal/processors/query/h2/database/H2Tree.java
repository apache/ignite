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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.stat.IoStatisticsHolder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 */
public abstract class H2Tree extends BPlusTree<SearchRow, GridH2Row> {
    /** */
    private final H2RowFactory rowStore;

    /** */
    private final int inlineSize;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /** */
    private final IndexColumn[] cols;

    /** */
    private final int[] columnIds;

    /** */
    private final IoStatisticsHolder stats;

    /** */
    private final Comparator<Value> comp = new Comparator<Value>() {
        @Override public int compare(Value o1, Value o2) {
            return compareValues(o1, o2);
        }
    };

    /** Row cache. */
    private final H2RowCache rowCache;

    /** Whether index was created from scratch during owning node lifecycle. */
    private final boolean created;

    /**
     * Constructor.
     *
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param grpId Cache group ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @param rowCache Row cache.
     * @param failureProcessor if the tree is corrupted.
     * @param stats Statistics holder.
     * @throws IgniteCheckedException If failed.
     */
    protected H2Tree(
        String name,
        ReuseList reuseList,
        int grpId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        H2RowFactory rowStore,
        long metaPageId,
        boolean initNew,
        IndexColumn[] cols,
        List<InlineIndexHelper> inlineIdxs,
        int inlineSize,
        @Nullable H2RowCache rowCache,
        @Nullable FailureProcessor failureProcessor,
        IoStatisticsHolder stats
    ) throws IgniteCheckedException {
        super(
            name,
            grpId,
            pageMem,
            wal,
            globalRmvId,
            metaPageId,
            reuseList,
            failureProcessor,
            null
        );

        this.stats = stats;
        this.rowCache = rowCache;

        this.rowStore = rowStore;
        this.cols = cols;

        this.columnIds = new int[cols.length];

        for (int i = 0; i < cols.length; i++)
            columnIds[i] = cols[i].column.getColumnId();

        if (!initNew) {
            // Page is ready - read meta information.
            MetaPageInfo metaInfo = getMetaInfo();

            if (metaInfo.useUnwrappedPk())
                throw new IgniteCheckedException("Unwrapped PK is not supported by current version");

            this.inlineSize = metaInfo.inlineSize();

            setIos(
                H2ExtrasInnerIO.getVersions(inlineSize),
                H2ExtrasLeafIO.getVersions(inlineSize));

            boolean inlineObjSupported = inlineSize > 0 && inlineObjectSupported(metaInfo, inlineIdxs);

            this.inlineIdxs = inlineObjSupported ? inlineIdxs : inlineIdxs.stream()
                .filter(ih -> ih.type() != Value.JAVA_OBJECT)
                .collect(Collectors.toList());

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);
        }
        else {
            this.inlineSize = inlineSize;

            this.inlineIdxs = inlineIdxs;

            setIos(
                H2ExtrasInnerIO.getVersions(inlineSize),
                H2ExtrasLeafIO.getVersions(inlineSize));

            initTree(initNew, inlineSize);
        }

        created = initNew;
    }

    /**
     * @param metaInfo Metapage info.
     * @param inlineIdxs Base collection of index helpers.
     * @return {@code true} if inline object is supported by exists tree.
     */
    private boolean inlineObjectSupported(MetaPageInfo metaInfo, List<InlineIndexHelper> inlineIdxs) {
        if (metaInfo.flagsSupported())
            return metaInfo.inlineObjectSupported();
        else {
            try {
                if (H2TreeInlineObjectDetector.objectMayBeInlined(inlineSize, inlineIdxs)) {
                    H2TreeInlineObjectDetector inlineObjDetector = new H2TreeInlineObjectDetector(
                        inlineSize, inlineIdxs);

                    findFirst(inlineObjDetector);

                    return inlineObjDetector.inlineObjectSupported();
                }
                else
                    return false;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Unexpected exception on detect inline object", e);
            }
        }
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public GridH2Row createRowFromLink(long link) throws IgniteCheckedException {
        if (rowCache != null) {
            GridH2Row row = rowCache.get(link);

            if (row == null) {
                row = rowStore.getRow(link);

                if (row instanceof GridH2KeyValueRowOnheap)
                    rowCache.put((GridH2KeyValueRowOnheap)row);
            }

            return row;
        }
        else
            return rowStore.getRow(link);
    }

    /** {@inheritDoc} */
    @Override protected GridH2Row getRow(BPlusIO<SearchRow> io, long pageAddr, int idx, Object filter)
        throws IgniteCheckedException {
        if (filter != null) {
            // Filter out not interesting partitions without deserializing the row.
            IndexingQueryCacheFilter filter0 = (IndexingQueryCacheFilter)filter;

            long link = ((H2RowLinkIO)io).getLink(pageAddr, idx);

            int part = PageIdUtils.partId(PageIdUtils.pageId(link));

            if (!filter0.applyPartition(part))
                return null;
        }

        return (GridH2Row)io.getLookupRow(this, pageAddr, idx);
    }

    /**
     * @return Inline size.
     */
    private int inlineSize() {
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
                        pageAddr, pageMem.pageSize()));
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
    @Override protected int compare(BPlusIO<SearchRow> io, long pageAddr, int idx,
        SearchRow row) throws IgniteCheckedException {
        if (inlineSize() == 0)
            return compareRows(getRow(io, pageAddr, idx), row);
        else {
            int off = io.offset(idx);

            int fieldOff = 0;

            int lastIdxUsed = 0;

            for (int i = 0; i < inlineIdxs.size(); i++) {
                InlineIndexHelper inlineIdx = inlineIdxs.get(i);

                Value v2 = row.getValue(inlineIdx.columnIndex());

                if (v2 == null)
                    return 0;

                int c = inlineIdx.compare(pageAddr, off + fieldOff, inlineSize() - fieldOff, v2, comp);

                if (c == -2)
                    break;

                lastIdxUsed++;

                if (c != 0)
                    return c;

                fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                if (fieldOff > inlineSize())
                    break;
            }

            if (lastIdxUsed == cols.length)
                return 0;

            SearchRow rowData = getRow(io, pageAddr, idx);

            for (int i = lastIdxUsed, len = cols.length; i < len; i++) {
                IndexColumn col = cols[i];
                int idx0 = col.column.getColumnId();

                Value v2 = row.getValue(idx0);

                if (v2 == null) {
                    // Can't compare further.
                    return 0;
                }

                Value v1 = rowData.getValue(idx0);

                int c = compareValues(v1, v2);

                if (c != 0)
                    return InlineIndexHelper.fixSort(c, col.sortType);
            }

            return 0;
        }
    }

    /**
     * Compares two H2 rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result: see {@link Comparator#compare(Object, Object)} for values.
     */
    public int compareRows(SearchRow r1, SearchRow r2) {
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            int idx = columnIds[i];

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null || v2 == null) {
                // Can't compare further.
                return 0;
            }

            int c = compareValues(v1, v2);

            if (c != 0)
                return InlineIndexHelper.fixSort(c, cols[i].sortType);
        }

        return 0;
    }

    /** {@inheritDoc} */
    @Override protected IoStatisticsHolder statisticsHolder() {
        return stats;
    }

    /**
     * @return Inline indexes for the segment.
     */
    public List<InlineIndexHelper> inlineIndexes() {
        return inlineIdxs;
    }

    /**
     * @param idxs Full set of inline helpers.
     */
    public void refreshColumnIds(List<InlineIndexHelper> idxs) {
        assert inlineIdxs.size() <= idxs.size();

        for (int i = 0; i < inlineIdxs.size(); ++i) {
            final int idx = i;

            inlineIdxs.set(idx, F.find(idxs, null,
                (IgnitePredicate<InlineIndexHelper>)ih -> ih.colName().equals(inlineIdxs.get(idx).colName())));

            assert inlineIdxs.get(idx) != null;
        }
    }

    /**
     *
     */
    private static class MetaPageInfo {
        /** */
        int inlineSize;

        /** */
        boolean useUnwrappedPk;

        /** */
        boolean flagsSupported;

        /** */
        Boolean inlineObjectSupported;

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

            if (flagsSupported)
                inlineObjectSupported = io.inlineObjectSupported(pageAddr);

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
            return inlineObjectSupported;
        }
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public abstract int compareValues(Value v1, Value v2);

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
}

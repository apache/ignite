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
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.DurableBackgroundCleanupIndexTreeTask;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;

/**
 * H2 Index over {@link BPlusTree}.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class H2TreeIndex extends GridH2IndexBase {
    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;

    /** */
    private final H2Tree[] segments;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Tree name. */
    private final String treeName;

    /** Override it for test purposes. */
    public static H2TreeFactory h2TreeFactory = (
        cctx,
        name,
        tblName,
        table,
        cacheName,
        idxName,
        reuseList,
        grpId,
        grpName,
        pageMem,
        wal,
        globalRmvId,
        rowStore,
        metaPageId,
        initNew,
        cols,
        inlineIdxs,
        inlineSize,
        pk,
        affinityKey,
        rowCache,
        failureProcessor,
        log
        ) -> new H2Tree(
            cctx,
            name,
            tblName,
            cacheName,
            idxName,
            reuseList,
            grpId,
            grpName,
            pageMem,
            wal,
            globalRmvId,
            rowStore,
            metaPageId,
            initNew,
            cols,
            inlineIdxs,
            inlineSize,
            pk,
            affinityKey,
            rowCache,
            failureProcessor,
            log
        ) {
            @Override public int compareValues(Value v1, Value v2) {
                return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);
            }
    };

    /**
     * @param cctx Cache context.
     * @param rowCache Row cache.
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param affinityKey {@code true} for affinity key.
     * @param colsList Index columns.
     * @param inlineSize Inline size.
     * @param segmentsCnt number of tree's segments.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    public H2TreeIndex(
        GridCacheContext<?, ?> cctx,
        @Nullable H2RowCache rowCache,
        GridH2Table tbl,
        String name,
        boolean pk,
        boolean affinityKey,
        List<IndexColumn> colsList,
        int inlineSize,
        int segmentsCnt,
        IgniteLogger log
    ) throws IgniteCheckedException {
        super(tbl);

        assert segmentsCnt > 0 : segmentsCnt;

        this.cctx = cctx;

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        treeName = BPlusTree.treeName((tbl.rowDescriptor() == null ? "" : typeId + "_") + name, "H2Tree");

        if (cctx.affinityNode()) {
            inlineIdxs = getAvailableInlineColumns(cols);

            segments = new H2Tree[segmentsCnt];

            IgniteCacheDatabaseSharedManager db = cctx.shared().database();

            for (int i = 0; i < segments.length; i++) {
                db.checkpointReadLock();

            try {
                RootPage page = getMetaPage(i);

                    segments[i] = h2TreeFactory.create(
                        cctx,
                        name,
                        tbl.getName(),
                        tbl,
                        tbl.cacheName(),
                        name,
                        cctx.offheap().reuseListForIndex(name),
                        cctx.groupId(),
                        cctx.group().name(),
                        cctx.dataRegion().pageMemory(),
                        cctx.shared().wal(),
                        cctx.offheap().globalRemoveId(),
                        tbl.rowFactory(),
                        page.pageId().pageId(),
                        page.isAllocated(),
                        cols,
                        inlineIdxs,
                        computeInlineSize(inlineIdxs, inlineSize),
                        pk,
                        affinityKey,
                        rowCache,
                        cctx.kernalContext().failure(),
                        log);
                }
                finally {
                    db.checkpointReadUnlock();
                }
            }
        }
        else {
            // We need indexes on the client node, but index will not contain any data.
            segments = null;
            inlineIdxs = null;
        }

        initDistributedJoinMessaging(tbl);
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return segments[0].inlineSize();
    }

    /**
     * Check if index exists in store.
     *
     * @return {@code True} if exists.
     */
    public boolean rebuildRequired() {
        assert segments != null;

        for (int i = 0; i < segments.length; i++) {
            try {
                H2Tree segment = segments[i];

                if (segment.created())
                    return true;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to check index tree root page existence [cacheName=" + cctx.name() +
                    ", segment=" + i + ']');
            }
        }

        return false;
    }

    /**
     * @param cols Columns array.
     * @return List of {@link InlineIndexHelper} objects.
     */
    private List<InlineIndexHelper> getAvailableInlineColumns(IndexColumn[] cols) {
        List<InlineIndexHelper> res = new ArrayList<>();

        for (IndexColumn col : cols) {
            if (!InlineIndexHelper.AVAILABLE_TYPES.contains(col.column.getType()))
                break;

            InlineIndexHelper idx = new InlineIndexHelper(
                col.column.getName(),
                col.column.getType(),
                col.column.getColumnId(),
                col.sortType,
                table.getCompareMode());

            res.add(idx);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected int segmentsCount() {
        return segments.length;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        try {
            IndexingQueryCacheFilter filter = partitionFilter(threadLocalFilter());

            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            if (indexType.isPrimaryKey() && lower != null && upper != null && tree.compareRows(lower, upper) == 0) {
                GridH2Row row = tree.findOne(lower, filter);

                return (row == null) ? EMPTY_CURSOR : new SingleRowCursor(row);
            }
            else {
                GridCursor<GridH2Row> cursor = tree.find(lower, upper, filter);

                registerCursor(cursor);

                return new H2Cursor(cursor);
            }
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     *  @param cursor Cursor to be closed after the query execution.
     */
    private void registerCursor(GridCursor<GridH2Row> cursor) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        if(qctx != null)
            qctx.addResource(cursor);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.put(row);
        }
        catch (Throwable t) {
            ctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        try {
            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            InlineIndexHelper.setCurrentInlineIndexes(tree.inlineIndexes());

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.putx(row);
        }
        catch (Throwable t) {
            ctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.remove(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.removex(row);
        }
        catch (Throwable t) {
            ctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = costRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        try {
            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            BPlusTree.TreeRowClosure<SearchRow, GridH2Row> filter = filterClosure();

            return tree.size(filter);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        try {
            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            GridH2Row row = b ? tree.findFirst(): tree.findLast();

            return new SingleRowCursor(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean rmvIdx) {
        destroy0(rmvIdx, false);
    }

    /** {@inheritDoc} */
    @Override public void asyncDestroy(boolean rmvIdx) {
        destroy0(rmvIdx, true);
    }

    /**
     * Internal method for destroying index with async option.
     *
     * @param rmvIdx Flag remove.
     * @param async Destroy asynchronously.
     */
    private void destroy0(boolean rmvIdx, boolean async) {
        try {
            if (cctx.affinityNode() && rmvIdx) {
                assert cctx.shared().database().checkpointLockIsHeldByThread();

                List<Long> rootPages = new ArrayList<>(segments.length);
                List<H2Tree> trees = new ArrayList<>(segments.length);

                for (int i = 0; i < segments.length; i++) {
                    H2Tree tree = segments[i];

                    if (async) {
                        tree.markDestroyed();

                        rootPages.add(tree.getMetaPageId());
                        trees.add(tree);
                    }
                    else
                        tree.destroy();

                    dropMetaPage(i);
                }

                if (async) {
                    DurableBackgroundTask task = new DurableBackgroundCleanupIndexTreeTask(
                        rootPages,
                        trees,
                        cctx.group().name(),
                        cctx.cache().name(),
                        table.getSchema().getName(),
                        getName()
                    );

                    cctx.kernalContext().durableBackgroundTasksProcessor().startDurableBackgroundTask(task, cctx.config());
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            super.destroy(rmvIdx);
        }
    }

    /** {@inheritDoc} */
    @Override protected H2Tree treeForRead(int segment) {
        return segments[segment];
    }

    /** {@inheritDoc} */
    @Override protected H2Cursor doFind0(
        IgniteTree t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last,
        IndexingQueryFilter filter) {
        try {
            IndexingQueryCacheFilter pf = partitionFilter(filter);

            GridCursor<GridH2Row> range = t.find(first, last, pf);

            if (range == null)
                range = GridCursor.EMPTY_CURSOR;

            return new H2Cursor(range);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from cache config.
     * @return Inline size.
     */
    private int computeInlineSize(List<InlineIndexHelper> inlineIdxs, int cfgInlineSize) {
        int confSize = cctx.config().getSqlIndexMaxInlineSize();

        int propSize = confSize == -1 ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE,
            IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT) : confSize;

        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        if (cfgInlineSize == -1) {
            if (propSize == 0)
                return 0;

            int size = 0;

            for (InlineIndexHelper idxHelper : inlineIdxs) {
                if (idxHelper.size() <= 0) {
                    size = propSize;
                    break;
                }
                // 1 byte type + size
                size += idxHelper.size() + 1;
            }

            return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
        }
        else
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);
    }

    /**
     * @param segIdx Segment index.
     * @return RootPage for meta page.
     * @throws IgniteCheckedException If failed.
     */
    private RootPage getMetaPage(int segIdx) throws IgniteCheckedException {
        return cctx.offheap().rootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /**
     * @param segIdx Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void dropMetaPage(int segIdx) throws IgniteCheckedException {
        cctx.offheap().dropRootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /**
     * Returns a filter which returns true for entries belonging to a particular partition.
     *
     * @param qryFilter Factory that creates a predicate for filtering entries for a particular cache.
     * @return The filter or null if the filter is not needed (e.g., if the cache is not partitioned).
     */
    @Nullable private IndexingQueryCacheFilter partitionFilter(@Nullable IndexingQueryFilter qryFilter) {
        if (qryFilter == null)
            return null;

        String cacheName = getTable().cacheName();

        return qryFilter.forCache(cacheName);
    }

    /**
     * An adapter from {@link IndexingQueryCacheFilter} to {@link BPlusTree.TreeRowClosure} which
     * filters entries that belong to the current partition.
     */
    private static class PartitionFilterTreeRowClosure implements BPlusTree.TreeRowClosure<SearchRow, GridH2Row> {
        /** Filter. */
        private final IndexingQueryCacheFilter filter;

        /**
         * Creates a {@link BPlusTree.TreeRowClosure} adapter based on the given partition filter.
         *
         * @param filter The partition filter.
         */
        public PartitionFilterTreeRowClosure(IndexingQueryCacheFilter filter) {
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BPlusTree<SearchRow, GridH2Row> tree,
            BPlusIO<SearchRow> io, long pageAddr, int idx) throws IgniteCheckedException {

            H2RowLinkIO h2io = (H2RowLinkIO)io;

            return filter.applyPartition(
                PageIdUtils.partId(
                    PageIdUtils.pageId(
                        h2io.getLink(pageAddr, idx))));
        }
    }

    /**
     * Returns a filter to apply to rows in the current index to obtain only the
     * ones owned by the this cache.
     *
     * @return The filter, which returns true for rows owned by this cache.
     */
    @Nullable private BPlusTree.TreeRowClosure<SearchRow, GridH2Row> filterClosure() {
        final IndexingQueryCacheFilter filter = partitionFilter(threadLocalFilter());

        return filter != null ? new PartitionFilterTreeRowClosure(filter) : null;
    }

    /** {@inheritDoc} */
    @Override public void refreshColumnIds() {
        super.refreshColumnIds();

        if (inlineIdxs == null)
            return;

        List<InlineIndexHelper> inlineHelpers = getAvailableInlineColumns(indexColumns);

        assert inlineIdxs.size() == inlineHelpers.size();

        for (int pos = 0; pos < inlineHelpers.size(); ++pos)
            inlineIdxs.set(pos, inlineHelpers.get(pos));

        for (H2Tree seg : segments)
            seg.refreshColumnIds(inlineIdxs);
    }

    /**
     * Empty cursor.
     */
    public static final Cursor EMPTY_CURSOR = new Cursor() {
        /** {@inheritDoc} */
        @Override public Row get() {
            throw DbException.convert(new NoSuchElementException("Empty cursor"));
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            throw DbException.convert(new NoSuchElementException("Empty cursor"));
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            return false;
        }
    };

    /**
     * Returns number of elements in the tree by scanning pages of the bottom (leaf) level.
     *
     * @return Number of elements in the tree.
     * @throws IgniteCheckedException If failed.
     */
    public long size() throws IgniteCheckedException {
        long ret = 0;

        for (int i = 0; i < segmentsCount(); i++) {
            final H2Tree tree = treeForRead(i);

            ret += tree.size();
        }

        return ret;
    }

    /**
     * Interface for {@link H2Tree} factory class.
     */
    public interface H2TreeFactory {
        /** */
        public H2Tree create(
            GridCacheContext cctx,
            String name,
            String tblName,
            Table table,
            String cacheName,
            String idxName,
            ReuseList reuseList,
            int grpId,
            String grpName,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            AtomicLong globalRmvId,
            H2RowFactory rowStore,
            long metaPageId,
            boolean initNew,
            IndexColumn[] cols,
            List<InlineIndexHelper> inlineIdxs,
            int inlineSize,
            boolean pk,
            boolean affinityKey,
            @Nullable H2RowCache rowCache,
            @Nullable FailureProcessor failureProcessor,
            IgniteLogger log
        ) throws IgniteCheckedException;
    }
}

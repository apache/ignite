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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
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
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

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

    /**
     * @param cctx Cache context.
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param colsList Index columns.
     * @param inlineSize Inline size.
     * @throws IgniteCheckedException If failed.
     */
    public H2TreeIndex(
        GridCacheContext<?, ?> cctx,
        @Nullable H2RowCache rowCache,
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList,
        int inlineSize,
        int segmentsCnt
    ) throws IgniteCheckedException {
        assert segmentsCnt > 0 : segmentsCnt;

        this.cctx = cctx;

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        name = (tbl.rowDescriptor() == null ? "" : typeId + "_") + name;

        name = BPlusTree.treeName(name, "H2Tree");

        if (cctx.affinityNode()) {
            inlineIdxs = getAvailableInlineColumns(cols);

            segments = new H2Tree[segmentsCnt];

            IgniteCacheDatabaseSharedManager db = cctx.shared().database();

            for (int i = 0; i < segments.length; i++) {
                db.checkpointReadLock();

                try {
                    RootPage page = getMetaPage(name, i);

                    segments[i] = new H2Tree(
                        name,
                        cctx.offheap().reuseListForIndex(name),
                        cctx.groupId(),
                        cctx.dataRegion().pageMemory(),
                        cctx.shared().wal(),
                        cctx.offheap().globalRemoveId(),
                        tbl.rowFactory(),
                        page.pageId().pageId(),
                        page.isAllocated(),
                        cols,
                        inlineIdxs,
                        computeInlineSize(inlineIdxs, inlineSize),
                        rowCache,
                        cctx.kernalContext().failure()) {
                        @Override public int compareValues(Value v1, Value v2) {
                            return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);
                        }
                    };
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
     * @param cols Columns array.
     * @return List of {@link InlineIndexHelper} objects.
     */
    private List<InlineIndexHelper> getAvailableInlineColumns(IndexColumn[] cols) {
        List<InlineIndexHelper> res = new ArrayList<>();

        for (IndexColumn col : cols) {
            if (!InlineIndexHelper.AVAILABLE_TYPES.contains(col.column.getType()))
                break;

            InlineIndexHelper idx = new InlineIndexHelper(
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

                return new H2Cursor(cursor);
            }
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
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
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.putx(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
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
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

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
    @Override public void destroy(boolean rmvIndex) {
        try {
            if (cctx.affinityNode() && rmvIndex) {
                assert cctx.shared().database().checkpointLockIsHeldByThread();

                for (int i = 0; i < segments.length; i++) {
                    H2Tree tree = segments[i];

                    tree.destroy();

                    dropMetaPage(tree.getName(), i);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            super.destroy(rmvIndex);
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
                range = GridH2IndexBase.EMPTY_CURSOR;

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
     * @param name Name.
     * @param segIdx Segment index.
     * @return RootPage for meta page.
     * @throws IgniteCheckedException If failed.
     */
    private RootPage getMetaPage(String name, int segIdx) throws IgniteCheckedException {
        return cctx.offheap().rootPageForIndex(cctx.cacheId(), name + "%" + segIdx);
    }

    /**
     * @param name Name.
     * @param segIdx Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void dropMetaPage(String name, int segIdx) throws IgniteCheckedException {
        cctx.offheap().dropRootPageForIndex(cctx.cacheId(), name + "%" + segIdx);
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
}

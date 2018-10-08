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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
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

    /** Table name/ */
    private final String tblName;

    /** */
    private final boolean pk;

    /** */
    private final boolean affinityKey;

    /** */
    private final String idxName;

    /** */
    private final IgniteLogger log;

    /**
     * @param cctx Cache context.
     * @param tbl Table.
     * @param idxName Index name.
     * @param pk Primary key.
     * @param affinityKey {@code true} for affinity key.
     * @param colsList Index columns.
     * @param inlineSize Inline size.
     * @throws IgniteCheckedException If failed.
     */
    public H2TreeIndex(
        GridCacheContext<?, ?> cctx,
        @Nullable H2RowCache rowCache,
        GridH2Table tbl,
        String idxName,
        boolean pk,
        boolean affinityKey,
        List<IndexColumn> colsList,
        int inlineSize,
        int segmentsCnt
    ) throws IgniteCheckedException {
        assert segmentsCnt > 0 : segmentsCnt;

        this.cctx = cctx;

        this.log = cctx.logger(getClass().getName());

        this.pk = pk;
        this.affinityKey = affinityKey;

        this.tblName = tbl.getName();
        this.idxName = idxName;

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, idxName, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        String treeName = (tbl.rowDescriptor() == null ? "" : typeId + "_") + idxName;

        treeName = BPlusTree.treeName(treeName, "H2Tree");

        if (cctx.affinityNode()) {
            inlineIdxs = getAvailableInlineColumns(cols);

            segments = new H2Tree[segmentsCnt];

            IgniteCacheDatabaseSharedManager db = cctx.shared().database();

            AtomicInteger maxCalculatedInlineSize = new AtomicInteger();

            for (int i = 0; i < segments.length; i++) {
                db.checkpointReadLock();

                try {
                    RootPage page = getMetaPage(treeName, i);

                    segments[i] = new H2Tree(
                        treeName,
                        idxName,
                        tblName,
                        tbl.cacheName(),
                        cctx.offheap().reuseListForIndex(treeName),
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
                        maxCalculatedInlineSize,
                        pk,
                        affinityKey,
                        cctx.mvccEnabled(),
                        rowCache,
                        cctx.kernalContext().failure(),
                        log) {
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
            if (!InlineIndexHelper.AVAILABLE_TYPES.contains(col.column.getType())) {
                String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

                U.warn(log, "Column cannot be inlined into the index because it's type doesn't support inlining, " +
                    "index access may be slow due to additional page reads (change column type if possible) " +
                    "[cacheName=" + cctx.name() +
                    ", tableName=" + tblName +
                    ", idxName=" + idxName +
                    ", idxType=" + idxType +
                    ", colName=" + col.columnName +
                    ", columnType=" + InlineIndexHelper.nameTypeBycode(col.column.getType()) + ']'
                );

                break;
            }

            InlineIndexHelper idx = new InlineIndexHelper(
                col.columnName,
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
            assert lower == null || lower instanceof GridH2SearchRow : lower;
            assert upper == null || upper instanceof GridH2SearchRow : upper;

            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            if (!cctx.mvccEnabled() && indexType.isPrimaryKey() && lower != null && upper != null &&
                tree.compareRows((GridH2SearchRow)lower, (GridH2SearchRow)upper) == 0) {
                GridH2Row row = tree.findOne((GridH2SearchRow)lower, filter(GridH2QueryContext.get()), null);

                return (row == null) ? GridH2Cursor.EMPTY : new SingleRowCursor(row);
            }
            else {
                return new H2Cursor(tree.find((GridH2SearchRow)lower,
                    (GridH2SearchRow)upper, filter(GridH2QueryContext.get()), null));
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
        assert row instanceof GridH2SearchRow : row;

        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.remove((GridH2SearchRow)row);
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
            assert row instanceof GridH2SearchRow : row;

            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.removex((GridH2SearchRow)row);
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

            GridH2QueryContext qctx = GridH2QueryContext.get();

            return tree.size(filter(qctx));
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
            H2Tree tree = treeForRead(threadLocalSegment());
            GridH2QueryContext qctx = GridH2QueryContext.get();

            return new SingleRowCursor(b ? tree.findFirst(filter(qctx)): tree.findLast(filter(qctx)));
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
        @Nullable SearchRow last,
        BPlusTree.TreeRowClosure<GridH2SearchRow, GridH2Row> filter) {
        try {
            GridCursor<GridH2Row> range = ((BPlusTree)t).find(first, last, filter, null);

            if (range == null)
                range = EMPTY_CURSOR;

            return new H2Cursor(range);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected BPlusTree.TreeRowClosure<GridH2SearchRow, GridH2Row> filter(GridH2QueryContext qctx) {
        if (qctx == null) {
            assert !cctx.mvccEnabled();

            return null;
        }

        IndexingQueryFilter f = qctx.filter();
        IndexingQueryCacheFilter p = f == null ? null : f.forCache(getTable().cacheName());
        MvccSnapshot v = qctx.mvccSnapshot();

        assert !cctx.mvccEnabled() || v != null;

        if(p == null && v == null)
            return null;

        return new H2TreeFilterClosure(p, v, cctx);
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
}

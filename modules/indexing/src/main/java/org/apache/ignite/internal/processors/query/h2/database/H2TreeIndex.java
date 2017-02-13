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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * H2 Index over {@link BPlusTree}.
 */
public class H2TreeIndex extends GridH2IndexBase {
    /** PageContext for use in IO's */
    private static final ThreadLocal<H2TreeIndexPageContext> currentPageContext = new ThreadLocal<>();

    /** */
    private static final int MAX_ITEM_SIZE = 1024;

    /** */
    private final H2Tree tree;

    /** Page context. */
    private final H2TreeIndexPageContext pageCtx;

    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

    /**
     * @param cctx Cache context.
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param colsList Index columns.
     * @throws IgniteCheckedException If failed.
     */
    public H2TreeIndex(
        GridCacheContext<?,?> cctx,
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList
    ) throws IgniteCheckedException {
        this.cctx = cctx;
        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        name = tbl.rowDescriptor().type().typeId() + "_" + name;

        name = BPlusTree.treeName(name, "H2Tree");

        if (cctx.affinityNode()) {
            IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

            RootPage page = cctx.offheap().rootPageForIndex(name);

            final H2TreeIndexPageContext ctx0 = getPageCtx(cols);

            if (pk || ctx0.empty()) {
                pageCtx = null;

                tree = new H2Tree(name, cctx.offheap().reuseListForIndex(name), cctx.cacheId(),
                    dbMgr.pageMemory(), cctx.shared().wal(), cctx.offheap().globalRemoveId(),
                    tbl.rowFactory(), page.pageId().pageId(), page.isAllocated()) {
                    @Override protected int compare(BPlusIO<SearchRow> io, long pageAddr, int idx, SearchRow row)
                        throws IgniteCheckedException {
                        return compareRows(getRow(io, pageAddr, idx), row);
                    }
                };
            }
            else {
                pageCtx = ctx0;

                tree = new H2Tree(name, cctx.offheap().reuseListForIndex(name), cctx.cacheId(),
                    dbMgr.pageMemory(), cctx.shared().wal(), cctx.offheap().globalRemoveId(),
                    tbl.rowFactory(), page.pageId().pageId(), page.isAllocated(),
                    H2ExtrasInnerIO.VERSIONS, H2ExtrasLeafIO.VERSIONS) {
                    @Override protected int compare(BPlusIO<SearchRow> io, long pageAddr, int idx, SearchRow row)
                        throws IgniteCheckedException {

                        int off = io.offset(pageAddr, idx, pageCtx.payloadSize() + 8);

                        int fieldOff = 0;

                        for (int i = 0; i < pageCtx.fastIdxs().size(); i++) {
                            FastIndexHelper fastIdx = pageCtx.fastIdxs().get(i);

                            Value v1 = fastIdx.get(pageAddr, off + fieldOff);
                            fieldOff += fastIdx.size();
                            if (v1 == null) {
                                // Can't compare further.
                                return 0;
                            }

                            Value v2 = row.getValue(fastIdx.columnIdx());
                            if (v2 == null) {
                                // Can't compare further.
                                return 0;
                            }

                            int c = compareValues(v1, v2, fastIdx.sortType());
                            if (c != 0)
                                return c;
                        }

                        SearchRow rowData = getRow(io, pageAddr, idx);

                        for (int i = pageCtx.fastIdxs().size(), len = indexColumns.length; i < len; i++) {
                            int idx0 = columnIds[i];

                            Value v1 = rowData.getValue(idx0);
                            if (v1 == null) {
                                // Can't compare further.
                                return 0;
                            }

                            Value v2 = row.getValue(idx0);
                            if (v2 == null) {
                                // Can't compare further.
                                return 0;
                            }

                            int c = compareValues(v1, v2, indexColumns[i].sortType);
                            if (c != 0)
                                return c;
                        }

                        return 0;
                    }
                };
            }
        }
        else {
            // We need indexes on the client node, but index will not contain any data.
            tree = null;
            pageCtx = null;
        }

        initDistributedJoinMessaging(tbl);
    }

    /**
     * @param cols Columns array.
     * @return List of {@link FastIndexHelper} objects.
     */
    private H2TreeIndexPageContext getPageCtx(IndexColumn[] cols) {
        int maxSize = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, MAX_ITEM_SIZE);

        if (maxSize == 0)
            return new H2TreeIndexPageContext(null, 0);

        List<FastIndexHelper> res = new ArrayList<>();

        int maxPayloadSize = Math.min(maxSize, MAX_ITEM_SIZE);

        int payloadSize = 0;

        for (int i = 0; i < cols.length; i++) {
            IndexColumn col = cols[i];

            if (!FastIndexHelper.AVAILABLE_TYPES.contains(col.column.getType()))
                break;

            FastIndexHelper idx = new FastIndexHelper(col.column.getType(), col.column.getColumnId(), col.sortType);

            if (payloadSize + idx.size() > maxPayloadSize)
                break;

            payloadSize += idx.size();
            res.add(idx);
        }

        return new H2TreeIndexPageContext(res, payloadSize);
    }

    /**
     * @return Tree updated in current thread.
     */
    public static H2TreeIndexPageContext getCurrentPageContext() {
        return currentPageContext.get();
    }

    /**
     * @param a First value.
     * @param b Second Value.
     * @param sortType Sort type.
     * @return Compare result.
     */
    private int compareValues(Value a, Value b, int sortType) {
        if (a == b)
            return 0;

        int comp = table.compareTypeSafe(a, b);

        if ((sortType & SortOrder.DESCENDING) != 0)
            comp = -comp;

        return comp;
    }

    /**
     * @return Tree.
     */
    public H2Tree tree() {
        return tree;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        try {
            IndexingQueryFilter f = threadLocalFilter();
            IgniteBiPredicate<Object,Object> p = null;

            if (f != null) {
                String spaceName = getTable().spaceName();

                p = f.forSpace(spaceName);
            }

            return new H2Cursor(tree.find(lower, upper), p);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row findOne(GridH2Row row) {
        try {
            return tree.findOne(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        try {
            currentPageContext.set(pageCtx);

            return tree.put(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentPageContext.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        try {
            currentPageContext.set(pageCtx);

            return tree.putx(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentPageContext.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        try {
            currentPageContext.set(pageCtx);
            return tree.remove(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentPageContext.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void removex(SearchRow row) {
        try {
            currentPageContext.set(pageCtx);
            tree.removex(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentPageContext.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;

    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Cursor cursor = find(ses, null, null);

        long res = 0;

        while (cursor.next())
            res++;

        return res;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        try {
            if (cctx.affinityNode()) {
                tree.destroy();

                cctx.offheap().dropRootPageForIndex(tree.getName());
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            super.destroy();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected IgniteTree<SearchRow, GridH2Row> doTakeSnapshot() {
        return tree;
    }

    /** {@inheritDoc} */
    protected IgniteTree<SearchRow, GridH2Row> treeForRead() {
        return tree;
    }

    /** {@inheritDoc} */
    protected GridCursor<GridH2Row> doFind0(
        IgniteTree t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last,
        IndexingQueryFilter filter) {
        includeFirst &= first != null;

        try {
            GridCursor<GridH2Row> range = tree.find(first, last);

            if (range == null)
                return EMPTY_CURSOR;

            return filter(range, filter);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * IO page context.
     */
    public static class H2TreeIndexPageContext {
        /** */
        private final List<FastIndexHelper> fastIdxs;
        /** */
        private final int payloadSize;

        /** */
        public H2TreeIndexPageContext(
            List<FastIndexHelper> fastIdxs, int payloadSize) {
            this.fastIdxs = fastIdxs;
            this.payloadSize = payloadSize;
        }

        /** */
        public List<FastIndexHelper> fastIdxs() {
            return fastIdxs;
        }

        /** */
        public int payloadSize() {
            return payloadSize;
        }

        /** */
        public boolean empty() {
            return F.isEmpty(fastIdxs);
        }
    }
}

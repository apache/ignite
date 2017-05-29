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
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
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
    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 0;

    /** PageContext for use in IO's */
    private static final ThreadLocal<H2TreeIndex> currentIndex = new ThreadLocal<>();

    /** */
    private final H2Tree tree;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

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
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList,
        int inlineSize
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

            inlineIdxs = getAvailableInlineColumns(cols);

            tree = new H2Tree(name, cctx.offheap().reuseListForIndex(name), cctx.cacheId(),
                dbMgr.pageMemory(), cctx.shared().wal(), cctx.offheap().globalRemoveId(),
                tbl.rowFactory(), page.pageId().pageId(), page.isAllocated(), computeInlineSize(inlineIdxs, inlineSize)) {
                @Override protected int compare(BPlusIO<SearchRow> io, long pageAddr, int idx, SearchRow row)
                    throws IgniteCheckedException {

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

                            Value v1 = inlineIdx.get(pageAddr, off + fieldOff, inlineSize() - fieldOff);

                            if (v1 == null)
                                break;

                            int c = compareValues(v1, v2, inlineIdx.sortType());

                            if (!canRelyOnCompare(c, v1, v2, inlineIdx))
                                break;

                            lastIdxUsed++;

                            if (c != 0)
                                return c;

                            fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                            if (fieldOff > inlineSize())
                                break;
                        }

                        SearchRow rowData = getRow(io, pageAddr, idx);

                        for (int i = lastIdxUsed, len = indexColumns.length; i < len; i++) {
                            int idx0 = columnIds[i];

                            Value v2 = row.getValue(idx0);
                            if (v2 == null) {
                                // Can't compare further.
                                return 0;
                            }

                            Value v1 = rowData.getValue(idx0);

                            int c = compareValues(v1, v2, indexColumns[i].sortType);
                            if (c != 0)
                                return c;
                        }

                        return 0;
                    }
                }
            };
        }
        else {
            // We need indexes on the client node, but index will not contain any data.
            tree = null;
            inlineIdxs = null;
        }

        initDistributedJoinMessaging(tbl);
    }

    /**
     * @param cols Columns array.
     * @return List of {@link InlineIndexHelper} objects.
     */
    private List<InlineIndexHelper> getAvailableInlineColumns(IndexColumn[] cols) {

        // todo: null
        List<InlineIndexHelper> res = new ArrayList<>();

        for (int i = 0; i < cols.length; i++) {
            IndexColumn col = cols[i];

            if (!InlineIndexHelper.AVAILABLE_TYPES.contains(col.column.getType()))
                break;

            InlineIndexHelper idx = new InlineIndexHelper(col.column.getType(), col.column.getColumnId(), col.sortType);

            res.add(idx);
        }

        return res;
    }

    /**
     * @return Tree updated in current thread.
     */
    public static H2TreeIndex getCurrentIndex() {
        return currentIndex.get();
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

    /**
     * @return InlineIndexHelper list.
     */
    public List<InlineIndexHelper> inlineIndexes() {
        return inlineIdxs;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        try {
            IndexingQueryFilter f = threadLocalFilter();
            IgniteBiPredicate<Object, Object> p = null;

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
            currentIndex.set(this);

            return tree.put(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentIndex.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        try {
            currentIndex.set(this);

            return tree.putx(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentIndex.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        try {
            currentIndex.set(this);
            return tree.remove(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentIndex.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void removex(SearchRow row) {
        try {
            currentIndex.set(this);
            tree.removex(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            currentIndex.remove();
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
                if (!cctx.kernalContext().cache().context().database().persistenceEnabled()) {
                    tree.destroy();

                    cctx.offheap().dropRootPageForIndex(tree.getName());
                }
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
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from cache config.
     * @return Inline size.
     */
    private int computeInlineSize(List<InlineIndexHelper> inlineIdxs, int cfgInlineSize) {
        int maxSize = PageIO.MAX_PAYLOAD_SIZE;

        int propSize = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE,
            IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT);

        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        if (cfgInlineSize == -1) {
            if (propSize == 0)
                return 0;

            int size = 0;

            for (int i = 0; i < inlineIdxs.size(); i++) {
                InlineIndexHelper idxHelper = inlineIdxs.get(i);
                if (idxHelper.size() <= 0) {
                    size = propSize;
                    break;
                }
                // 1 byte type + size
                size += idxHelper.size() + 1;
            }

            return Math.min(maxSize, size);
        }
        else
            return Math.min(maxSize, cfgInlineSize);
    }

    /**
     * @param c Compare result.
     * @param shortVal Short value.
     * @param v2 Second value;
     * @param inlineIdx Index helper.
     * @return {@code true} if we can rely on compare result.
     */
    protected static boolean canRelyOnCompare(int c, Value shortVal, Value v2, InlineIndexHelper inlineIdx) {
        if (inlineIdx.type() == Value.STRING) {
            if (c == 0 && shortVal.getType() != Value.NULL && v2.getType() != Value.NULL)
                return false;

            if (shortVal.getType() != Value.NULL
                && v2.getType() != Value.NULL
                && ((c < 0 && inlineIdx.sortType() == SortOrder.ASCENDING) || (c > 0 && inlineIdx.sortType() == SortOrder.DESCENDING))
                && shortVal.getString().length() <= v2.getString().length()) {
                // Can't rely on compare, should use full string.
                return false;
            }
        }

        return true;
    }
}

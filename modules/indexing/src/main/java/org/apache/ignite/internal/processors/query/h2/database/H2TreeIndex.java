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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;

/**
 * H2 Index over {@link BPlusTree}.
 */
public class H2TreeIndex extends GridH2IndexBase {
    /** */
    private final H2Tree tree;

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
        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        name = tbl.rowDescriptor().type().typeId() + "_" + name;

        name = BPlusTree.treeName(name, "H2Tree");

        IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

        RootPage page = cctx.offheap().rootPageForIndex(name);

        tree = new H2Tree(name, cctx.offheap().reuseListForIndex(name), cctx.cacheId(),
            dbMgr.pageMemory(), cctx.shared().wal(), cctx.offheap().globalRemoveId(),
            tbl.rowFactory(), page.pageId().pageId(), page.isAllocated()) {
            @Override protected int compare(BPlusIO<SearchRow> io, ByteBuffer buf, int idx, SearchRow row)
                throws IgniteCheckedException {
                return compareRows(getRow(io, buf, idx), row);
            }
        };

        initDistributedJoinMessaging(tbl);
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
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public GridH2Row put(GridH2Row row) {
        try {
            return tree.put(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        try {
            return tree.remove(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
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
            tree.destroy();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            super.destroy();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object doTakeSnapshot() {
        assert false;

        return this;
    }

    /**
     * Cursor.
     */
    private static class H2Cursor implements Cursor {
        /** */
        final GridCursor<GridH2Row> cursor;

        /** */
        final IgniteBiPredicate<Object,Object> filter;

        /** */
        final long time = U.currentTimeMillis();

        /**
         * @param cursor Cursor.
         * @param filter Filter.
         */
        private H2Cursor(GridCursor<GridH2Row> cursor, IgniteBiPredicate<Object,Object> filter) {
            assert cursor != null;

            this.cursor = cursor;
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            try {
                return cursor.get();
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            try {
                while (cursor.next()) {
                    GridH2Row row = cursor.get();

                    if (row.expireTime() > 0 && row.expireTime() <= time)
                        continue;

                    if (filter == null)
                        return true;

                    Object key = row.getValue(0).getObject();
                    Object val = row.getValue(1).getObject();

                    assert key != null;
                    assert val != null;

                    if (filter.apply(key, val))
                        return true;
                }

                return false;
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}

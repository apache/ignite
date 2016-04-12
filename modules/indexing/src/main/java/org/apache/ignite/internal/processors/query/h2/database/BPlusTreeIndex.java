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
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * H2 Index over {@link BPlusTree}.
 */
public class BPlusTreeIndex extends PageMemoryIndex {
    /** */
    private GridCacheContext<?,?> cctx;

    /** */
    private PageMemory pageMem;

    /** */
    private H2BPlusTree tree;

    /**
     * @param cctx Cache context.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @param keyCol Key column.
     * @param valCol Value column.
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param cols Index columns.
     * @throws IgniteCheckedException If failed.
     */
    public BPlusTreeIndex(
        GridCacheContext<?,?> cctx,
        PageMemory pageMem,
        FullPageId metaPageId,
        boolean initNew,
        int keyCol,
        int valCol,
        GridH2Table tbl,
        String name,
        boolean pk,
        IndexColumn[] cols
    ) throws IgniteCheckedException {
        super(keyCol, valCol);

        assert cctx.cacheId() == metaPageId.cacheId();

        if (!pk) {
            // For other indexes we add primary key at the end to avoid conflicts.
            cols = Arrays.copyOf(cols, cols.length + 1);

            cols[cols.length - 1] = tbl.indexColumn(keyCol, SortOrder.ASCENDING);
        }

        this.pageMem = pageMem;
        this.cctx = cctx;

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        tree = new H2BPlusTree(tbl.rowStore(), metaPageId, initNew);
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        try {
            return new H2Cursor(tree.find(lower, upper));
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
    @Override public double getCost(Session ses, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filter, sortOrder);
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

    /**
     * Cursor.
     */
    private static class H2Cursor implements Cursor {
        /** */
        final GridCursor<GridH2Row> cursor;

        /**
         * @param cursor Cursor.
         */
        private H2Cursor(GridCursor<GridH2Row> cursor) {
            assert cursor != null;

            this.cursor = cursor;
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
                return cursor.next();
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

    /**
     * Specialization of {@link BPlusTree} for H2 index.
     */
    private class H2BPlusTree extends BPlusTree<SearchRow, GridH2Row> {
        /** */
        private final H2RowStore rowStore;

        /**
         * @param rowStore Row data store.
         * @param metaPageId Meta page ID.
         * @param initNew    Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        public H2BPlusTree(H2RowStore rowStore, FullPageId metaPageId, boolean initNew)
            throws IgniteCheckedException {
            super(metaPageId, initNew);

            assert rowStore != null;

            this.rowStore = rowStore;
        }

        /** {@inheritDoc} */
        @Override protected Page page(long pageId) throws IgniteCheckedException {
            return pageMem.page(new FullPageId(pageId, cctx.cacheId()));
        }

        /** {@inheritDoc} */
        @Override protected Page allocatePage() throws IgniteCheckedException {
            FullPageId pageId = pageMem.allocatePage(cctx.cacheId(), -1, PageIdAllocator.FLAG_IDX);

            return pageMem.page(pageId);
        }

        /** {@inheritDoc} */
        @Override protected BPlusIO<SearchRow> io(int type, int ver) {
            if (type == PageIO.T_H2_REF_INNER)
                return H2InnerIO.VERSIONS.forVersion(ver);

            assert type == PageIO.T_H2_REF_LEAF: type;

            return H2LeafIO.VERSIONS.forVersion(ver);
        }

        /** {@inheritDoc} */
        @Override protected BPlusInnerIO<SearchRow> latestInnerIO() {
            return H2InnerIO.VERSIONS.latest();
        }

        /** {@inheritDoc} */
        @Override protected BPlusLeafIO<SearchRow> latestLeafIO() {
            return H2LeafIO.VERSIONS.latest();
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<SearchRow> io, ByteBuffer buf, int idx, SearchRow row)
            throws IgniteCheckedException {
            return compareRows(getRow(io, buf, idx), row);
        }

        /** {@inheritDoc} */
        @Override protected GridH2Row getRow(BPlusIO<SearchRow> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            return rowStore.getRow((H2RowLinkIO)io, buf, idx);
        }
    }
}

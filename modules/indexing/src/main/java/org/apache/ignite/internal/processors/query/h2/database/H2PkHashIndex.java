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
 *
 */

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 *
 */
public class H2PkHashIndex extends GridH2IndexBase {
    /** */
    private final GridH2Table tbl;

    /** */
    private final GridCacheContext cctx;

    /**
     * @param cctx Cache context.
     * @param tbl Table.
     * @param name Index name.
     * @param colsList Index columns.
     */
    public H2PkHashIndex(
        GridCacheContext<?, ?> cctx,
        GridH2Table tbl,
        String name,
        List<IndexColumn> colsList
    ) {

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols, IndexType.createPrimaryKey(false, true));

        this.tbl = tbl;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override protected int segmentsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, final SearchRow lower, final SearchRow upper) {
        IndexingQueryCacheFilter filter = null;
        MvccSnapshot mvccSnapshot = null;

        GridH2QueryContext qctx = GridH2QueryContext.get();

        if (qctx != null) {
            IndexingQueryFilter f = qctx.filter();
            filter = f != null ? f.forCache(getTable().cacheName()) : null;
            mvccSnapshot = qctx.mvccSnapshot();
        }

        assert !cctx.mvccEnabled() || mvccSnapshot != null;

        KeyCacheObject lowerObj = lower != null ? cctx.toCacheKeyObject(lower.getValue(0).getObject()) : null;
        KeyCacheObject upperObj = upper != null ? cctx.toCacheKeyObject(upper.getValue(0).getObject()) : null;

        try {
            Collection<GridCursor<? extends CacheDataRow>> cursors = new ArrayList<>();

            for (IgniteCacheOffheapManager.CacheDataStore store : cctx.offheap().cacheDataStores())
                if (filter == null || filter.applyPartition(store.partId()))
                    cursors.add(store.cursor(cctx.cacheId(), lowerObj, upperObj, null, mvccSnapshot));

            return new H2Cursor(cursors.iterator());
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean canScan() {
        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public GridH2Row put(GridH2Row row) {
        // Should not be called directly. Rows are inserted into underlying cache data stores.
        assert false;

        throw DbException.getUnsupportedException("put");
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        // Should not be called directly. Rows are inserted into underlying cache data stores.
        assert false;

        throw DbException.getUnsupportedException("putx");
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        // Should not be called directly. Rows are removed from underlying cache data stores.

        assert false;

        throw DbException.getUnsupportedException("remove");
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        // Should not be called directly. Rows are removed from underlying cache data stores.
        assert false;

        throw DbException.getUnsupportedException("removex");
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        return Double.MAX_VALUE;
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
    @Override public Cursor findFirstOrLast(Session ses, boolean b) {
        throw new UnsupportedOperationException();
    }

    /**
     * Cursor.
     */
    private class H2Cursor implements Cursor {
        /** */
        private final GridH2RowDescriptor desc;

        /** */
        private final Iterator<GridCursor<? extends CacheDataRow>> iter;

        /** */
        private GridCursor<? extends CacheDataRow> curr;

        /**
         * @param iter Cursors iterator.
         */
        private H2Cursor(Iterator<GridCursor<? extends CacheDataRow>> iter) {
            assert iter != null;

            this.iter = iter;

            desc = tbl.rowDescriptor();
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            try {
                return desc.createRow(curr.get());
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
                if (curr != null && curr.next())
                    return true;

                while (iter.hasNext()) {
                    curr = iter.next();

                    if (curr.next())
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

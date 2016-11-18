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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
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

public class H2PartitionedTreeIndex extends GridH2IndexBase implements Comparator<SearchRow> {

    private final ConcurrentMap<Integer, H2Tree> trees = new ConcurrentHashMap<>();

    private final GridCacheContext<?, ?> cctx;

    public H2PartitionedTreeIndex(
        GridCacheContext<?, ?> cctx,
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList) throws IgniteCheckedException {
        this.cctx = cctx;
        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        name = tbl.rowDescriptor().type().typeId() + "_" + name;

        name = BPlusTree.treeName(name, "H2Tree");

        IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

        for (int p = 0; p < cctx.affinity().partitions(); p++) {
            RootPage page = cctx.offheap().rootPageForIndex(name, p);

            trees.put(p, new H2Tree(name, cctx.offheap().reuseListForIndex(name, p), cctx.cacheId(),
                dbMgr.pageMemory(), cctx.shared().wal(), cctx.offheap().globalRemoveId(),
                tbl.rowFactory(), page.pageId().pageId(), page.isAllocated()) {
                @Override protected int compare(BPlusIO<SearchRow> io, ByteBuffer buf, int idx, SearchRow row)
                    throws IgniteCheckedException {
                    return compareRows(getRow(io, buf, idx), row);
                }
            });
        }

        initDistributedJoinMessaging(tbl);
    }

    private H2Tree tree(int part) {
        return trees.get(part);
    }

    @Override public int compare(SearchRow o1, SearchRow o2) {
        return compareRows(o1, o2);
    }

    @Override public GridH2Row put(GridH2Row row) {
        try {
            return tree(row.partition()).put(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    @Override public GridH2Row remove(SearchRow row) {
        if (row instanceof CacheDataRow)
            try {
                return tree(((CacheDataRow)row).partition()).remove(row);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public GridH2Row findOne(GridH2Row row) {
        try {
            return tree(row.partition()).findOne(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    @Nullable @Override protected Object doTakeSnapshot() {
        assert false;

        return this;
    }

    @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
        try {
            IndexingQueryFilter f = threadLocalFilter();
            IgniteBiPredicate<Object, Object> p = null;

            if (f != null) {
                String spaceName = getTable().spaceName();

                p = f.forSpace(spaceName);
            }

            Collection<GridCursor<GridH2Row>> cursors = new ArrayList<>();

            for (GridDhtLocalPartition partition : cctx.topology().currentLocalPartitions()) {
                cursors.add(tree(partition.id()).find(first, last));
            }

            return new PartitionedH2Cursor(cursors, p, this);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false);

        int mul = getDistributedMultiplier(session, filters, filter);

        return mul * baseCost;
    }

    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    @Override public Cursor findFirstOrLast(Session session, boolean first) {
        throw new UnsupportedOperationException();
    }

    @Override public long getRowCount(Session session) {
        Cursor cursor = find(session, null, null);

        long res = 0;

        while (cursor.next())
            res++;

        return res;
    }

    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }

    /**
     * Cursor.
     */
    private static class PartitionedH2Cursor implements Cursor {
        /** */
        final IgniteBiPredicate<Object, Object> filter;

        /** */
        final Comparator<SearchRow> comparator;

        final NavigableMap<GridH2Row, GridCursor<GridH2Row>> nextResults;

        GridH2Row current;

        /** */
        final long time = U.currentTimeMillis();

        /**
         * @param cursors Cursors.
         * @param filter Filter.
         */
        private PartitionedH2Cursor(Iterable<GridCursor<GridH2Row>> cursors,
            IgniteBiPredicate<Object, Object> filter, Comparator<SearchRow> comparator) throws IgniteCheckedException {
            assert cursors != null;

            this.filter = filter;
            this.comparator = comparator;

            this.nextResults = new TreeMap<>(comparator);

            for (GridCursor<GridH2Row> cursor : cursors) {
                if (next(cursor))
                    nextResults.put(cursor.get(), cursor);
            }
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return current;
        }

        private boolean next(GridCursor<GridH2Row> cursor) throws IgniteCheckedException {
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

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            try {
                if (nextResults.isEmpty())
                    return false;

                GridCursor<GridH2Row> cursor = nextResults.remove(nextResults.firstKey());

                current = cursor.get();

                if (next(cursor))
                    nextResults.put(cursor.get(), cursor);

                return true;
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

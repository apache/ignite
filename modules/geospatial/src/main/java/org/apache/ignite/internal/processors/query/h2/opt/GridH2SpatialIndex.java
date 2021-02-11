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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.GridCursorIteratorWrapper;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.index.SpatialIndex;
import org.h2.index.SpatialTreeIndex;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Spatial index.
 */
@SuppressWarnings("unused"/*reflection*/)
public class GridH2SpatialIndex extends GridH2IndexBase implements SpatialIndex {
    /** Cache context. */
    private final GridCacheContext ctx;

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private volatile long rowCnt;

    /** */
    private long rowIds;

    /** */
    private boolean closed;

    /** */
    private final MVRTreeMap<Long>[] segments;

    /** */
    private final Map<Long, H2CacheRow> idToRow = new HashMap<>();

    /** */
    private final Map<Value, Long> keyToId = new HashMap<>();

    /** */
    private final MVStore store;

    /**
     * @param tbl Table.
     * @param idxName Index name.
     * @param cols Columns.
     */
    public GridH2SpatialIndex(GridH2Table tbl, String idxName, IndexColumn... cols) {
        this(tbl, idxName, 1, cols);
    }

    /**
     * @param tbl Table.
     * @param idxName Index name.
     * @param segmentsCnt Index segments count.
     * @param cols Columns.
     */
    @SuppressWarnings("unchecked")
    public GridH2SpatialIndex(GridH2Table tbl, String idxName, int segmentsCnt, IndexColumn... cols) {
        super(tbl, idxName, cols, IndexType.createNonUnique(false, false, true));

        if (cols.length > 1)
            throw DbException.getUnsupportedException("can only do one column");

        if ((cols[0].sortType & SortOrder.DESCENDING) != 0)
            throw DbException.getUnsupportedException("cannot do descending");

        if ((cols[0].sortType & SortOrder.NULLS_FIRST) != 0)
            throw DbException.getUnsupportedException("cannot do nulls first");

        if ((cols[0].sortType & SortOrder.NULLS_LAST) != 0)
            throw DbException.getUnsupportedException("cannot do nulls last");

        table = tbl;

        if (cols[0].column.getType() != Value.GEOMETRY) {
            throw DbException.getUnsupportedException("spatial index on non-geometry column, " +
                cols[0].column.getCreateSQL());
        }

        // Index in memory
        store = MVStore.open(null);

        segments = new MVRTreeMap[segmentsCnt];

        for (int i = 0; i < segmentsCnt; i++)
            segments[i] = store.openMap("spatialIndex-" + i, new MVRTreeMap.Builder<Long>());

        ctx = tbl.rowDescriptor().context();
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        if (getTable().isPartitioned()) {
            assert filter > 0; // Lookup batch will not be created for the first table filter.

            throw DbException.throwInternalError(
                "Table with a spatial index must be the first in the query: " + getTable());
        }

        return null; // Support must be explicitly added.
    }

    /**
     * Check closed.
     */
    private void checkClosed() {
        if (closed)
            throw DbException.throwInternalError();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return segments.length;
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        assert row instanceof H2CacheRow : "requires key to be at 0";

        Lock l = lock.writeLock();

        l.lock();

        try {
            checkClosed();

            Value key = row.getValue(QueryUtils.KEY_COL);

            assert key != null;

            final int seg = segmentForRow(ctx, row);

            Long rowId = keyToId.get(key);

            if (rowId != null) {
                Long oldRowId = segments[seg].remove(getEnvelope(idToRow.get(rowId), rowId));

                assert rowId.equals(oldRowId);
            }
            else {
                rowId = ++rowIds;

                keyToId.put(key, rowId);
            }

            H2CacheRow old = idToRow.put(rowId, row);

            segments[seg].put(getEnvelope(row, rowId), rowId);

            if (old == null)
                rowCnt++; // No replace.

            return old;
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        H2CacheRow old = put(row);

        return old != null;
    }

    /**
     * @param row Row.
     * @param rowId Row id.
     * @return Envelope.
     */
    private SpatialKey getEnvelope(SearchRow row, long rowId) {
        Value v = row.getValue(columnIds[0]);
        Geometry g = ((ValueGeometry) v.convertTo(Value.GEOMETRY)).getGeometry();
        Envelope env = g.getEnvelopeInternal();
        return new SpatialKey(rowId,
            (float) env.getMinX(), (float) env.getMaxX(),
            (float) env.getMinY(), (float) env.getMaxY());
    }

    /**
     * Remove row.
     *
     * @param row Row.
     * @return Old row.
     */
    private H2CacheRow remove(SearchRow row) {
        Lock l = lock.writeLock();

        l.lock();

        try {
            checkClosed();

            Value key = row.getValue(QueryUtils.KEY_COL);

            assert key != null;

            Long rowId = keyToId.remove(key);

            assert rowId != null;

            H2CacheRow oldRow = idToRow.remove(rowId);

            assert oldRow != null;

            final int seg = segmentForRow(ctx, row);

            if (!segments[seg].remove(getEnvelope(row, rowId), rowId))
                throw DbException.throwInternalError("row not found");

            rowCnt--;

            return oldRow;
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        H2Row old = remove(row);

        return old != null;
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean rmIndex) {
        Lock l = lock.writeLock();

        l.lock();

        try {
            closed = true;

            store.close();
        }
        finally {
            l.unlock();
        }

        super.destroy(rmIndex);
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, HashSet<Column> cols) {
        return SpatialTreeIndex.getCostRangeIndex(masks, columns) / 10;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find0(filter);
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        return find0(null);
    }

    /**
     * @param filter Table filter.
     * @return Cursor.
     */
    private Cursor find0(TableFilter filter) {
        Lock l = lock.readLock();

        l.lock();

        try {
            checkClosed();

            final int seg = segment(H2Utils.context(filter.getSession()));

            final MVRTreeMap<Long> segment = segments[seg];

            return new H2Cursor(rowIterator(segment.keySet().iterator(), filter));
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /**
     * @param i Spatial key iterator.
     * @param filter Table filter.
     * @return Iterator over rows.
     */
    @SuppressWarnings("unchecked")
    private GridCursor<H2Row> rowIterator(Iterator<SpatialKey> i, TableFilter filter) {
        if (!i.hasNext())
            return H2Utils.EMPTY_CURSOR;

        long time = System.currentTimeMillis();

        IndexingQueryFilter qryFilter = null;

        QueryContext qctx = H2Utils.context(filter.getSession());

        if (qctx != null)
            qryFilter = qctx.filter();

        IndexingQueryCacheFilter qryCacheFilter = qryFilter != null ? qryFilter.forCache(getTable().cacheName()) : null;

        List<H2CacheRow> rows = new ArrayList<>();

        do {
            H2CacheRow row = idToRow.get(i.next().getId());

            assert row != null;

            if (row.expireTime() != 0 && row.expireTime() <= time)
                continue;

            if (qryCacheFilter == null || qryCacheFilter.applyPartition(row.partition()))
                rows.add(row);
        }
        while (i.hasNext());

        return new GridCursorIteratorWrapper(rows.iterator());
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        Lock l = lock.readLock();

        l.lock();

        try {
            checkClosed();

            if (!first)
                throw DbException.throwInternalError("Spatial Index can only be fetch by ascending order");

            final int seg = segment(H2Utils.context(ses));

            final MVRTreeMap<Long> segment = segments[seg];

            GridCursor<H2Row> iter = rowIterator(segment.keySet().iterator(), null);

            return new SingleRowCursor(iter.next() ? iter.get() : null);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return rowCnt;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return rowCnt;
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        return rowCnt;
    }

    /** {@inheritDoc} */
    @Override public Cursor findByGeometry(TableFilter filter, SearchRow first, SearchRow last,
        SearchRow intersection) {
        Lock l = lock.readLock();

        l.lock();

        try {
            if (intersection == null)
                return find(filter.getSession(), null, null);

            final int seg = segment(H2Utils.context(filter.getSession()));

            final MVRTreeMap<Long> segment = segments[seg];

            return new H2Cursor(rowIterator(segment.findIntersectingKeys(getEnvelope(intersection, 0)), filter));
        }
        finally {
            l.unlock();
        }
    }
}

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.cache.query.index.AbstractIndex;
import org.apache.ignite.internal.cache.query.index.SingleCursor;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.GridCursorIteratorWrapper;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.index.IndexLookupBatch;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Spatial index implementation.
 */
public class GeoSpatialIndexImpl extends AbstractIndex implements GeoSpatialIndex {
    /** Index unique ID. */
    private final UUID id = UUID.randomUUID();

    /** Cache context. */
    private final GridCacheContext cctx;

    /** */
    final GeoSpatialIndexDefinition def;

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
    private final Map<Long, IndexRow> idToRow = new HashMap<>();

    /** */
    private final Map<Object, Long> keyToId = new HashMap<>();

    /** */
    private final MVStore store;

    /**
     * @param cctx Cache context.
     * @param def Index definition.
     */
    public GeoSpatialIndexImpl(GridCacheContext cctx, GeoSpatialIndexDefinition def) {
        this.cctx = cctx;
        this.def = def;

        // Index in memory
        store = MVStore.open(null);

        segments = new MVRTreeMap[def.segmentsCnt()];

        for (int i = 0; i < def.segmentsCnt(); i++)
            segments[i] = store.openMap("spatialIndex-" + i, new MVRTreeMap.Builder<Long>());
    }

    /** */
    IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        if (cctx.config().getCacheMode() == CacheMode.PARTITIONED) {
            assert filter > 0; // Lookup batch will not be created for the first table filter.

            throw DbException.throwInternalError(
                "Table with a spatial index must be the first in the query: " + def.idxName().tableName());
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

    /**
     * @param rowId Row id.
     * @return Envelope.
     */
    private SpatialKey getEnvelope(CacheDataRow row, long rowId) {
        Geometry g = (Geometry)def.rowHandler().indexKey(0, row).key();

        return getEnvelope(g, rowId);
    }

    /**
     * @param rowId Row id.
     * @return Envelope.
     */
    private SpatialKey getEnvelope(Geometry key, long rowId) {
        Envelope env = key.getEnvelopeInternal();

        return new SpatialKey(rowId,
            (float)env.getMinX(), (float)env.getMaxX(),
            (float)env.getMinY(), (float)env.getMaxY());
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return def.idxName().idxName();
    }

    /** {@inheritDoc} */
    @Override public boolean canHandle(CacheDataRow row) throws IgniteCheckedException {
        return cctx.kernalContext().query().belongsToTable(
            cctx, def.idxName().cacheName(), def.idxName().tableName(), row.key(), row.value());
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow,
        boolean prevRowAvailable) throws IgniteCheckedException {

        Lock l = lock.writeLock();

        l.lock();

        try {
            if (oldRow != null && prevRowAvailable)
                remove(oldRow);

            if (newRow != null)
                put(newRow);
        }
        finally {
            l.unlock();
        }
    }

    /** */
    private boolean put(CacheDataRow row) {
        checkClosed();

        Object key = def.rowHandler().cacheKey(row);

        assert key != null;

        final int seg = segmentForRow(row);

        Long rowId = keyToId.get(key);

        if (rowId != null) {
            Long oldRowId = segments[seg].remove(getEnvelope(idToRow.get(rowId).cacheDataRow(), rowId));

            assert rowId.equals(oldRowId);
        }
        else {
            rowId = ++rowIds;

            keyToId.put(key, rowId);
        }

        IndexRow old = idToRow.put(rowId, new IndexRowImpl(def.rowHandler(), row));

        segments[seg].put(getEnvelope(row, rowId), rowId);

        if (old == null)
            rowCnt++; // No replace.

        return old != null;
    }

    /** */
    private boolean remove(CacheDataRow row) {
        checkClosed();

        Object key = def.rowHandler().cacheKey(row);

        assert key != null;

        Long rowId = keyToId.remove(key);

        assert rowId != null;

        IndexRow oldRow = idToRow.remove(rowId);

        assert oldRow != null;

        final int seg = segmentForRow(row);

        if (!segments[seg].remove(getEnvelope(row, rowId), rowId))
            throw DbException.throwInternalError("row not found");

        rowCnt--;

        return oldRow != null;
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean softDelete) {
        Lock l = lock.writeLock();

        l.lock();

        try {
            closed = true;

            store.close();
        }
        finally {
            l.unlock();
        }
    }

    /**
     * @param filter Table filter.
     * @return Cursor.
     */
    @Override public GridCursor<IndexRow> find(int seg, TableFilter filter) {
        Lock l = lock.readLock();

        l.lock();

        try {
            checkClosed();

            final MVRTreeMap<Long> segment = segments[seg];

            return rowIterator(segment.keySet().iterator(), filter);
        }
        finally {
            l.unlock();
        }
    }

    /**
     * @param i Spatial key iterator.
     * @param filter Table filter.
     * @return Iterator over rows.
     */
    @SuppressWarnings("unchecked")
    private GridCursor<IndexRow> rowIterator(Iterator<SpatialKey> i, TableFilter filter) {
        if (!i.hasNext())
            return IndexValueCursor.EMPTY;

        long time = System.currentTimeMillis();

        IndexingQueryFilter qryFilter = null;

        QueryContext qctx = H2Utils.context(filter.getSession());

        if (qctx != null)
            qryFilter = qctx.filter();

        IndexingQueryCacheFilter qryCacheFilter = qryFilter != null ?
            qryFilter.forCache(cctx.name()) : null;

        List<IndexRow> rows = new ArrayList<>();

        do {
            IndexRow row = idToRow.get(i.next().getId());

            CacheDataRow cacheRow = row.cacheDataRow();

            assert row != null;

            if (cacheRow.expireTime() != 0 && cacheRow.expireTime() <= time)
                continue;

            if (qryCacheFilter == null || qryCacheFilter.applyPartition(cacheRow.partition()))
                rows.add(row);
        }
        while (i.hasNext());

        return new GridCursorIteratorWrapper(rows.iterator());
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findFirstOrLast(int seg, boolean first) {
        Lock l = lock.readLock();

        l.lock();

        try {
            checkClosed();

            if (!first)
                throw DbException.throwInternalError("Spatial Index can only be fetch by ascending order");

            final MVRTreeMap<Long> segment = segments[seg];

            GridCursor<IndexRow> iter = rowIterator(segment.keySet().iterator(), null);

            return new SingleCursor<>(iter.next() ? iter.get() : null);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long totalCount() {
        return rowCnt;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findByGeometry(int seg, TableFilter filter, Geometry intersection) {
        Lock l = lock.readLock();

        l.lock();

        try {
            if (intersection == null)
                return find(seg, filter);

            final MVRTreeMap<Long> segment = segments[seg];

            return rowIterator(segment.findIntersectingKeys(getEnvelope(intersection, 0)), filter);
        }
        finally {
            l.unlock();
        }
    }

    /**
     * @param row cache row.
     * @return Segment ID for given key
     */
    public int segmentForRow(CacheDataRow row) {
        return segments.length == 1 ? 0 : (def.rowHandler().partition(row) % segments.length);
    }
}

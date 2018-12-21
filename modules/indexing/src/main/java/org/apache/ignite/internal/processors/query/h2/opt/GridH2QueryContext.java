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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class GridH2QueryContext {
    /** */
    private static final ThreadLocal<GridH2QueryContext> qctx = new ThreadLocal<>();

    /** */
    private static final ConcurrentMap<Key, GridH2QueryContext> qctxs = new ConcurrentHashMap<>();

    /** */
    private final Key key;

    /** */
    private volatile boolean cleared;

    /** */
    private List<GridReservable> reservations;

    /** Range streams for indexes. */
    private Map<Integer, Object> streams;

    /** Range sources for indexes. */
    private Map<SourceKey, Object> sources;

    /** */
    private int batchLookupIdGen;

    /** */
    private IndexingQueryFilter filter;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private Map<UUID, int[]> partsMap;

    /** */
    private UUID[] partsNodes;

    /** */
    private DistributedJoinMode distributedJoinMode;

    /** */
    private int pageSize;

    /** */
    private GridH2CollocationModel qryCollocationMdl;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** */
    private MapQueryLazyWorker lazyWorker;

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     */
    public GridH2QueryContext(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        assert type != MAP;

        key = new Key(locNodeId, nodeId, qryId, 0, type);
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @param type Query type.
     */
    public GridH2QueryContext(UUID locNodeId,
        UUID nodeId,
        long qryId,
        int segmentId,
        GridH2QueryType type) {
        assert segmentId == 0 || type == MAP;

        key = new Key(locNodeId, nodeId, qryId, segmentId, type);
    }

    /**
     * @return Mvcc snapshot.
     */
    @Nullable public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @param mvccSnapshot Mvcc snapshot.
     * @return {@code this}.
     */
    public GridH2QueryContext mvccSnapshot(MvccSnapshot mvccSnapshot) {
        this.mvccSnapshot = mvccSnapshot;

        return this;
    }

    /**
     * @return Type.
     */
    public GridH2QueryType type() {
        return key.type;
    }

    /**
     * @return Origin node ID.
     */
    public UUID originNodeId() {
        return key.nodeId;
    }

    /**
     * @return Query request ID.
     */
    public long queryId() {
        return key.qryId;
    }

    /**
     * @return Query collocation model.
     */
    public GridH2CollocationModel queryCollocationModel() {
        return qryCollocationMdl;
    }

    /**
     * @param qryCollocationMdl Query collocation model.
     */
    public void queryCollocationModel(GridH2CollocationModel qryCollocationMdl) {
        this.qryCollocationMdl = qryCollocationMdl;
    }

    /**
     * @param distributedJoinMode Distributed join mode.
     * @return {@code this}.
     */
    public GridH2QueryContext distributedJoinMode(DistributedJoinMode distributedJoinMode) {
        this.distributedJoinMode = distributedJoinMode;

        return this;
    }

    /**
     * @return Distributed join mode.
     */
    public DistributedJoinMode distributedJoinMode() {
        return distributedJoinMode;
    }

    /**
     * @param reservations Reserved partitions or group reservations.
     * @return {@code this}.
     */
    public GridH2QueryContext reservations(List<GridReservable> reservations) {
        this.reservations = reservations;

        return this;
    }

    /**
     * @param topVer Topology version.
     * @return {@code this}.
     */
    public GridH2QueryContext topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;

        return this;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param partsMap Partitions map.
     * @return {@code this}.
     */
    public GridH2QueryContext partitionsMap(Map<UUID,int[]> partsMap) {
        this.partsMap = partsMap;

        return this;
    }

    /**
     * @return Partitions map.
     */
    public Map<UUID,int[]> partitionsMap() {
        return partsMap;
    }

    /**
     * @param p Partition.
     * @param cctx Cache context.
     * @return Owning node ID.
     */
    public UUID nodeForPartition(int p, GridCacheContext<?, ?> cctx) {
        UUID[] nodeIds = partsNodes;

        if (nodeIds == null) {
            assert partsMap != null;

            nodeIds = new UUID[cctx.affinity().partitions()];

            for (Map.Entry<UUID, int[]> e : partsMap.entrySet()) {
                UUID nodeId = e.getKey();
                int[] nodeParts = e.getValue();

                assert nodeId != null;
                assert !F.isEmpty(nodeParts);

                for (int part : nodeParts) {
                    assert nodeIds[part] == null;

                    nodeIds[part] = nodeId;
                }
            }

            partsNodes = nodeIds;
        }

        return nodeIds[p];
    }

    /** @return index segment ID. */
    public int segment() {
        return key.segmentId;
    }

    /**
     * @param batchLookupId Batch lookup ID.
     * @param streams Range streams.
     */
    public synchronized void putStreams(int batchLookupId, Object streams) {
        if (this.streams == null) {
            if (streams == null)
                return;

            this.streams = new HashMap<>();
        }

        if (streams == null)
            this.streams.remove(batchLookupId);
        else
            this.streams.put(batchLookupId, streams);
    }

    /**
     * @param batchLookupId Batch lookup ID.
     * @return Range streams.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T getStreams(int batchLookupId) {
        if (streams == null)
            return null;

        return (T)streams.get(batchLookupId);
    }

    /**
     * @param ownerId Owner node ID.
     * @param segmentId Index segment ID.
     * @param batchLookupId Batch lookup ID.
     * @param src Range source.
     */
    public synchronized void putSource(UUID ownerId, int segmentId, int batchLookupId, Object src) {
        SourceKey srcKey = new SourceKey(ownerId, segmentId, batchLookupId);

        if (src != null) {
            if (sources == null)
                sources = new HashMap<>();

            sources.put(srcKey, src);
        }
        else if (sources != null)
            sources.remove(srcKey);
    }

    /**
     * @param ownerId Owner node ID.
     * @param segmentId Index segment ID.
     * @param batchLookupId Batch lookup ID.
     * @return Range source.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T getSource(UUID ownerId, int segmentId, int batchLookupId) {
        if (sources == null)
            return null;

        return (T)sources.get(new SourceKey(ownerId, segmentId, batchLookupId));
    }

    /**
     * @return Next batch ID.
     */
    public int nextBatchLookupId() {
        return ++batchLookupIdGen;
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param x Query context.
     */
     public static void set(GridH2QueryContext x) {
         assert qctx.get() == null;

         // We need MAP query context to be available to other threads to run distributed joins.
         if (x.key.type == MAP && x.distributedJoinMode() != OFF && qctxs.putIfAbsent(x.key, x) != null)
             throw new IllegalStateException("Query context is already set.");

         qctx.set(x);
    }

    /**
     * Drops current thread local context.
     */
    public static void clearThreadLocal() {
        GridH2QueryContext x = qctx.get();

        assert x != null;

        qctx.remove();
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     * @return {@code True} if context was found.
     */
    public static boolean clear(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        boolean res = false;

        for (Key key : qctxs.keySet()) {
            if (key.locNodeId.equals(locNodeId) && key.nodeId.equals(nodeId) && key.qryId == qryId && key.type == type)
                res |= doClear(key, false);
        }

        return res;
    }

    /**
     * @param key Context key.
     * @param nodeStop Node is stopping.
     * @return {@code True} if context was found.
     */
    private static boolean doClear(Key key, boolean nodeStop) {
        assert key.type == MAP : key.type;

        GridH2QueryContext x = qctxs.remove(key);

        if (x == null)
            return false;

        assert x.key.equals(key);

        if (x.lazyWorker() != null)
            x.lazyWorker().stop(nodeStop);
        else
            x.clearContext(nodeStop);

        return true;
    }

    /**
     * @param nodeStop Node is stopping.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void clearContext(boolean nodeStop) {
        cleared = true;

        List<GridReservable> r = reservations;

        if (!nodeStop && !F.isEmpty(r)) {
            for (int i = 0; i < r.size(); i++)
                r.get(i).release();
        }
    }

    /**
     * @return {@code true} If the context is cleared.
     */
    public boolean isCleared() {
        return cleared;
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId Dead node ID.
     */
    public static void clearAfterDeadNode(UUID locNodeId, UUID nodeId) {
        for (Key key : qctxs.keySet()) {
            if (key.locNodeId.equals(locNodeId) && key.nodeId.equals(nodeId))
                doClear(key, false);
        }
    }

    /**
     * @param locNodeId Local node ID.
     */
    public static void clearLocalNodeStop(UUID locNodeId) {
        for (Key key : qctxs.keySet()) {
            if (key.locNodeId.equals(locNodeId))
                doClear(key, true);
        }
    }

    /**
     * Access current thread local query context (if it was set).
     *
     * @return Current thread local query context or {@code null} if the query runs outside of Ignite context.
     */
    @Nullable public static GridH2QueryContext get() {
        return qctx.get();
    }

    /**
     * Access query context from another thread.
     *
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @param type Query type.
     * @return Query context.
     */
    @Nullable public static GridH2QueryContext get(
        UUID locNodeId,
        UUID nodeId,
        long qryId,
        int segmentId,
        GridH2QueryType type
    ) {
        return qctxs.get(new Key(locNodeId, nodeId, qryId, segmentId, type));
    }

    /**
     * @return Filter.
     */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     * @return {@code this}.
     */
    public GridH2QueryContext filter(IndexingQueryFilter filter) {
        this.filter = filter;

        return this;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param pageSize Page size.
     * @return {@code this}.
     */
    public GridH2QueryContext pageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Lazy worker, if any, or {@code null} if none.
     */
    public MapQueryLazyWorker lazyWorker() {
        return lazyWorker;
    }

    /**
     * @param lazyWorker Lazy worker, if any, or {@code null} if none.
     * @return {@code this}.
     */
    public GridH2QueryContext lazyWorker(MapQueryLazyWorker lazyWorker) {
        this.lazyWorker = lazyWorker;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2QueryContext.class, this);
    }

    /**
     * Unique key for the query context.
     */
    private static class Key {
        /** */
        private final UUID locNodeId;

        /** */
        private final UUID nodeId;

        /** */
        private final long qryId;

        /** */
        private final int segmentId;

        /** */
        private final GridH2QueryType type;

        /**
         * @param locNodeId Local node ID.
         * @param nodeId The node who initiated the query.
         * @param qryId The query ID.
         * @param segmentId Index segment ID.
         * @param type Query type.
         */
        private Key(UUID locNodeId, UUID nodeId, long qryId, int segmentId, GridH2QueryType type) {
            assert locNodeId != null;
            assert nodeId != null;
            assert type != null;

            this.locNodeId = locNodeId;
            this.nodeId = nodeId;
            this.qryId = qryId;
            this.segmentId = segmentId;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return qryId == key.qryId && nodeId.equals(key.nodeId) && type == key.type &&
               locNodeId.equals(key.locNodeId) ;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = locNodeId.hashCode();

            res = 31 * res + nodeId.hashCode();
            res = 31 * res + (int)(qryId ^ (qryId >>> 32));
            res = 31 * res + type.hashCode();
            res = 31 * res + segmentId;

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Key.class, this);
        }
    }

    /**
     * Key for source.
     */
    private static final class SourceKey {
        /** */
        UUID ownerId;

        /** */
        int segmentId;

        /** */
        int batchLookupId;

        /**
         * @param ownerId Owner node ID.
         * @param segmentId Index segment ID.
         * @param batchLookupId Batch lookup ID.
         */
        SourceKey(UUID ownerId, int segmentId, int batchLookupId) {
            this.ownerId = ownerId;
            this.segmentId = segmentId;
            this.batchLookupId = batchLookupId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == null || !(o instanceof SourceKey))
                return false;

            SourceKey srcKey = (SourceKey)o;

            return batchLookupId == srcKey.batchLookupId && segmentId == srcKey.segmentId &&
                ownerId.equals(srcKey.ownerId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int hash = ownerId.hashCode();
            hash = 31 * hash + segmentId;
            return 31 * hash + batchLookupId;
        }
    }
}

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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.LOCAL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class GridH2QueryContext {
    /** */
    private static final ThreadLocal<GridH2QueryContext> qctx = new ThreadLocal<>();

    /** */
    private static final ConcurrentMap<Key, GridH2QueryContext> qctxs = new ConcurrentHashMap8<>();

    /** */
    private final Key key;

    /** Index snapshots. */
    @GridToStringInclude
    private Map<Long, Object> snapshots;

    /** Range streams for indexes. */
    private Map<Integer, Object> streams;

    /** Range sources for indexes. */
    private Map<SourceKey, Object> sources;

    /** */
    private byte batchLookupIdGen;

    /** */
    private IndexingQueryFilter filter;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private Map<UUID,int[]> partsMap;

    /** */
    private UUID[] partsNodes;

    /** */
    private boolean distributedJoins;

    /** */
    private int pageSize;

    /** */
    private Map<TableFilter, GridH2TableFilterCollocation>  tableFilterStateCache;

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     */
    public GridH2QueryContext(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        key = new Key(locNodeId, nodeId, qryId, type);
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
     * @return Cache for table filter collocation states.
     */
    public Map<TableFilter, GridH2TableFilterCollocation> tableFilterStateCache() {
        if (tableFilterStateCache == null)
            tableFilterStateCache = new HashMap<>();

        return tableFilterStateCache;
    }

    /**
     * @param distributedJoins Distributed joins can be run in this query.
     * @return {@code this}.
     */
    public GridH2QueryContext distributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;

        return this;
    }

    /**
     * @return {@code true} If distributed joins can be run in this query.
     */
    public boolean distributedJoins() {
        return distributedJoins;
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
    public UUID nodeForPartition(int p, GridCacheContext<?,?> cctx) {
        UUID[] nodeIds = partsNodes;

        if (nodeIds == null) {
            assert partsMap != null;

            nodeIds = new UUID[cctx.affinity().partitions()];

            for (Map.Entry<UUID,int[]> e : partsMap.entrySet()) {
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

    /**
     * @param idxId Index ID.
     * @param snapshot Index snapshot.
     */
    public void putSnapshot(long idxId, Object snapshot) {
        assert snapshot != null;
        assert get() == null : "need to snapshot indexes before setting query context for correct visibility";

        if (snapshot instanceof GridReservable && !((GridReservable)snapshot).reserve())
            throw new IllegalStateException("Must be already reserved before.");

        if (snapshots == null)
            snapshots = new HashMap<>();

        if (snapshots.put(idxId, snapshot) != null)
            throw new IllegalStateException("Index already snapshoted.");
    }

    /**
     * @param idxId Index ID.
     * @return Index snapshot or {@code null} if none.
     */
    @SuppressWarnings("unchecked")
    public <T> T getSnapshot(long idxId) {
        if (snapshots == null)
            return null;

        return (T)snapshots.get(idxId);
    }

    /**
     * @param batchLookupId Batch lookup ID.
     * @param streams Range streams.
     */
    public synchronized void putStreams(int batchLookupId, Object streams) {
        if (this.streams == null)
            this.streams = new HashMap<>();

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
     * @param batchLookupId Batch lookup ID.
     * @param src Range source.
     */
    public synchronized void putSource(UUID ownerId, int batchLookupId, Object src) {
        SourceKey srcKey = new SourceKey(ownerId, batchLookupId);

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
     * @param batchLookupId Batch lookup ID.
     * @return Range source.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T getSource(UUID ownerId, int batchLookupId) {
        if (sources == null)
            return null;

        return (T)sources.get(new SourceKey(ownerId, batchLookupId));
    }

    /**
     * @return Next batch ID.
     */
    public byte nextBatchLookupId() {
        return ++batchLookupIdGen;
    }

    /**
     * @return If indexes were snapshotted before query execution.
     */
    public boolean hasIndexSnapshots() {
        return snapshots != null;
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param x Query context.
     */
     public static void set(GridH2QueryContext x) {
         assert qctx.get() == null;

         if (x.key.type == MAP && qctxs.putIfAbsent(x.key, x) != null)
             throw new IllegalStateException("Query context is already set.");

         qctx.set(x);
    }

    /**
     * Drops current thread local context.
     *
     * @param onlyThreadLoc Drop only thread local context but keep global.
     */
    public static void clear(boolean onlyThreadLoc) {
        GridH2QueryContext x = qctx.get();

        assert x != null;

        qctx.remove();

        if (!onlyThreadLoc && x.key.type != LOCAL)
            doClear(x.key);
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     */
    public static void clear(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        doClear(new Key(locNodeId, nodeId, qryId, type));
    }

    /**
     * @param key Context key.
     */
    private static void doClear(Key key) {
        GridH2QueryContext x = qctxs.remove(key);

        if (x != null && !F.isEmpty(x.snapshots)) {
            for (Object snapshot : x.snapshots.values()) {
                if (snapshot instanceof GridReservable)
                    ((GridReservable)snapshot).release();
            }
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
     * @param type Query type.
     * @return Query context.
     */
    @Nullable public static GridH2QueryContext get(
        UUID locNodeId,
        UUID nodeId,
        long qryId,
        GridH2QueryType type
    ) {
        return qctxs.get(new Key(locNodeId, nodeId, qryId, type));
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
        private final GridH2QueryType type;

        /**
         * @param locNodeId Local node ID.
         * @param nodeId The node who initiated the query.
         * @param qryId The query ID.
         * @param type Query type.
         */
        private Key(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
            assert locNodeId != null;
            assert nodeId != null;
            assert type != null;

            this.locNodeId = locNodeId;
            this.nodeId = nodeId;
            this.qryId = qryId;
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
            int result = locNodeId.hashCode();

            result = 31 * result + nodeId.hashCode();
            result = 31 * result + (int)(qryId ^ (qryId >>> 32));
            result = 31 * result + type.hashCode();

            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Key.class, this);
        }
    }

    /**
     * Key for source.
     */
    private static class SourceKey {
        /** */
        UUID ownerId;

        /** */
        int batchLookupId;

        /**
         * @param ownerId Owner node ID.
         * @param batchLookupId Batch lookup ID.
         */
        SourceKey(UUID ownerId, int batchLookupId) {
            this.ownerId = ownerId;
            this.batchLookupId = batchLookupId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            SourceKey sourceKey = (SourceKey)o;

            return batchLookupId == sourceKey.batchLookupId && ownerId.equals(sourceKey.ownerId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * ownerId.hashCode() + batchLookupId;
        }
    }
}

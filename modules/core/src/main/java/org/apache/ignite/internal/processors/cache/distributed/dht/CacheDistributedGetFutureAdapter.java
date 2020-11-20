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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_NEAR_GET_MAX_REMAPS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public abstract class CacheDistributedGetFutureAdapter<K, V>
    extends GridCacheCompoundIdentityFuture<Map<K, V>> implements CacheGetFuture {
    /** Logger reference. */
    protected static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Maximum number of attempts to remap key to the same primary node. */
    protected static final int MAX_REMAP_CNT = getInteger(IGNITE_NEAR_GET_MAX_REMAPS, DFLT_MAX_REMAP_CNT);

    /** Remap count updater. */
    protected static final AtomicIntegerFieldUpdater<CacheDistributedGetFutureAdapter> REMAP_CNT_UPD =
        AtomicIntegerFieldUpdater.newUpdater(CacheDistributedGetFutureAdapter.class, "remapCnt");

    /** Context. */
    protected final GridCacheContext<K, V> cctx;

    /** Keys. */
    protected Collection<KeyCacheObject> keys;

    /** Read through flag. */
    protected boolean readThrough;

    /** Force primary flag. */
    protected boolean forcePrimary;

    /** Future ID. */
    protected IgniteUuid futId;

    /** Trackable flag. */
    protected boolean trackable;

    /** Remap count. */
    protected volatile int remapCnt;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name. */
    protected String taskName;

    /** Whether to deserialize binary objects. */
    protected boolean deserializeBinary;

    /** Skip values flag. */
    protected boolean skipVals;

    /** Expiry policy. */
    protected IgniteCacheExpiryPolicy expiryPlc;

    /** Flag indicating that get should be done on a locked topology version. */
    protected boolean canRemap = true;

    /** */
    protected final boolean needVer;

    /** */
    protected final boolean keepCacheObjects;

    /** */
    protected final boolean recovery;

    /** */
    protected Map<AffinityTopologyVersion, Map<Integer, Set<ClusterNode>>> invalidNodes = Collections.emptyMap();

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even
     *          if called on backup node.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObjects Keep cache objects flag.
     */
    protected CacheDistributedGetFutureAdapter(
        GridCacheContext<K, V> cctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        boolean keepCacheObjects,
        boolean recovery
    ) {
        super(CU.<K, V>mapsReducer(keys.size()));

        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.readThrough = readThrough;
        this.forcePrimary = forcePrimary;
        this.subjId = subjId;
        this.taskName = taskName;
        this.deserializeBinary = deserializeBinary;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;
        this.needVer = needVer;
        this.keepCacheObjects = keepCacheObjects;
        this.recovery = recovery;

        futId = IgniteUuid.randomUuid();
    }

    /**
     * @param aclass Class.
     */
    protected void initLogger(Class<?> aclass) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, aclass);
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // Should not flip trackable flag from true to false since get future can be remapped.
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param part Partition.
     * @return {@code True} if partition is in owned state.
     */
    protected final boolean partitionOwned(int part) {
        return cctx.topology().partitionState(cctx.localNodeId(), part) == OWNING;
    }

    /**
     * @param fut Future.
     */
    protected void registrateFutureInMvccManager(GridCacheFuture<?> fut) {
        if (!trackable) {
            trackable = true;

            cctx.mvcc().addFuture(fut, futId);
        }
    }

    /**
     * @param node Cluster node.
     * @param part Invalid partition.
     * @param topVer Topology version.
     */
    protected synchronized void addNodeAsInvalid(ClusterNode node, int part, AffinityTopologyVersion topVer) {
        if (invalidNodes == Collections.<AffinityTopologyVersion, Map<Integer, Set<ClusterNode>>>emptyMap()) {
            invalidNodes = new HashMap<>();
        }

        Map<Integer, Set<ClusterNode>> invalidNodeMap = invalidNodes.get(topVer);

        if (invalidNodeMap == null)
            invalidNodes.put(topVer, invalidNodeMap = new HashMap<>());

        Set<ClusterNode> invalidNodeSet = invalidNodeMap.get(part);

        if (invalidNodeSet == null)
            invalidNodeMap.put(part, invalidNodeSet = new HashSet<>());

        invalidNodeSet.add(node);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Set of invalid cluster nodes.
     */
    protected synchronized Set<ClusterNode> getInvalidNodes(int part, AffinityTopologyVersion topVer) {
        Set<ClusterNode> invalidNodeSet = Collections.emptySet();

        Map<Integer, Set<ClusterNode>> invalidNodesMap = invalidNodes.get(topVer);

        if (invalidNodesMap != null) {
            Set<ClusterNode> nodes = invalidNodesMap.get(part);

            if (nodes != null)
                invalidNodeSet = nodes;
        }

        return invalidNodeSet;
    }

    /**
     *
     * @param key Key.
     * @param node Mapped node.
     * @param missedNodesToKeysMapping Full node mapping.
     */
    protected boolean checkRetryPermits(
        KeyCacheObject key,
        ClusterNode node,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> missedNodesToKeysMapping
    ) {
        LinkedHashMap<KeyCacheObject, Boolean> keys = missedNodesToKeysMapping.get(node);

        if (keys != null && keys.containsKey(key)) {
            if (REMAP_CNT_UPD.incrementAndGet(this) > MAX_REMAP_CNT) {
                onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " +
                    MAX_REMAP_CNT + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                    U.toShortString(node) + ", mappings=" + missedNodesToKeysMapping + ']'));

                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                AbstractMiniFuture f = (AbstractMiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    found = true;

                    f.onNodeLeft(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));
                }
            }

        return found;
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridNearGetResponse res) {
        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                AbstractMiniFuture f = (AbstractMiniFuture)fut;

                if (f.futureId().equals(res.miniId())) {
                    assert f.node().id().equals(nodeId);

                    f.onResult(res);
                }
            }
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Exception.
     */
    protected final ClusterTopologyServerNotFoundException serverNotFoundError(int part, AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
            "(all partition nodes left the grid) [topVer=" + topVer +
            ", part" + part + ", cache=" + cctx.name() + ", localNodeId=" + cctx.localNodeId() + ']');
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    protected abstract boolean isMini(IgniteInternalFuture<?> f);

    /**
     * @param keys Collection of mapping keys.
     * @param mapped Previous mapping.
     * @param topVer Topology version.
     */
    protected abstract void map(
        Collection<KeyCacheObject> keys,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        AffinityTopologyVersion topVer
    );

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futuresStrings = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                if (isMini(f)) {
                    AbstractMiniFuture mini = (AbstractMiniFuture)f;

                    return "miniFuture([futId=" + mini.futureId() + ", node=" + mini.node().id() +
                        ", loc=" + mini.node().isLocal() +
                        ", done=" + f.isDone() + "])";
                }
                else
                    return f.getClass().getSimpleName() + " [loc=true, done=" + f.isDone() + "]";
            }
        });

        return S.toString(CacheDistributedGetFutureAdapter.class, this,
            "innerFuts", futuresStrings,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    protected abstract class AbstractMiniFuture extends GridFutureAdapter<Map<K, V>> {
        /** Mini-future id. */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Mapped node. */
        protected final ClusterNode node;

        /** Mapped keys. */
        @GridToStringInclude
        protected final LinkedHashMap<KeyCacheObject, Boolean> keys;

        /** Topology version on which this future was mapped. */
        protected final AffinityTopologyVersion topVer;

        /** Post processing closure. */
        private final IgniteInClosure<Collection<GridCacheEntryInfo>> postProcessingClos;

        /** {@code True} if remapped after node left. */
        private boolean remapped;

        /**
         * @param node Node.
         * @param keys Keys.
         * @param topVer Topology version.
         */
        protected AbstractMiniFuture(
            ClusterNode node,
            LinkedHashMap<KeyCacheObject, Boolean> keys,
            AffinityTopologyVersion topVer
        ) {
            this.node = node;
            this.keys = keys;
            this.topVer = topVer;
            this.postProcessingClos = CU.createBackupPostProcessingClosure(
                topVer, log, cctx, null, expiryPlc, readThrough && cctx.readThroughConfigured(), skipVals);
        }

        /**
         * @return Future ID.
         */
        public IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @return Keys.
         */
        public Collection<KeyCacheObject> keys() {
            return keys.keySet();
        }

        /**
         * Factory methond for generate request associated with this miniFuture.
         *
         * @param rootFutId Root future id.
         * @return Near get request.
         */
        public GridNearGetRequest createGetRequest(IgniteUuid rootFutId) {
            return createGetRequest0(rootFutId, futureId());
        }

        /**
         * @param rootFutId Root future id.
         * @param futId Mini future id.
         * @return Near get request.
         */
        protected abstract GridNearGetRequest createGetRequest0(IgniteUuid rootFutId, IgniteUuid futId);

        /**
         * @param entries Collection of entries.
         * @return Map with key value results.
         */
        protected abstract Map<K, V> createResultMap(Collection<GridCacheEntryInfo> entries);

        /**
         * @param e Error.
         */
        public void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Failure exception.
         */
       public synchronized void onNodeLeft(ClusterTopologyCheckedException e) {
            if (remapped)
                return;

            remapped = true;

            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            // Try getting from existing nodes.
            if (!canRemap) {
                map(keys.keySet(), F.t(node, keys), topVer);

                onDone(Collections.<K, V>emptyMap());
            }
            else {
                long maxTopVer = Math.max(topVer.topologyVersion() + 1, cctx.discovery().topologyVersion());

                AffinityTopologyVersion awaitTopVer = new AffinityTopologyVersion(maxTopVer);

                cctx.shared().exchange()
                    .affinityReadyFuture(awaitTopVer)
                    .listen((f) -> {
                            try {
                                // Remap.
                                map(keys.keySet(), F.t(node, keys), f.get());

                                onDone(Collections.<K, V>emptyMap());
                            }
                            catch (IgniteCheckedException ex) {
                                CacheDistributedGetFutureAdapter.this.onDone(ex);
                            }
                        }
                    );
            }
        }

        /**
         * @param res Result callback.
         */
        public void onResult(GridNearGetResponse res) {
            // If error happened on remote node, fail the whole future.
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            Collection<Integer> invalidParts = res.invalidPartitions();

            // Remap invalid partitions.
            if (!F.isEmpty(invalidParts)) {
                AffinityTopologyVersion rmtTopVer = res.topologyVersion();

                for (Integer part : invalidParts)
                    addNodeAsInvalid(node, part, topVer);

                if (log.isDebugEnabled())
                    log.debug("Remapping mini get future [invalidParts=" + invalidParts + ", fut=" + this + ']');

                if (!canRemap) {
                    map(F.view(keys.keySet(), new P1<KeyCacheObject>() {
                        @Override public boolean apply(KeyCacheObject key) {
                            return invalidParts.contains(cctx.affinity().partition(key));
                        }
                    }), F.t(node, keys), topVer);

                    postProcessResult(res);

                    onDone(createResultMap(res.entries()));

                    return;
                }

                // Remap after remote version will be finished localy.
                cctx.shared().exchange().affinityReadyFuture(rmtTopVer)
                    .listen(new CIX1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void applyx(
                            IgniteInternalFuture<AffinityTopologyVersion> fut
                        ) throws IgniteCheckedException {
                            AffinityTopologyVersion topVer = fut.get();

                            // This will append new futures to compound list.
                            map(F.view(keys.keySet(), new P1<KeyCacheObject>() {
                                @Override public boolean apply(KeyCacheObject key) {
                                    return invalidParts.contains(cctx.affinity().partition(key));
                                }
                            }), F.t(node, keys), topVer);

                            postProcessResult(res);

                            onDone(createResultMap(res.entries()));
                        }
                    });
            }
            else {
                try {
                    postProcessResult(res);

                    onDone(createResultMap(res.entries()));
                }
                catch (Exception e) {
                    onDone(e);
                }
            }
        }

        /**
         * @param res Response.
         */
        protected void postProcessResult(final GridNearGetResponse res) {
            if (postProcessingClos != null)
                postProcessingClos.apply(res.entries());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AbstractMiniFuture.class, this);
        }
    }
}

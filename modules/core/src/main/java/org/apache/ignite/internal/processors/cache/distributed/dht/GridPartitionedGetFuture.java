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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Colocated get future.
 */
public class GridPartitionedGetFuture<K, V> extends CacheDistributedGetFutureAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even
     *          if called on backup node.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param recovery Recovery mode flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObjects Keep cache objects flag.
     */
    public GridPartitionedGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        boolean keepCacheObjects
    ) {
        super(cctx,
            keys,
            readThrough,
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            expiryPlc,
            skipVals,
            needVer,
            keepCacheObjects,
            recovery);

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridPartitionedGetFuture.class);
    }

    /**
     * Initializes future.
     *
     * @param topVer Topology version.
     */
    public void init(AffinityTopologyVersion topVer) {
        AffinityTopologyVersion lockedTopVer = cctx.shared().lockedTopologyVersion(null);

        if (lockedTopVer != null) {
            canRemap = false;

            map(keys, Collections.<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>>emptyMap(), lockedTopVer);
        }
        else {
            topVer = topVer.topologyVersion() > 0 ? topVer :
                canRemap ? cctx.affinity().affinityTopologyVersion() : cctx.shared().exchange().readyAffinityVersion();

            map(keys, Collections.<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>>emptyMap(), topVer);
        }

        markInitialized();
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

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    found = true;

                    f.onNodeLeft(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));
                }
            }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    @Override public void onResult(UUID nodeId, GridNearGetResponse res) {
        for (IgniteInternalFuture<Map<K, V>> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.futureId().equals(res.miniId())) {
                    assert f.node().id().equals(nodeId);

                    f.onResult(res);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Map<K, V> res, Throwable err) {
        if (super.onDone(res, err)) {
            // Don't forget to clean up.
            if (trackable)
                cctx.mvcc().removeFuture(futId);

            cache().sendTtlUpdateRequest(expiryPlc);

            return true;
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * @param keys Keys.
     * @param mapped Mappings to check for duplicates.
     * @param topVer Topology version on which keys should be mapped.
     */
    private void map(
        Collection<KeyCacheObject> keys,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        final AffinityTopologyVersion topVer
    ) {
        Collection<ClusterNode> cacheNodes = CU.affinityNodes(cctx, topVer);

        if (cacheNodes.isEmpty()) {
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                "(all partition nodes left the grid) [topVer=" + topVer + ", cache=" + cctx.name() + ']'));

            return;
        }

        GridDhtTopologyFuture topFut = cctx.shared().exchange().lastFinishedFuture();

        Throwable err = topFut != null ? topFut.validateCache(cctx, recovery, true, null, keys) : null;

        if (err != null) {
            onDone(err);

            return;
        }

        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings = U.newHashMap(cacheNodes.size());

        final int keysSize = keys.size();

        Map<K, V> locVals = U.newHashMap(keysSize);

        boolean hasRmtNodes = false;

        // Assign keys to primary nodes.
        for (KeyCacheObject key : keys)
            hasRmtNodes |= map(key, mappings, locVals, topVer, mapped);

        if (isDone())
            return;

        if (!locVals.isEmpty())
            add(new GridFinishedFuture<>(locVals));

        if (hasRmtNodes) {
            if (!trackable) {
                trackable = true;

                cctx.mvcc().addFuture(this, futId);
            }
        }

        // Create mini futures.
        for (Map.Entry<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> entry : mappings.entrySet()) {
            final ClusterNode n = entry.getKey();

            final LinkedHashMap<KeyCacheObject, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo>> fut =
                    cache().getDhtAsync(n.id(),
                        -1,
                        mappedKeys,
                        false,
                        readThrough,
                        topVer,
                        subjId,
                        taskName == null ? 0 : taskName.hashCode(),
                        expiryPlc,
                        skipVals,
                        recovery);

                final Collection<Integer> invalidParts = fut.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<KeyCacheObject> remapKeys = new ArrayList<>(keysSize);

                    for (KeyCacheObject key : keys) {
                        if (key != null && invalidParts.contains(cctx.affinity().partition(key)))
                            remapKeys.add(key);
                    }

                    AffinityTopologyVersion updTopVer = cctx.shared().exchange().readyAffinityVersion();

                    assert updTopVer.compareTo(topVer) > 0 : "Got invalid partitions for local node but topology version did " +
                        "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                        ", invalidParts=" + invalidParts + ']';

                    // Remap recursively.
                    map(remapKeys, mappings, updTopVer);
                }

                // Add new future.
                add(fut.chain(new C1<IgniteInternalFuture<Collection<GridCacheEntryInfo>>, Map<K, V>>() {
                    @Override public Map<K, V> apply(IgniteInternalFuture<Collection<GridCacheEntryInfo>> fut) {
                        try {
                            return createResultMap(fut.get());
                        }
                        catch (Exception e) {
                            U.error(log, "Failed to get values from dht cache [fut=" + fut + "]", e);

                            onDone(e);

                            return Collections.emptyMap();
                        }
                    }
                }));
            }
            else {
                MiniFuture fut = new MiniFuture(n, mappedKeys, topVer,
                    CU.createBackupPostProcessingClosure(topVer, log, cctx, null, expiryPlc, readThrough, skipVals));

                GridCacheMessage req = new GridNearGetRequest(
                    cctx.cacheId(),
                    futId,
                    fut.futureId(),
                    null,
                    mappedKeys,
                    readThrough,
                    topVer,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forCreate() : -1L,
                    expiryPlc != null ? expiryPlc.forAccess() : -1L,
                    false,
                    skipVals,
                    cctx.deploymentEnabled(),
                    recovery);

                add(fut); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onNodeLeft((ClusterTopologyCheckedException)e);
                    else
                        fut.onResult(e);
                }
            }
        }
    }

    /**
     * @param mappings Mappings.
     * @param key Key to map.
     * @param locVals Local values.
     * @param topVer Topology version.
     * @param mapped Previously mapped.
     * @return {@code True} if has remote nodes.
     */
    @SuppressWarnings("ConstantConditions")
    private boolean map(
        KeyCacheObject key,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings,
        Map<K, V> locVals,
        AffinityTopologyVersion topVer,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped
    ) {
        int part = cctx.affinity().partition(key);

        List<ClusterNode> affNodes = cctx.affinity().nodesByPartition(part, topVer);

        if (affNodes.isEmpty()) {
            onDone(serverNotFoundError(topVer));

            return false;
        }

        boolean fastLocGet = (!forcePrimary || affNodes.get(0).isLocal()) &&
            cctx.reserveForFastLocalGet(part, topVer);

        if (fastLocGet) {
            try {
                if (localGet(topVer, key, part, locVals))
                    return false;
            }
            finally {
                cctx.releaseForFastLocalGet(part, topVer);
            }
        }

        ClusterNode node = cctx.selectAffinityNodeBalanced(affNodes, canRemap);

        if (node == null) {
            onDone(serverNotFoundError(topVer));

            return false;
        }

        boolean remote = !node.isLocal();

        LinkedHashMap<KeyCacheObject, Boolean> keys = mapped.get(node);

        if (keys != null && keys.containsKey(key)) {
            if (REMAP_CNT_UPD.incrementAndGet(this) > MAX_REMAP_CNT) {
                onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " +
                    MAX_REMAP_CNT + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                    U.toShortString(node) + ", mappings=" + mapped + ']'));

                return false;
            }
        }

        LinkedHashMap<KeyCacheObject, Boolean> old = mappings.get(node);

        if (old == null)
            mappings.put(node, old = new LinkedHashMap<>(3, 1f));

        old.put(key, false);

        return remote;
    }

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param part Partition.
     * @param locVals Local values.
     * @return {@code True} if there is no need to further search value.
     */
    private boolean localGet(AffinityTopologyVersion topVer, KeyCacheObject key, int part, Map<K, V> locVals) {
        assert cctx.affinityNode() : this;

        GridDhtCacheAdapter<K, V> cache = cache();

        boolean readNoEntry = cctx.readNoEntry(expiryPlc, false);
        boolean evt = !skipVals;

        while (true) {
            try {
                boolean skipEntry = readNoEntry;

                EntryGetResult getRes = null;
                CacheObject v = null;
                GridCacheVersion ver = null;

                if (readNoEntry) {
                    CacheDataRow row = cctx.offheap().read(cctx, key);

                    if (row != null) {
                        long expireTime = row.expireTime();

                        if (expireTime == 0 || expireTime > U.currentTimeMillis()) {
                            v = row.value();

                            if (needVer)
                                ver = row.version();

                            if (evt) {
                                cctx.events().readEvent(key,
                                    null,
                                    row.value(),
                                    subjId,
                                    taskName,
                                    !deserializeBinary);
                            }
                        }
                        else
                            skipEntry = false;
                    }
                }

                if (!skipEntry) {
                    GridCacheEntryEx entry = cache.entryEx(key);

                    // If our DHT cache do has value, then we peek it.
                    if (entry != null) {
                        boolean isNew = entry.isNewLocked();

                        if (needVer) {
                            getRes = entry.innerGetVersioned(
                                null,
                                null,
                                /*update-metrics*/false,
                                /*event*/evt,
                                subjId,
                                null,
                                taskName,
                                expiryPlc,
                                !deserializeBinary,
                                null);

                            if (getRes != null) {
                                v = getRes.value();
                                ver = getRes.version();
                            }
                        }
                        else {
                            v = entry.innerGet(
                                null,
                                null,
                                /*read-through*/false,
                                /*update-metrics*/false,
                                /*event*/evt,
                                subjId,
                                null,
                                taskName,
                                expiryPlc,
                                !deserializeBinary);
                        }

                        cache.context().evicts().touch(entry, topVer);

                        // Entry was not in memory or in swap, so we remove it from cache.
                        if (v == null) {
                            if (isNew && entry.markObsoleteIfEmpty(ver))
                                cache.removeEntry(entry);
                        }
                    }
                }

                if (v != null) {
                    cctx.addResult(locVals,
                        key,
                        v,
                        skipVals,
                        keepCacheObjects,
                        deserializeBinary,
                        true,
                        getRes,
                        ver,
                        0,
                        0,
                        needVer);

                    return true;
                }

                boolean topStable = cctx.isReplicated() || topVer.equals(cctx.topology().lastTopologyChangeVersion());

                // Entry not found, do not continue search if topology did not change and there is no store.
                if (!cctx.readThroughConfigured() && (topStable || partitionOwned(part))) {
                    if (!skipVals && cctx.statisticsEnabled())
                        cache.metrics0().onRead(false);

                    return true;
                }

                return false;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op, will retry.
            }
            catch (GridDhtInvalidPartitionException ignored) {
                return false;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                return true;
            }
        }
    }

    /**
     * @return Near cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return cctx.dht();
    }

    /**
     * @param infos Entry infos.
     * @return Result map.
     */
    private Map<K, V> createResultMap(Collection<GridCacheEntryInfo> infos) {
        int keysSize = infos.size();

        if (keysSize != 0) {
            Map<K, V> map = new GridLeanMap<>(keysSize);

            for (GridCacheEntryInfo info : infos) {
                assert skipVals == (info.value() == null);

                cctx.addResult(map,
                    info.key(),
                    info.value(),
                    skipVals,
                    keepCacheObjects,
                    deserializeBinary,
                    false,
                    needVer ? info.version() : null,
                    0,
                    0);
            }

            return map;
        }

        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @SuppressWarnings("unchecked")
            @Override public String apply(IgniteInternalFuture<?> f) {
                if (isMini(f)) {
                    return "[node=" + ((MiniFuture)f).node().id() +
                        ", loc=" + ((MiniFuture)f).node().isLocal() +
                        ", done=" + f.isDone() + "]";
                }
                else
                    return "[loc=true, done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridPartitionedGetFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Map<K, V>> {
        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node ID. */
        private final ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private final LinkedHashMap<KeyCacheObject, Boolean> keys;

        /** Topology version on which this future was mapped. */
        private final AffinityTopologyVersion topVer;

        /** Post processing closure. */
        private final IgniteInClosure<Collection<GridCacheEntryInfo>> postProcessingClos;

        /** {@code True} if remapped after node left. */
        private boolean remapped;

        /**
         * @param node Node.
         * @param keys Keys.
         * @param topVer Topology version.
         * @param postProcessingClos Post processing closure.
         */
        MiniFuture(ClusterNode node, LinkedHashMap<KeyCacheObject, Boolean> keys, AffinityTopologyVersion topVer,
            @Nullable IgniteInClosure<Collection<GridCacheEntryInfo>> postProcessingClos) {
            this.node = node;
            this.keys = keys;
            this.topVer = topVer;
            this.postProcessingClos = postProcessingClos;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
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
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Failure exception.
         */
        @SuppressWarnings("UnusedParameters")
        synchronized void onNodeLeft(ClusterTopologyCheckedException e) {
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
                AffinityTopologyVersion updTopVer =
                    new AffinityTopologyVersion(Math.max(topVer.topologyVersion() + 1, cctx.discovery().topologyVersion()));

                cctx.affinity().affinityReadyFuture(updTopVer).listen(
                    new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                            try {
                                // Remap.
                                map(keys.keySet(), F.t(node, keys), fut.get());

                                onDone(Collections.<K, V>emptyMap());
                            }
                            catch (IgniteCheckedException e) {
                                GridPartitionedGetFuture.this.onDone(e);
                            }
                        }
                    }
                );
            }
        }

        /**
         * @param res Result callback.
         */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void onResult(final GridNearGetResponse res) {
            final Collection<Integer> invalidParts = res.invalidPartitions();

            // If error happened on remote node, fail the whole future.
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            // Remap invalid partitions.
            if (!F.isEmpty(invalidParts)) {
                AffinityTopologyVersion rmtTopVer = res.topologyVersion();

                assert !rmtTopVer.equals(AffinityTopologyVersion.ZERO);

                if (rmtTopVer.compareTo(topVer) <= 0) {
                    // Fail the whole get future.
                    onDone(new IgniteCheckedException("Failed to process invalid partitions response (remote node reported " +
                        "invalid partitions but remote topology version does not differ from local) " +
                        "[topVer=" + topVer + ", rmtTopVer=" + rmtTopVer + ", invalidParts=" + invalidParts +
                        ", nodeId=" + node.id() + ']'));

                    return;
                }

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

                // Need to wait for next topology version to remap.
                IgniteInternalFuture<AffinityTopologyVersion> topFut = cctx.affinity().affinityReadyFuture(rmtTopVer);

                topFut.listen(new CIX1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @SuppressWarnings("unchecked")
                    @Override public void applyx(
                        IgniteInternalFuture<AffinityTopologyVersion> fut) throws IgniteCheckedException {
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
        private void postProcessResult(final GridNearGetResponse res) {
            if (postProcessingClos != null)
                postProcessingClos.apply(res.entries());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}

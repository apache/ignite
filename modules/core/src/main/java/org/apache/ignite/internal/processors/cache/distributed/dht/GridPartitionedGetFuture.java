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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.F;
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
    /** Transaction label. */
    protected final String txLbl;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** Explicit predefined single mapping (backup or primary). */
    protected final ClusterNode affNode;

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
     * @param txLbl Transaction label.
     * @param mvccSnapshot Mvcc snapshot.
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
        boolean keepCacheObjects,
        @Nullable String txLbl,
        @Nullable MvccSnapshot mvccSnapshot,
        ClusterNode affNode
    ) {
        super(
            cctx,
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
            recovery
        );

        assert (mvccSnapshot == null) == !cctx.mvccEnabled();

        this.mvccSnapshot = mvccSnapshot;
        this.txLbl = txLbl;
        this.affNode = affNode;

        initLogger(GridPartitionedGetFuture.class);
    }

    /**
     * @return Mvcc snapshot if mvcc is enabled for cache.
     */
    @Nullable private MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * Initializes future.
     *
     * @param topVer Topology version.
     */
    public void init(AffinityTopologyVersion topVer) {
        AffinityTopologyVersion lockedTopVer = cctx.shared().lockedTopologyVersion(null);

        // Can not remap if we in transaction and locked on some topology.
        if (lockedTopVer != null) {
            topVer = lockedTopVer;

            canRemap = false;
        }
        else {
            // Use affinity topology version if constructor version is not specify.
            topVer = topVer.topologyVersion() > 0 ? topVer : cctx.affinity().affinityTopologyVersion();
        }

        map(keys, Collections.emptyMap(), topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Map<K, V> res, Throwable err) {
        if (super.onDone(res, err)) {
            if (trackable)
                cctx.mvcc().removeFuture(futId);

            cache().sendTtlUpdateRequest(expiryPlc);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * @param keys Keys.
     * @param mapped Mappings to check for duplicates.
     * @param topVer Topology version on which keys should be mapped.
     */
    @Override protected void map(
        Collection<KeyCacheObject> keys,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        AffinityTopologyVersion topVer
    ) {
        GridDhtPartitionsExchangeFuture fut = cctx.shared().exchange().lastTopologyFuture();

        // Finished DHT future is required for topology validation.
        if (!fut.isDone()) {
            if (fut.initialVersion().after(topVer) || (fut.exchangeActions() != null && fut.exchangeActions().hasStop()))
                fut = cctx.shared().exchange().lastFinishedFuture();
            else {
                fut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        if (fut.error() != null)
                            onDone(fut.error());
                        else {
                            cctx.closures().runLocalSafe(new GridPlainRunnable() {
                                @Override public void run() {
                                    map(keys, mapped, topVer);
                                }
                            }, true);
                        }
                    }
                });

                return;
            }
        }

        Collection<ClusterNode> cacheNodes = CU.affinityNodes(cctx, topVer);

        validate(cacheNodes, fut);

        // Future can be already done with some exception.
        if (isDone())
            return;

        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings = U.newHashMap(cacheNodes.size());

        int keysSize = keys.size();

        // Map for local (key,value) pairs.
        Map<K, V> locVals = U.newHashMap(keysSize);

        // True if we have remote nodes after key mapping complete.
        boolean hasRmtNodes = false;

        // Assign keys to nodes.
        for (KeyCacheObject key : keys)
            hasRmtNodes |= map(key, topVer, mappings, mapped, locVals);

        // Future can be alredy done with some exception.
        if (isDone())
            return;

        // Add local read (key,value) in result.
        if (!locVals.isEmpty())
            add(new GridFinishedFuture<>(locVals));

        // If we have remote nodes in mapping we should registrate future in mvcc manager.
        if (hasRmtNodes)
            registrateFutureInMvccManager(this);

        // Create mini futures after mapping to remote nodes.
        for (Map.Entry<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> entry : mappings.entrySet()) {
            // Node for request.
            ClusterNode n = entry.getKey();

            // Keys for request.
            LinkedHashMap<KeyCacheObject, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                GridDhtFuture<Collection<GridCacheEntryInfo>> fut0 = cache()
                    .getDhtAsync(
                        n.id(),
                        -1,
                        mappedKeys,
                        false,
                        readThrough,
                        topVer,
                        subjId,
                        taskName == null ? 0 : taskName.hashCode(),
                        expiryPlc,
                        skipVals,
                        recovery,
                        txLbl,
                        mvccSnapshot()
                    );

                Collection<Integer> invalidParts = fut0.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<KeyCacheObject> remapKeys = new ArrayList<>(keysSize);

                    for (KeyCacheObject key : keys) {
                        int part = cctx.affinity().partition(key);

                        if (key != null && invalidParts.contains(part)) {
                            addNodeAsInvalid(n, part, topVer);

                            remapKeys.add(key);
                        }
                    }

                    AffinityTopologyVersion updTopVer = cctx.shared().exchange().readyAffinityVersion();

                    // Remap recursively.
                    map(remapKeys, mappings, updTopVer);
                }

                // Add new future.
                add(fut0.chain(f -> {
                    try {
                        return createResultMap(f.get());
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to get values from dht cache [fut=" + fut0 + "]", e);

                        onDone(e);

                        return Collections.emptyMap();
                    }
                }));
            }
            else {
                MiniFuture miniFut = new MiniFuture(n, mappedKeys, topVer);

                GridCacheMessage req = miniFut.createGetRequest(futId);

                add(miniFut); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        miniFut.onNodeLeft((ClusterTopologyCheckedException)e);
                    else
                        miniFut.onResult(e);
                }
            }
        }

        markInitialized();
    }

    /**
     * @param nodesToKeysMapping Mappings.
     * @param key Key to map.
     * @param locVals Local values.
     * @param topVer Topology version.
     * @param missedNodesToKeysMapping Previously mapped.
     * @return {@code True} if has remote nodes.
     */
    private boolean map(
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> nodesToKeysMapping,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> missedNodesToKeysMapping,
        Map<K, V> locVals
    ) {
        ClusterNode node;

        int part = cctx.affinity().partition(key);

        Set<ClusterNode> invalidNodeSet = getInvalidNodes(part, topVer);

        List<ClusterNode> affNodes = cctx.affinity().nodesByPartition(part, topVer);

        if (affNode != null) {
            if (invalidNodeSet.contains(affNode) || !cctx.discovery().alive(affNode)) {
                onDone(Collections.emptyMap());

                return false;
            }

            node = affNodes.contains(affNode) ? affNode : null;
        }
        else {
            // Failed if none affinity node found.
            if (affNodes.isEmpty()) {
                onDone(serverNotFoundError(part, topVer));

                return false;
            }

            // Try to read key localy if we can.
            if (tryLocalGet(key, part, topVer, affNodes, locVals))
                return false;

            node = cctx.selectAffinityNodeBalanced(affNodes, invalidNodeSet, part, canRemap, forcePrimary);
        }

        // Failed if none remote node found.
        if (node == null) {
            onDone(serverNotFoundError(part, topVer));

            return false;
        }

        // The node still can be local, see details implementation of #tryLocalGet().
        boolean remote = !node.isLocal();

        // Check retry counter, bound for avoid inifinit remap.
        if (!checkRetryPermits(key, node, missedNodesToKeysMapping))
            return false;

        addNodeMapping(key, node, nodesToKeysMapping);

        return remote;
    }

    /**
     *
     * @param key Key.
     * @param node Mapped node.
     * @param mappings Full node mapping.
     */
    private void addNodeMapping(
        KeyCacheObject key,
        ClusterNode node,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings
    ) {
        LinkedHashMap<KeyCacheObject, Boolean> old = mappings.get(node);

        if (old == null)
            mappings.put(node, old = new LinkedHashMap<>(3, 1f));

        old.put(key, false);
    }

    /**
     *
     * @param key Key.
     * @param part Partition.
     * @param topVer Topology version.
     * @param affNodes Affynity nodes.
     * @param locVals Map for local (key,value) pairs.
     */
    private boolean tryLocalGet(
        KeyCacheObject key,
        int part,
        AffinityTopologyVersion topVer,
        List<ClusterNode> affNodes,
        Map<K, V> locVals
    ) {
        // Local get cannot be used with MVCC as local node can contain some visible version which is not latest.
        boolean fastLocGet = !cctx.mvccEnabled() &&
            (!forcePrimary || affNodes.get(0).isLocal()) &&
            cctx.reserveForFastLocalGet(part, topVer);

        if (fastLocGet) {
            try {
                if (localGet(topVer, key, part, locVals))
                    return true;
            }
            finally {
                cctx.releaseForFastLocalGet(part, topVer);
            }
        }

        return false;
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
            cctx.shared().database().checkpointReadLock();

            try {
                boolean skipEntry = readNoEntry;

                EntryGetResult getRes = null;
                CacheObject v = null;
                GridCacheVersion ver = null;

                if (readNoEntry) {
                    KeyCacheObject key0 = (KeyCacheObject)cctx.cacheObjects().prepareForCache(key, cctx);

                    CacheDataRow row = cctx.mvccEnabled() ?
                        cctx.offheap().mvccRead(cctx, key0, mvccSnapshot()) :
                        cctx.offheap().read(cctx, key0);

                    if (row != null) {
                        long expireTime = row.expireTime();

                        if (expireTime == 0 || expireTime > U.currentTimeMillis()) {
                            v = row.value();

                            if (needVer)
                                ver = row.version();

                            if (evt) {
                                cctx.events().readEvent(key,
                                    null,
                                    txLbl,
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

                        entry.touch();

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
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param cacheNodes Cache affynity nodes.
     * @param topFut Topology future.
     */
    private void validate(Collection<ClusterNode> cacheNodes, GridDhtTopologyFuture topFut) {
        assert topFut.isDone() : topFut;

        Throwable err = topFut.validateCache(cctx, recovery, true, null, keys);

        if (err != null) {
            onDone(err);

            return;
        }

        if (cacheNodes.isEmpty())
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                "(all partition nodes left the grid) [topVer=" + topFut.topologyVersion() + ", cache=" + cctx.name() + ']'));
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
        return S.toString(GridPartitionedGetFuture.class, this,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends AbstractMiniFuture {
        /**
         * @param node Node.
         * @param keys Keys.
         * @param topVer Topology version.
         */
        public MiniFuture(
            ClusterNode node,
            LinkedHashMap<KeyCacheObject, Boolean> keys,
            AffinityTopologyVersion topVer
        ) {
            super(node, keys, topVer);
        }

        /** {@inheritDoc} */
        @Override protected GridNearGetRequest createGetRequest0(IgniteUuid rootFutId, IgniteUuid futId) {
            return new GridNearGetRequest(
                cctx.cacheId(),
                rootFutId,
                futId,
                null,
                keys,
                readThrough,
                topVer,
                subjId,
                taskName == null ? 0 : taskName.hashCode(),
                expiryPlc != null ? expiryPlc.forCreate() : -1L,
                expiryPlc != null ? expiryPlc.forAccess() : -1L,
                false,
                skipVals,
                cctx.deploymentEnabled(),
                recovery,
                txLbl,
                mvccSnapshot()
            );
        }

        /** {@inheritDoc} */
        @Override protected Map<K, V> createResultMap(Collection<GridCacheEntryInfo> entries) {
            return GridPartitionedGetFuture.this.createResultMap(entries);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}

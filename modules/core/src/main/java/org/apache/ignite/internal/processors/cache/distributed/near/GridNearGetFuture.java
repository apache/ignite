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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.CacheDistributedGetFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public final class GridNearGetFuture<K, V> extends CacheDistributedGetFutureAdapter<K, V> {
    /** Transaction. */
    private final IgniteTxLocalEx tx;

    /** */
    private GridCacheVersion ver;

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param forcePrimary If {@code true} get will be performed on primary node even if
     *      called on backup node.
     * @param tx Transaction.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObjects Keep cache objects flag.
     */
    public GridNearGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean forcePrimary,
        @Nullable IgniteTxLocalEx tx,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        boolean keepCacheObjects,
        boolean recovery
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

        assert !F.isEmpty(keys);

        this.tx = tx;

        ver = tx == null ? cctx.versions().next() : tx.xidVersion();

        initLogger(GridNearGetFuture.class);
    }

    /**
     * Initializes future.
     *
     * @param topVer Topology version.
     */
    public void init(@Nullable AffinityTopologyVersion topVer) {
        AffinityTopologyVersion lockedTopVer = cctx.shared().lockedTopologyVersion(null);

        if (lockedTopVer != null) {
            canRemap = false;

            map(keys, Collections.emptyMap(), lockedTopVer);
        }
        else {
            AffinityTopologyVersion mapTopVer = topVer;

            if (mapTopVer == null)
                mapTopVer = tx == null ? cctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

            map(keys, Collections.emptyMap(), mapTopVer);
        }

        markInitialized();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Map<K, V> res, Throwable err) {
        if (super.onDone(res, err)) {
            // Don't forget to clean up.
            if (trackable)
                cctx.mvcc().removeFuture(futId);

            cache().dht().sendTtlUpdateRequest(expiryPlc);

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
     * @param topVer Topology version to map on.
     */
    @Override protected void map(
        Collection<KeyCacheObject> keys,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        AffinityTopologyVersion topVer
    ) {
        Collection<ClusterNode> affNodes = CU.affinityNodes(cctx, topVer);

        if (affNodes.isEmpty()) {
            assert !cctx.affinityNode();

            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for near-only cache (all partition " +
                "nodes left the grid)."));

            return;
        }

        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings = U.newHashMap(affNodes.size());

        Map<KeyCacheObject, GridNearCacheEntry> savedEntries = null;

        {
            boolean success = false;

            try {
                // Assign keys to primary nodes.
                for (KeyCacheObject key : keys)
                    savedEntries = map(key, topVer, mappings, mapped, savedEntries);

                success = true;
            }
            finally {
                // Exception has been thrown, must release reserved near entries.
                if (!success) {
                    GridCacheVersion obsolete = cctx.versions().next(topVer);

                    if (savedEntries != null) {
                        for (GridNearCacheEntry reserved : savedEntries.values()) {
                            reserved.releaseEviction();

                            if (reserved.markObsolete(obsolete))
                                reserved.context().cache().removeEntry(reserved);
                        }
                    }
                }
            }
        }

        if (isDone())
            return;

        Map<KeyCacheObject, GridNearCacheEntry> saved =
            savedEntries != null ? savedEntries : Collections.emptyMap();

        int keysSize = keys.size();

        // Create mini futures.
        for (Map.Entry<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> entry : mappings.entrySet()) {
            ClusterNode n = entry.getKey();

            LinkedHashMap<KeyCacheObject, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                GridDhtFuture<Collection<GridCacheEntryInfo>> fut = dht()
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
                        null,
                        null
                    ); // TODO IGNITE-7371

                Collection<Integer> invalidParts = fut.invalidPartitions();

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
                add(fut.chain(f -> {
                    try {
                        return loadEntries(n.id(), mappedKeys.keySet(), f.get(), saved, topVer);
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to get values from dht cache [fut=" + fut + "]", e);

                        onDone(e);

                        return Collections.emptyMap();
                    }
                }));
            }
            else {
                registrateFutureInMvccManager(this);

                MiniFuture miniFuture = new MiniFuture(n, mappedKeys, saved, topVer);

                GridNearGetRequest req = miniFuture.createGetRequest(futId);

                add(miniFuture); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        miniFuture.onNodeLeft((ClusterTopologyCheckedException)e);
                    else
                        miniFuture.onResult(e);
                }
            }
        }
    }

    /**
     * @param mappings Mappings.
     * @param key Key to map.
     * @param topVer Topology version
     * @param mapped Previously mapped.
     * @param saved Reserved near cache entries.
     * @return Map.
     */
    private Map<KeyCacheObject, GridNearCacheEntry> map(
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        Map<KeyCacheObject, GridNearCacheEntry> saved
    ) {
        int part = cctx.affinity().partition(key);

        List<ClusterNode> affNodes = cctx.affinity().nodesByPartition(part, topVer);

        if (affNodes.isEmpty()) {
            onDone(serverNotFoundError(part, topVer));

            return null;
        }

        final GridNearCacheAdapter near = cache();

        // Allow to get cached value from the local node.
        boolean allowLocRead = !forcePrimary || cctx.localNode().equals(affNodes.get(0));

        while (true) {
            GridNearCacheEntry entry = allowLocRead ? (GridNearCacheEntry)near.peekEx(key) : null;

            try {
                CacheObject v = null;
                GridCacheVersion ver = null;

                boolean isNear = entry != null;

                // First we peek into near cache.
                if (isNear) {
                    if (needVer) {
                        EntryGetResult res = entry.innerGetVersioned(
                            null,
                            null,
                            /*update-metrics*/true,
                            /*event*/!skipVals,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            !deserializeBinary,
                            null);

                        if (res != null) {
                            v = res.value();
                            ver = res.version();
                        }
                    }
                    else {
                        v = entry.innerGet(
                            null,
                            tx,
                            /*read-through*/false,
                            /*metrics*/true,
                            /*events*/!skipVals,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            !deserializeBinary);
                    }
                }

                if (v == null) {
                    boolean fastLocGet = allowLocRead && cctx.reserveForFastLocalGet(part, topVer);

                    if (fastLocGet) {
                        try {
                            if (localDhtGet(key, part, topVer, isNear))
                                break;
                        }
                        catch (IgniteException ex) {
                            onDone(ex);

                            return saved;
                        }
                        finally {
                            cctx.releaseForFastLocalGet(part, topVer);
                        }
                    }

                    Set<ClusterNode> invalidNodesSet = getInvalidNodes(part, topVer);

                    ClusterNode affNode = cctx.selectAffinityNodeBalanced(affNodes, invalidNodesSet, part, canRemap);

                    if (affNode == null) {
                        onDone(serverNotFoundError(part, topVer));

                        return saved;
                    }

                    if (cctx.statisticsEnabled() && !skipVals && !affNode.isLocal() && !isNear)
                        cache().metrics0().onRead(false);

                    if (!checkRetryPermits(key,affNode,mapped))
                        return saved;

                    if (!affNodes.contains(cctx.localNode())) {
                        GridNearCacheEntry nearEntry = entry != null ? entry : near.entryExx(key, topVer);

                        nearEntry.reserveEviction();

                        entry = null;

                        if (saved == null)
                            saved = U.newHashMap(3);

                        saved.put(key, nearEntry);
                    }

                    // Don't add reader if transaction acquires lock anyway to avoid deadlock.
                    boolean addRdr = tx == null || tx.optimistic();

                    if (!addRdr && tx.readCommitted() && !tx.writeSet().contains(cctx.txKey(key)))
                        addRdr = true;

                    LinkedHashMap<KeyCacheObject, Boolean> old = mappings.get(affNode);

                    if (old == null)
                        mappings.put(affNode, old = new LinkedHashMap<>(3, 1f));

                    old.put(key, addRdr);
                }
                else
                    addResult(key, v, ver);

                break;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // Retry.
            }
            finally {
                if (entry != null && tx == null)
                    entry.touch();
            }
        }

        return saved;
    }

    /**
     * @param key Key.
     * @param part Partition.
     * @param topVer Topology version.
     * @param nearRead {@code True} if already tried to read from near cache.
     * @return {@code True} if there is no need to further search value.
     */
    private boolean localDhtGet(
        KeyCacheObject key,
        int part,
        AffinityTopologyVersion topVer,
        boolean nearRead
    ) {
        GridDhtCacheAdapter<K, V> dht = cache().dht();

        assert dht.context().affinityNode() : this;

        while (true) {
            GridCacheEntryEx dhtEntry = null;

            try {
                dhtEntry = dht.entryEx(key);

                CacheObject v = null;

                // If near cache does not have value, then we peek DHT cache.
                if (dhtEntry != null) {
                    boolean isNew = dhtEntry.isNewLocked() || !dhtEntry.valid(topVer);

                    if (needVer) {
                        EntryGetResult res = dhtEntry.innerGetVersioned(
                            null,
                            null,
                            /*update-metrics*/false,
                            /*event*/!nearRead && !skipVals,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            !deserializeBinary,
                            null);

                        if (res != null) {
                            v = res.value();
                            ver = res.version();
                        }
                    }
                    else {
                        v = dhtEntry.innerGet(
                            null,
                            tx,
                            /*read-through*/false,
                            /*update-metrics*/false,
                            /*events*/!nearRead && !skipVals,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            !deserializeBinary);
                    }

                    // Entry was not in memory or in swap, so we remove it from cache.
                    if (v == null && isNew && dhtEntry.markObsoleteIfEmpty(ver))
                        dht.removeEntry(dhtEntry);
                }

                if (v != null) {
                    if (cctx.statisticsEnabled() && !skipVals)
                        cache().metrics0().onRead(true);

                    addResult(key, v, ver);

                    return true;
                }
                else {
                    boolean topStable = cctx.isReplicated() || topVer.equals(cctx.topology().lastTopologyChangeVersion());

                    // Entry not found, do not continue search if topology did not change and there is no store.
                    return !cctx.readThroughConfigured() && (topStable || partitionOwned(part));
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                // Retry.
            }
            catch (GridDhtInvalidPartitionException ignored) {
                return false;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                return false;
            }
            finally {
                if (dhtEntry != null)
                    // Near cache is enabled, so near entry will be enlisted in the transaction.
                    // Always touch DHT entry in this case.
                    dhtEntry.touch();
            }
        }
    }

    /**
     * @param key Key.
     * @param v Value.
     * @param ver Version.
     */
    private void addResult(KeyCacheObject key, CacheObject v, GridCacheVersion ver) {
        if (keepCacheObjects) {
            K key0 = (K)key;
            V val0 = needVer ?
                (V)new EntryGetResult(skipVals ? true : v, ver) :
                (V)(skipVals ? true : v);

            add(new GridFinishedFuture<>(Collections.singletonMap(key0, val0)));
        }
        else {
            K key0 = (K)cctx.unwrapBinaryIfNeeded(key, !deserializeBinary, false);
            V val0 = needVer ?
                (V)new EntryGetResult(!skipVals ?
                    (V)cctx.unwrapBinaryIfNeeded(v, !deserializeBinary, false) :
                    (V)Boolean.TRUE, ver) :
                !skipVals ?
                    (V)cctx.unwrapBinaryIfNeeded(v, !deserializeBinary, false) :
                    (V)Boolean.TRUE;

            add(new GridFinishedFuture<>(Collections.singletonMap(key0, val0)));
        }
    }

    /**
     * @return Near cache.
     */
    private GridNearCacheAdapter<K, V> cache() {
        return (GridNearCacheAdapter<K, V>)cctx.cache();
    }

    /**
     * @return DHT cache.
     */
    private GridDhtCacheAdapter<K, V> dht() {
        return cache().dht();
    }

    /**
     * @param nodeId Node id.
     * @param keys Keys.
     * @param infos Entry infos.
     * @param savedEntries Saved entries.
     * @param topVer Topology version
     * @return Result map.
     */
    private Map<K, V> loadEntries(
        UUID nodeId,
        Collection<KeyCacheObject> keys,
        Collection<GridCacheEntryInfo> infos,
        Map<KeyCacheObject, GridNearCacheEntry> savedEntries,
        AffinityTopologyVersion topVer
    ) {
        boolean empty = F.isEmpty(keys);

        Map<K, V> map = empty ? Collections.<K, V>emptyMap() : new GridLeanMap<K, V>(keys.size());

        if (!empty) {
            boolean atomic = cctx.atomic();

            GridCacheVersion ver = atomic ? null : F.isEmpty(infos) ? null : cctx.versions().next();

            for (GridCacheEntryInfo info : infos) {
                try {
                    info.unmarshalValue(cctx, cctx.deploy().globalLoader());

                    // Entries available locally in DHT should not be loaded into near cache for reading.
                    if (!cctx.affinity().keyLocalNode(info.key(), cctx.affinity().affinityTopologyVersion())) {
                        GridNearCacheEntry entry = savedEntries.get(info.key());

                        if (entry == null)
                            entry = cache().entryExx(info.key(), topVer);

                        // Load entry into cache.
                        entry.loadedValue(tx,
                            nodeId,
                            info.value(),
                            atomic ? info.version() : ver,
                            info.version(),
                            info.ttl(),
                            info.expireTime(),
                            true,
                            !deserializeBinary,
                            topVer,
                            subjId);
                    }

                    CacheObject val = info.value();
                    KeyCacheObject key = info.key();

                    assert skipVals == (info.value() == null);

                    cctx.addResult(map,
                        key,
                        val,
                        skipVals,
                        keepCacheObjects,
                        deserializeBinary,
                        false,
                        needVer ? info.version() : null,
                        0,
                        0);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while processing get response (will not retry).");
                }
                catch (Exception e) {
                    // Fail.
                    onDone(e);

                    return Collections.emptyMap();
                }
            }
        }

        return map;
    }

    /**
     * @param keys Keys.
     * @param saved Saved entries.
     * @param topVer Topology version.
     */
    private void releaseEvictions(
        Collection<KeyCacheObject> keys,
        Map<KeyCacheObject, GridNearCacheEntry> saved,
        AffinityTopologyVersion topVer
    ) {
        for (KeyCacheObject key : keys) {
            GridNearCacheEntry entry = saved.get(key);

            if (entry != null) {
                entry.releaseEviction();

                if (tx == null)
                    entry.touch();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetFuture.class, this,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single node as opposed to multiple nodes.
     */
    private class MiniFuture extends AbstractMiniFuture {
        /** Saved entry versions. */
        private final Map<KeyCacheObject, GridNearCacheEntry> savedEntries;

        /**
         * @param node Node.
         * @param keys Keys.
         * @param savedEntries Saved entries.
         * @param topVer Topology version.
         */
        MiniFuture(
            ClusterNode node,
            LinkedHashMap<KeyCacheObject, Boolean> keys,
            Map<KeyCacheObject, GridNearCacheEntry> savedEntries,
            AffinityTopologyVersion topVer
        ) {
            super(node, keys, topVer);
            this.savedEntries = savedEntries;
        }

        /** {@inheritDoc} */
        @Override protected GridNearGetRequest createGetRequest0(IgniteUuid rootFutId, IgniteUuid futId) {
            return new GridNearGetRequest(
                cctx.cacheId(),
                rootFutId,
                futId,
                ver,
                keys,
                readThrough,
                topVer,
                subjId,
                taskName == null ? 0 : taskName.hashCode(),
                expiryPlc != null ? expiryPlc.forCreate() : -1L,
                expiryPlc != null ? expiryPlc.forAccess() : -1L,
                true,
                skipVals,
                cctx.deploymentEnabled(),
                recovery,
                null,
                null
            ); // TODO IGNITE-7371
        }

        /** {@inheritDoc} */
        @Override protected Map<K, V> createResultMap(Collection<GridCacheEntryInfo> entries) {
            return loadEntries(node.id(), keys.keySet(), entries, savedEntries, topVer);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Map<K, V> res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                releaseEvictions(keys.keySet(), savedEntries, topVer);

                return true;
            }
            else
                return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}

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
import org.apache.ignite.internal.processors.cache.distributed.dht.CacheDistributedGetFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public final class GridNearGetFuture<K, V> extends CacheDistributedGetFutureAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Transaction. */
    private IgniteTxLocalEx tx;

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
     * @param canRemap Flag indicating whether future can be remapped on a newer topology version.
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
        boolean canRemap,
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
            canRemap,
            needVer,
            keepCacheObjects);

        assert !F.isEmpty(keys);

        this.tx = tx;

        futId = IgniteUuid.randomUuid();

        ver = tx == null ? cctx.versions().next() : tx.xidVersion();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridNearGetFuture.class);
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

            map(keys, Collections.<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>>emptyMap(), lockedTopVer);
        }
        else {
            AffinityTopologyVersion mapTopVer = topVer;

            if (mapTopVer == null) {
                mapTopVer = tx == null ?
                    (canRemap ? cctx.affinity().affinityTopologyVersion() : cctx.shared().exchange().readyAffinityVersion()) :
                    tx.topologyVersion();
            }

            map(keys, Collections.<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>>emptyMap(), mapTopVer);
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

                    f.onNodeLeft();
                }
            }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearGetResponse res) {
        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.futureId().equals(res.miniId())) {
                    assert f.node().id().equals(nodeId);

                    f.onResult(res);
                }
            }
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
     * @param topVer Topology version to map on.
     */
    private void map(
        Collection<KeyCacheObject> keys,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        final AffinityTopologyVersion topVer
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
                    savedEntries = map(key, mappings, topVer, mapped, savedEntries);

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

        final Map<KeyCacheObject, GridNearCacheEntry> saved = savedEntries != null ? savedEntries :
            Collections.<KeyCacheObject, GridNearCacheEntry>emptyMap();

        final int keysSize = keys.size();

        // Create mini futures.
        for (Map.Entry<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> entry : mappings.entrySet()) {
            final ClusterNode n = entry.getKey();

            final LinkedHashMap<KeyCacheObject, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo>> fut =
                    dht().getDhtAsync(n.id(),
                        -1,
                        mappedKeys,
                        readThrough,
                        topVer,
                        subjId,
                        taskName == null ? 0 : taskName.hashCode(),
                        expiryPlc,
                        skipVals);

                final Collection<Integer> invalidParts = fut.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<KeyCacheObject> remapKeys = new ArrayList<>(keysSize);

                    for (KeyCacheObject key : keys) {
                        if (key != null && invalidParts.contains(cctx.affinity().partition(key)))
                            remapKeys.add(key);
                    }

                    AffinityTopologyVersion updTopVer = cctx.discovery().topologyVersionEx();

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
                            return loadEntries(n.id(), mappedKeys.keySet(), fut.get(), saved, topVer);
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
                if (!trackable) {
                    trackable = true;

                    cctx.mvcc().addFuture(this, futId);
                }

                MiniFuture fut = new MiniFuture(n, mappedKeys, saved, topVer);

                GridCacheMessage req = new GridNearGetRequest(
                    cctx.cacheId(),
                    futId,
                    fut.futureId(),
                    ver,
                    mappedKeys,
                    readThrough,
                    topVer,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forCreate() : -1L,
                    expiryPlc != null ? expiryPlc.forAccess() : -1L,
                    skipVals,
                    cctx.deploymentEnabled());

                add(fut); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onNodeLeft();
                    else
                        fut.onResult(e);
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
    @SuppressWarnings("unchecked")
    private Map<KeyCacheObject, GridNearCacheEntry> map(
        KeyCacheObject key,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mappings,
        AffinityTopologyVersion topVer,
        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> mapped,
        Map<KeyCacheObject, GridNearCacheEntry> saved
    ) {
        int part = cctx.affinity().partition(key);

        List<ClusterNode> affNodes = cctx.affinity().nodesByPartition(part, topVer);

        if (affNodes.isEmpty()) {
            onDone(serverNotFoundError(topVer));

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
                            /*swap*/true,
                            /*unmarshal*/true,
                            /**update-metrics*/true,
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
                            /*swap*/false,
                            /*read-through*/false,
                            /*metrics*/true,
                            /*events*/!skipVals,
                            /*temporary*/false,
                            subjId,
                            null,
                            taskName,
                            expiryPlc,
                            !deserializeBinary);
                    }
                }

                if (v == null) {
                    boolean fastLocGet = allowLocRead && cctx.allowFastLocalRead(part, affNodes, topVer);

                    if (fastLocGet && localDhtGet(key, part, topVer, isNear))
                        break;

                    ClusterNode affNode = affinityNode(affNodes);

                    if (affNode == null) {
                        onDone(serverNotFoundError(topVer));

                        return saved;
                    }

                    if (cctx.cache().configuration().isStatisticsEnabled() && !skipVals && !affNode.isLocal())
                        cache().metrics0().onRead(false);

                    LinkedHashMap<KeyCacheObject, Boolean> keys = mapped.get(affNode);

                    if (keys != null && keys.containsKey(key)) {
                        if (REMAP_CNT_UPD.incrementAndGet(this) > MAX_REMAP_CNT) {
                            onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " +
                                MAX_REMAP_CNT + " attempts (key got remapped to the same node) " +
                                "[key=" + key + ", node=" + U.toShortString(affNode) + ", mappings=" + mapped + ']'));

                            return saved;
                        }
                    }

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
                    cctx.evicts().touch(entry, topVer);
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
    private boolean localDhtGet(KeyCacheObject key,
        int part,
        AffinityTopologyVersion topVer,
        boolean nearRead) {
        GridDhtCacheAdapter<K, V> dht = cache().dht();

        assert dht.context().affinityNode() : this;

        while (true) {
            GridCacheEntryEx dhtEntry = null;

            try {
                dhtEntry = dht.context().isSwapOrOffheapEnabled() ? dht.entryEx(key) : dht.peekEx(key);

                CacheObject v = null;

                // If near cache does not have value, then we peek DHT cache.
                if (dhtEntry != null) {
                    boolean isNew = dhtEntry.isNewLocked() || !dhtEntry.valid(topVer);

                    if (needVer) {
                        EntryGetResult res = dhtEntry.innerGetVersioned(
                            null,
                            null,
                            /*swap*/true,
                            /*unmarshal*/true,
                            /**update-metrics*/false,
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
                            /*swap*/true,
                            /*read-through*/false,
                            /*update-metrics*/false,
                            /*events*/!nearRead && !skipVals,
                            /*temporary*/false,
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
                    if (cctx.cache().configuration().isStatisticsEnabled() && !skipVals)
                        cache().metrics0().onRead(true);

                    addResult(key, v, ver);

                    return true;
                }
                else {
                    boolean topStable = cctx.isReplicated() || topVer.equals(cctx.topology().topologyVersion());

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
                    dht.context().evicts().touch(dhtEntry, topVer);
            }
        }
    }

    /**
     * @param key Key.
     * @param v Value.
     * @param ver Version.
     */
    @SuppressWarnings("unchecked")
    private void addResult(KeyCacheObject key, CacheObject v, GridCacheVersion ver) {
        if (keepCacheObjects) {
            K key0 = (K)key;
            V val0 = needVer ?
                (V)new T2<>(skipVals ? true : v, ver) :
                (V)(skipVals ? true : v);

            add(new GridFinishedFuture<>(Collections.singletonMap(key0, val0)));
        }
        else {
            K key0 = (K)cctx.unwrapBinaryIfNeeded(key, !deserializeBinary, false);
            V val0 = needVer ?
                (V)new T2<>(!skipVals ?
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
                        needVer ? info.version() : null);
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
    private void releaseEvictions(Collection<KeyCacheObject> keys,
        Map<KeyCacheObject, GridNearCacheEntry> saved,
        AffinityTopologyVersion topVer) {
        for (KeyCacheObject key : keys) {
            GridNearCacheEntry entry = saved.get(key);

            if (entry != null) {
                entry.releaseEviction();

                if (tx == null)
                    cctx.evicts().touch(entry, topVer);
            }
        }
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

        return S.toString(GridNearGetFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Map<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node ID. */
        private ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private LinkedHashMap<KeyCacheObject, Boolean> keys;

        /** Saved entry versions. */
        private Map<KeyCacheObject, GridNearCacheEntry> savedEntries;

        /** Topology version on which this future was mapped. */
        private AffinityTopologyVersion topVer;

        /** {@code True} if remapped after node left. */
        private boolean remapped;

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
            this.node = node;
            this.keys = keys;
            this.savedEntries = savedEntries;
            this.topVer = topVer;
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

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Map<K, V> res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                releaseEvictions(keys.keySet(), savedEntries, topVer);

                return true;
            }
            else
                return false;
        }

        /**
         */
        synchronized void onNodeLeft() {
            if (remapped)
                return;

            remapped = true;

            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            // Try getting value from alive nodes.
            if (!canRemap) {
                // Remap
                map(keys.keySet(), F.t(node, keys), topVer);

                onDone(Collections.<K, V>emptyMap());
            }
            else {
                final AffinityTopologyVersion updTopVer =
                    new AffinityTopologyVersion(Math.max(topVer.topologyVersion() + 1, cctx.discovery().topologyVersion()));

                cctx.affinity().affinityReadyFuture(updTopVer).listen(
                    new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                            try {
                                fut.get();

                                // Remap.
                                map(keys.keySet(), F.t(node, keys), updTopVer);

                                onDone(Collections.<K, V>emptyMap());
                            }
                            catch (IgniteCheckedException e) {
                                GridNearGetFuture.this.onDone(e);
                            }
                        }
                    }
                );
            }
        }

        /**
         * @param res Result callback.
         */
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

                assert rmtTopVer.topologyVersion() != 0;

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

                    // It is critical to call onDone after adding futures to compound list.
                    onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedEntries, topVer));

                    return;
                }

                // Need to wait for next topology version to remap.
                IgniteInternalFuture<AffinityTopologyVersion> topFut = cctx.affinity().affinityReadyFuture(rmtTopVer);

                topFut.listen(new CIX1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void applyx(IgniteInternalFuture<AffinityTopologyVersion> fut) throws IgniteCheckedException {
                        AffinityTopologyVersion readyTopVer = fut.get();

                        // This will append new futures to compound list.
                        map(F.view(keys.keySet(), new P1<KeyCacheObject>() {
                            @Override public boolean apply(KeyCacheObject key) {
                                return invalidParts.contains(cctx.affinity().partition(key));
                            }
                        }), F.t(node, keys), readyTopVer);

                        // It is critical to call onDone after adding futures to compound list.
                        onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedEntries, topVer));
                    }
                });
            }
            else
                onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedEntries, topVer));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}

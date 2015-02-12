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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 *
 */
public final class GridNearGetFuture<K, V> extends GridCompoundIdentityFuture<Map<K, V>>
    implements GridCacheFuture<Map<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Maximum number of attempts to remap key to the same primary node. */
    private static final int MAX_REMAP_CNT = getInteger(IGNITE_NEAR_GET_MAX_REMAPS, DFLT_MAX_REMAP_CNT);

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Keys. */
    private Collection<? extends K> keys;

    /** Reload flag. */
    private boolean reload;

    /** Read through flag. */
    private boolean readThrough;

    /** Force primary flag. */
    private boolean forcePrimary;

    /** Future ID. */
    private IgniteUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Transaction. */
    private IgniteTxLocalEx<K, V> tx;

    /** Filters. */
    private IgnitePredicate<Cache.Entry<K, V>>[] filters;

    /** Logger. */
    private IgniteLogger log;

    /** Trackable flag. */
    private boolean trackable;

    /** Remap count. */
    private AtomicInteger remapCnt = new AtomicInteger();

    /** Subject ID. */
    private UUID subjId;

    /** Task name. */
    private String taskName;

    /** Whether to deserialize portable objects. */
    private boolean deserializePortable;

    /** Expiry policy. */
    private IgniteCacheExpiryPolicy expiryPlc;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearGetFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param forcePrimary If {@code true} get will be performed on primary node even if
     *      called on backup node.
     * @param tx Transaction.
     * @param filters Filters.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     */
    public GridNearGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        boolean readThrough,
        boolean reload,
        boolean forcePrimary,
        @Nullable IgniteTxLocalEx<K, V> tx,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filters,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc
    ) {
        super(cctx.kernalContext(), CU.<K, V>mapsReducer(keys.size()));

        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.readThrough = readThrough;
        this.reload = reload;
        this.forcePrimary = forcePrimary;
        this.filters = filters;
        this.tx = tx;
        this.subjId = subjId;
        this.taskName = taskName;
        this.deserializePortable = deserializePortable;
        this.expiryPlc = expiryPlc;

        futId = IgniteUuid.randomUuid();

        ver = tx == null ? cctx.versions().next() : tx.xidVersion();

        log = U.logger(ctx, logRef, GridNearGetFuture.class);
    }

    /**
     * Initializes future.
     */
    public void init() {
        long topVer = tx == null ? cctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

        map(keys, Collections.<ClusterNode, LinkedHashMap<K, Boolean>>emptyMap(), topVer);

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

    /**
     * @return Keys.
     */
    Collection<? extends K> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends ClusterNode> nodes() {
        return
            F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<Map<K, V>>, ClusterNode>() {
                @Nullable @Override public ClusterNode apply(IgniteInternalFuture<Map<K, V>> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridNearGetResponse<K, V> res) {
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
                cctx.mvcc().removeFuture(this);

            cache().dht().sendTtlUpdateRequest(expiryPlc);

            return true;
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<Map<K, V>> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * @param keys Keys.
     * @param mapped Mappings to check for duplicates.
     * @param topVer Topology version to map on.
     */
    private void map(Collection<? extends K> keys,
        Map<ClusterNode, LinkedHashMap<K, Boolean>> mapped,
        final long topVer) {
        Collection<ClusterNode> affNodes = CU.affinityNodes(cctx, topVer);

        if (affNodes.isEmpty()) {
            assert !isAffinityNode(cctx.config());

            onDone(new ClusterTopologyCheckedException("Failed to map keys for near-only cache (all partition " +
                "nodes left the grid)."));

            return;
        }

        Map<ClusterNode, LinkedHashMap<K, Boolean>> mappings = U.newHashMap(affNodes.size());

        Map<K, GridCacheVersion> savedVers = null;

        // Assign keys to primary nodes.
        for (K key : keys) {
            if (key == null) {
                NullPointerException err = new NullPointerException("Null key.");

                onDone(err);

                throw err;
            }

            savedVers = map(key, mappings, topVer, mapped, savedVers);
        }

        if (isDone())
            return;

        final Map<K, GridCacheVersion> saved = savedVers;

        final int keysSize = keys.size();

        // Create mini futures.
        for (Map.Entry<ClusterNode, LinkedHashMap<K, Boolean>> entry : mappings.entrySet()) {
            final ClusterNode n = entry.getKey();

            final LinkedHashMap<K, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
                    dht().getDhtAsync(n.id(),
                        -1,
                        mappedKeys,
                        readThrough,
                        reload,
                        topVer,
                        subjId,
                        taskName == null ? 0 : taskName.hashCode(),
                        deserializePortable,
                        filters,
                        expiryPlc);

                final Collection<Integer> invalidParts = fut.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<K> remapKeys = new ArrayList<>(keysSize);

                    for (K key : keys) {
                        if (key != null && invalidParts.contains(cctx.affinity().partition(key)))
                            remapKeys.add(key);
                    }

                    long updTopVer = ctx.discovery().topologyVersion();

                    assert updTopVer > topVer : "Got invalid partitions for local node but topology version did " +
                        "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                        ", invalidParts=" + invalidParts + ']';

                    // Remap recursively.
                    map(remapKeys, mappings, updTopVer);
                }

                // Add new future.
                add(fut.chain(new C1<IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>>, Map<K, V>>() {
                    @Override public Map<K, V> apply(IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>> fut) {
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

                    cctx.mvcc().addFuture(this);
                }

                MiniFuture fut = new MiniFuture(n, mappedKeys, saved, topVer);

                GridCacheMessage<K, V> req = new GridNearGetRequest<>(
                    cctx.cacheId(),
                    futId,
                    fut.futureId(),
                    ver,
                    mappedKeys,
                    readThrough,
                    reload,
                    topVer,
                    filters,
                    subjId,
                    taskName == null ? 0 : taskName.hashCode(),
                    expiryPlc != null ? expiryPlc.forAccess() : -1L);

                add(fut); // Append new future.

                try {
                    cctx.io().send(n, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onResult((ClusterTopologyCheckedException)e);
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
     * @param savedVers Saved versions.
     * @return Map.
     */
    private Map<K, GridCacheVersion> map(K key, Map<ClusterNode, LinkedHashMap<K, Boolean>> mappings,
        long topVer, Map<ClusterNode, LinkedHashMap<K, Boolean>> mapped, Map<K, GridCacheVersion> savedVers) {
        final GridNearCacheAdapter<K, V> near = cache();

        // Allow to get cached value from the local node.
        boolean allowLocRead = !forcePrimary || cctx.affinity().primary(cctx.localNode(), key, topVer);

        GridCacheEntryEx<K, V> entry = allowLocRead ? near.peekEx(key) : null;

        while (true) {
            try {
                V v = null;

                boolean isNear = entry != null;

                // First we peek into near cache.
                if (isNear)
                    v = entry.innerGet(tx,
                        /*swap*/false,
                        /*read-through*/false,
                        /*fail-fast*/true,
                        /*unmarshal*/true,
                        /*metrics*/true,
                        /*events*/true,
                        /*temporary*/false,
                        subjId,
                        null,
                        taskName,
                        filters,
                        expiryPlc);

                ClusterNode primary = null;

                if (v == null && allowLocRead) {
                    GridDhtCacheAdapter<K, V> dht = cache().dht();

                    try {
                        entry = dht.context().isSwapOrOffheapEnabled() ? dht.entryEx(key) : dht.peekEx(key);

                        // If near cache does not have value, then we peek DHT cache.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked() || !entry.valid(topVer);

                            v = entry.innerGet(tx,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /*update-metrics*/false,
                                /*events*/!isNear,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                filters,
                                expiryPlc);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null && isNew && entry.markObsoleteIfEmpty(ver))
                                dht.removeIfObsolete(key);
                        }

                        if (v != null) {
                            if (cctx.cache().configuration().isStatisticsEnabled())
                                near.metrics0().onRead(true);
                        }
                        else {
                            primary = cctx.affinity().primary(key, topVer);

                            if (!primary.isLocal() && cctx.cache().configuration().isStatisticsEnabled())
                                near.metrics0().onRead(false);
                        }
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        // No-op.
                    }
                    finally {
                        if (entry != null && (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED))) {
                            dht.context().evicts().touch(entry, topVer);

                            entry = null;
                        }
                    }
                }

                if (v != null && !reload) {
                    if (cctx.portableEnabled())
                        v = (V)cctx.unwrapPortableIfNeeded(v, !deserializePortable);

                    add(new GridFinishedFuture<>(cctx.kernalContext(), Collections.singletonMap(key, v)));
                }
                else {
                    if (primary == null)
                        primary = cctx.affinity().primary(key, topVer);

                    GridNearCacheEntry<K, V> nearEntry = allowLocRead ? near.peekExx(key) : null;

                    entry = nearEntry;

                    if (savedVers == null)
                        savedVers = U.newHashMap(3);

                    savedVers.put(key, nearEntry == null ? null : nearEntry.dhtVersion());

                    LinkedHashMap<K, Boolean> keys = mapped.get(primary);

                    if (keys != null && keys.containsKey(key)) {
                        if (remapCnt.incrementAndGet() > MAX_REMAP_CNT) {
                            onDone(new ClusterTopologyCheckedException("Failed to remap key to a new node after " + MAX_REMAP_CNT
                                + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                                U.toShortString(primary) + ", mappings=" + mapped + ']'));

                            return savedVers;
                        }
                    }

                    // Don't add reader if transaction acquires lock anyway to avoid deadlock.
                    boolean addRdr = tx == null || tx.optimistic();

                    if (!addRdr && tx.readCommitted() && !tx.writeSet().contains(key))
                        addRdr = true;

                    LinkedHashMap<K, Boolean> old = mappings.get(primary);

                    if (old == null)
                        mappings.put(primary, old = new LinkedHashMap<>(3, 1f));

                    old.put(key, addRdr);
                }

                break;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                entry = allowLocRead ? near.peekEx(key) : null;
            }
            catch (GridCacheFilterFailedException e) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for entry: " + e);

                break;
            }
            finally {
                if (entry != null && !reload && tx == null)
                    cctx.evicts().touch(entry, topVer);
            }
        }

        return savedVers;
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
     * @param savedVers Saved versions.
     * @param topVer Topology version
     * @return Result map.
     */
    private Map<K, V> loadEntries(UUID nodeId,
        Collection<K> keys,
        Collection<GridCacheEntryInfo<K, V>> infos,
        Map<K, GridCacheVersion> savedVers,
        long topVer) {
        boolean empty = F.isEmpty(keys);

        Map<K, V> map = empty ? Collections.<K, V>emptyMap() : new GridLeanMap<K, V>(keys.size());

        if (!empty) {
            boolean atomic = cctx.atomic();

            GridCacheVersion ver = atomic ? null : F.isEmpty(infos) ? null : cctx.versions().next();

            for (GridCacheEntryInfo<K, V> info : infos) {
                try {
                    info.unmarshalValue(cctx, cctx.deploy().globalLoader());

                    // Entries available locally in DHT should not be loaded into near cache for reading.
                    if (!cctx.cache().affinity().isPrimaryOrBackup(cctx.localNode(), info.key())) {
                        GridNearCacheEntry<K, V> entry = cache().entryExx(info.key(), topVer);

                        GridCacheVersion saved = savedVers.get(info.key());

                        // Load entry into cache.
                        entry.loadedValue(tx,
                            nodeId,
                            info.value(),
                            info.valueBytes(),
                            atomic ? info.version() : ver,
                            info.version(),
                            saved,
                            info.ttl(),
                            info.expireTime(),
                            true,
                            topVer,
                            subjId);

                        cctx.evicts().touch(entry, topVer);
                    }

                    V val = info.value();

                    if (cctx.portableEnabled())
                        val = (V)cctx.unwrapPortableIfNeeded(val, !deserializePortable);

                    map.put(info.key(), val);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while processing get response (will not retry).");
                }
                catch (IgniteCheckedException e) {
                    // Fail.
                    onDone(e);

                    return Collections.emptyMap();
                }
            }
        }

        return map;
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
        private LinkedHashMap<K, Boolean> keys;

        /** Saved entry versions. */
        private Map<K, GridCacheVersion> savedVers;

        /** Topology version on which this future was mapped. */
        private long topVer;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param node Node.
         * @param keys Keys.
         * @param savedVers Saved entry versions.
         * @param topVer Topology version.
         */
        MiniFuture(ClusterNode node, LinkedHashMap<K, Boolean> keys, Map<K, GridCacheVersion> savedVers, long topVer) {
            super(cctx.kernalContext());

            this.node = node;
            this.keys = keys;
            this.savedVers = savedVers;
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
        public Collection<K> keys() {
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
         * @param e Topology exception.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            long updTopVer = ctx.discovery().topologyVersion();

            if (updTopVer > topVer) {
                // Remap.
                map(keys.keySet(), F.t(node, keys), updTopVer);

                onDone(Collections.<K, V>emptyMap());
            }
            else {
                final RemapTimeoutObject timeout = new RemapTimeoutObject(ctx.config().getNetworkTimeout(), topVer, e);

                ctx.discovery().topologyFuture(topVer + 1).listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                    @Override public void apply(IgniteInternalFuture<Long> longIgniteFuture) {
                        if (timeout.finish()) {
                            ctx.timeout().removeTimeoutObject(timeout);

                            // Remap.
                            map(keys.keySet(), F.t(node, keys), cctx.affinity().affinityTopologyVersion());

                            onDone(Collections.<K, V>emptyMap());
                        }
                    }
                });

                ctx.timeout().addTimeoutObject(timeout);
            }
        }

        /**
         * @param res Result callback.
         */
        void onResult(final GridNearGetResponse<K, V> res) {
            final Collection<Integer> invalidParts = res.invalidPartitions();

            // If error happened on remote node, fail the whole future.
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            // Remap invalid partitions.
            if (!F.isEmpty(invalidParts)) {
                long rmtTopVer = res.topologyVersion();

                assert rmtTopVer != 0;

                if (rmtTopVer <= topVer) {
                    // Fail the whole get future.
                    onDone(new IgniteCheckedException("Failed to process invalid partitions response (remote node reported " +
                        "invalid partitions but remote topology version does not differ from local) " +
                        "[topVer=" + topVer + ", rmtTopVer=" + rmtTopVer + ", invalidParts=" + invalidParts +
                        ", nodeId=" + node.id() + ']'));

                    return;
                }

                if (log.isDebugEnabled())
                    log.debug("Remapping mini get future [invalidParts=" + invalidParts + ", fut=" + this + ']');

                // Need to wait for next topology version to remap.
                IgniteInternalFuture<Long> topFut = ctx.discovery().topologyFuture(rmtTopVer);

                topFut.listenAsync(new CIX1<IgniteInternalFuture<Long>>() {
                    @Override public void applyx(IgniteInternalFuture<Long> fut) throws IgniteCheckedException {
                        long readyTopVer = fut.get();

                        // This will append new futures to compound list.
                        map(F.view(keys.keySet(), new P1<K>() {
                            @Override public boolean apply(K key) {
                                return invalidParts.contains(cctx.affinity().partition(key));
                            }
                        }), F.t(node, keys), readyTopVer);

                        // It is critical to call onDone after adding futures to compound list.
                        onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedVers, topVer));
                    }
                });
            }
            else
                onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedVers, topVer));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }

        /**
         * Remap timeout object.
         */
        private class RemapTimeoutObject extends GridTimeoutObjectAdapter {
            /** Finished flag. */
            private AtomicBoolean finished = new AtomicBoolean();

            /** Topology version to wait. */
            private long topVer;

            /** Exception cause. */
            private IgniteCheckedException e;

            /**
             * @param timeout Timeout.
             * @param topVer Topology version timeout was created on.
             */
            private RemapTimeoutObject(long timeout, long topVer, IgniteCheckedException e) {
                super(timeout);

                this.topVer = topVer;
                this.e = e;
            }

            /** {@inheritDoc} */
            @Override public void onTimeout() {
                if (finish())
                    // Fail the whole get future.
                    onDone(new IgniteCheckedException("Failed to wait for topology version to change: " + (topVer + 1), e));
                // else remap happened concurrently.
            }

            /**
             * @return Guard against concurrent completion.
             */
            public boolean finish() {
                return finished.compareAndSet(false, true);
            }
        }
    }
}

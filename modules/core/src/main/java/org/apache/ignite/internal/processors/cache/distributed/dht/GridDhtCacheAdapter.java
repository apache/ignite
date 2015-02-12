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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

/**
 * DHT cache adapter.
 */
public abstract class GridDhtCacheAdapter<K, V> extends GridDistributedCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology. */
    private GridDhtPartitionTopology<K, V> top;

    /** Preloader. */
    protected GridCachePreloader<K, V> preldr;

    /** Multi tx future holder. */
    private ThreadLocal<IgniteBiTuple<IgniteUuid, GridDhtTopologyFuture>> multiTxHolder = new ThreadLocal<>();

    /** Multi tx futures. */
    private ConcurrentMap<IgniteUuid, MultiUpdateFuture> multiTxFuts = new ConcurrentHashMap8<>();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridDhtCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx) {
        super(ctx, ctx.config().getStartSize());

        top = new GridDhtPartitionTopologyImpl<>(ctx);
    }

    /**
     * Constructor used for near-only cache.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);

        top = new GridDhtPartitionTopologyImpl<>(ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridDhtCacheEntry<>(ctx, topVer, key, hash, val, next, ttl, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.io().addHandler(ctx.cacheId(), GridCacheTtlUpdateRequest.class, new CI2<UUID, GridCacheTtlUpdateRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheTtlUpdateRequest<K, V> req) {
                processTtlUpdateRequest(req);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        super.stop();

        if (preldr != null)
            preldr.stop();

        // Clean up to help GC.
        preldr = null;
        top = null;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        preldr.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        super.onKernalStop();

        if (preldr != null)
            preldr.onKernalStop();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        super.printMemoryStats();

        top.printMemoryStats(1024);
    }

    /**
     * @return Near cache.
     */
    public abstract GridNearCacheAdapter<K, V> near();

    /**
     * @return Partition topology.
     */
    public GridDhtPartitionTopology<K, V> topology() {
        return top;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader<K, V> preloader() {
        return preldr;
    }

    /**
     * @return DHT preloader.
     */
    public GridDhtPreloader<K, V> dhtPreloader() {
        assert preldr instanceof GridDhtPreloader;

        return (GridDhtPreloader<K, V>)preldr;
    }

    /**
     * @return Topology version future registered for multi-update.
     */
    @Nullable public GridDhtTopologyFuture multiUpdateTopologyFuture() {
        IgniteBiTuple<IgniteUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        return tup == null ? null : tup.get2();
    }

    /**
     * Starts multi-update lock. Will wait for topology future is ready.
     *
     * @return Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public long beginMultiUpdate() throws IgniteCheckedException {
        IgniteBiTuple<IgniteUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        if (tup != null)
            throw new IgniteCheckedException("Nested multi-update locks are not supported");

        top.readLock();

        GridDhtTopologyFuture topFut;

        long topVer;

        try {
            // While we are holding read lock, register lock future for partition release future.
            IgniteUuid lockId = IgniteUuid.fromUuid(ctx.localNodeId());

            topVer = top.topologyVersion();

            MultiUpdateFuture fut = new MultiUpdateFuture(ctx.kernalContext(), topVer);

            MultiUpdateFuture old = multiTxFuts.putIfAbsent(lockId, fut);

            assert old == null;

            topFut = top.topologyVersionFuture();

            multiTxHolder.set(F.t(lockId, topFut));
        }
        finally {
            top.readUnlock();
        }

        topFut.get();

        return topVer;
    }

    /**
     * Ends multi-update lock.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void endMultiUpdate() throws IgniteCheckedException {
        IgniteBiTuple<IgniteUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        if (tup == null)
            throw new IgniteCheckedException("Multi-update was not started or released twice.");

        top.readLock();

        try {
            IgniteUuid lockId = tup.get1();

            MultiUpdateFuture multiFut = multiTxFuts.remove(lockId);

            multiTxHolder.set(null);

            // Finish future.
            multiFut.onDone(lockId);
        }
        finally {
            top.readUnlock();
        }
    }

    /**
     * Creates multi update finish future. Will return {@code null} if no multi-update locks are found.
     *
     * @param topVer Topology version.
     * @return Finish future.
     */
    @Nullable public IgniteInternalFuture<?> multiUpdateFinishFuture(long topVer) {
        GridCompoundFuture<IgniteUuid, Object> fut = null;

        for (MultiUpdateFuture multiFut : multiTxFuts.values()) {
            if (multiFut.topologyVersion() <= topVer) {
                if (fut == null)
                    fut = new GridCompoundFuture<>(ctx.kernalContext());

                fut.add(multiFut);
            }
        }

        if (fut != null)
            fut.markInitialized();

        return fut;
    }

    /**
     * @param key Key.
     * @return DHT entry.
     */
    @Nullable public GridDhtCacheEntry<K, V> peekExx(K key) {
        return (GridDhtCacheEntry<K, V>)peekEx(key);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public Cache.Entry<K, V> entry(K key) throws GridDhtInvalidPartitionException {
        return super.entry(key);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntryEx<K, V> entryEx(K key, boolean touch) throws GridDhtInvalidPartitionException {
        return super.entryEx(key, touch);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntryEx<K, V> entryEx(K key, long topVer) throws GridDhtInvalidPartitionException {
        return super.entryEx(key, topVer);
    }

    /**
     * @param key Key.
     * @return DHT entry.
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    public GridDhtCacheEntry<K, V> entryExx(K key) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry<K, V>)entryEx(key);
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return DHT entry.
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    public GridDhtCacheEntry<K, V> entryExx(K key, long topVer) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry<K, V>)entryEx(key, topVer);
    }

    /**
     * Gets or creates entry for given key. If key belongs to local node, dht entry will be returned, otherwise
     * if {@code allowDetached} is {@code true}, detached entry will be returned, otherwise exception will be
     * thrown.
     *
     * @param key Key for which entry should be returned.
     * @param allowDetached Whether to allow detached entries.
     * @param touch {@code True} if entry should be passed to eviction policy.
     * @return Cache entry.
     * @throws GridDhtInvalidPartitionException if entry does not belong to this node and
     *      {@code allowDetached} is {@code false}.
     */
    public GridCacheEntryEx<K, V> entryExx(K key, long topVer, boolean allowDetached, boolean touch) {
        try {
            return allowDetached && !ctx.affinity().localNode(key, topVer) ?
                new GridDhtDetachedCacheEntry<>(ctx, key, key.hashCode(), null, null, 0, 0) :
                entryEx(key, touch);
        }
        catch (GridDhtInvalidPartitionException e) {
            if (!allowDetached)
                throw e;

            return new GridDhtDetachedCacheEntry<>(ctx, key, key.hashCode(), null, null, 0, 0);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoad(Collection<? extends K> keys, final ExpiryPolicy plc)
        throws IgniteCheckedException {
        if (ctx.store().isLocalStore()) {
            super.localLoad(keys, plc);

            return;
        }

        // Version for all loaded entries.
        final GridCacheVersion ver0 = ctx.shared().versions().nextForLoad(topology().topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

        final long topVer = ctx.affinity().affinityTopologyVersion();

        final ExpiryPolicy plc0 = plc != null ? plc : ctx.expiry();

        ctx.store().loadAllFromStore(null, keys, new CI2<K, V>() {
            @Override public void apply(K key, V val) {
                loadEntry(key, val, ver0, null, topVer, replicate, plc0);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiPredicate<K, V> p, final long ttl, Object[] args) throws IgniteCheckedException {
        if (ctx.store().isLocalStore()) {
            super.loadCache(p, ttl, args);

            return;
        }

        // Version for all loaded entries.
        final GridCacheVersion ver0 = ctx.shared().versions().nextForLoad(topology().topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

        final long topVer = ctx.affinity().affinityTopologyVersion();

        final ExpiryPolicy plc = ctx.expiry();

        ctx.store().loadCache(new CI3<K, V, GridCacheVersion>() {
            @Override public void apply(K key, V val, @Nullable GridCacheVersion ver) {
                assert ver == null;

                loadEntry(key, val, ver0, p, topVer, replicate, plc);
            }
        }, args);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Cache version.
     * @param p Optional predicate.
     * @param topVer Topology version.
     * @param replicate Replication flag.
     * @param plc Expiry policy.
     */
    private void loadEntry(K key,
        V val,
        GridCacheVersion ver,
        @Nullable IgniteBiPredicate<K, V> p,
        long topVer,
        boolean replicate,
        @Nullable ExpiryPolicy plc) {
        if (p != null && !p.apply(key, val))
            return;

        try {
            GridDhtLocalPartition<K, V> part = top.localPartition(ctx.affinity().partition(key), -1, true);

            // Reserve to make sure that partition does not get unloaded.
            if (part.reserve()) {
                GridCacheEntryEx<K, V> entry = null;

                try {
                    long ttl = 0;

                    if (plc != null) {
                        ttl = CU.toTtl(plc.getExpiryForCreation());

                        if (ttl == CU.TTL_ZERO)
                            return;
                        else if (ttl == CU.TTL_NOT_CHANGED)
                            ttl = 0;
                    }

                    if (ctx.portableEnabled()) {
                        key = (K)ctx.marshalToPortable(key);
                        val = (V)ctx.marshalToPortable(val);
                    }

                    entry = entryEx(key, false);

                    entry.initialValue(val, null, ver, ttl, -1, false, topVer, replicate ? DR_LOAD : DR_NONE);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to put cache value: " + entry, e);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry during loadCache (will ignore): " + entry);
                }
                finally {
                    if (entry != null)
                        entry.context().evicts().touch(entry, topVer);

                    part.release();
                }
            }
            else if (log.isDebugEnabled())
                log.debug("Will node load entry into cache (partition is invalid): " + part);
        }
        catch (GridDhtInvalidPartitionException e) {
            if (log.isDebugEnabled())
                log.debug("Ignoring entry for partition that does not belong [key=" + key + ", val=" + val +
                    ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        int sum = 0;

        long topVer = ctx.affinity().affinityTopologyVersion();

        for (GridDhtLocalPartition<K, V> p : topology().currentLocalPartitions()) {
            if (p.primary(topVer))
                sum += p.publicSize();
        }

        return sum;
    }

    /**
     * This method is used internally. Use
     * {@link #getDhtAsync(UUID, long, LinkedHashMap, boolean, boolean, long, UUID, int, boolean, IgnitePredicate[], IgniteCacheExpiryPolicy)}
     * method instead to retrieve DHT value.
     *
     * @param keys {@inheritDoc}
     * @param forcePrimary {@inheritDoc}
     * @param skipTx {@inheritDoc}
     * @param filter {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        return getAllAsync(keys,
            true,
            null,
            /*don't check local tx. */false,
            subjId,
            taskName,
            deserializePortable,
            forcePrimary,
            null,
            filter);
    }

    /** {@inheritDoc} */
    @Override public V reload(K key, @Nullable IgnitePredicate<Cache.Entry<K, V>>... filter)
        throws IgniteCheckedException {
        try {
            return super.reload(key, filter);
        }
        catch (GridDhtInvalidPartitionException ignored) {
            return null;
        }
    }

    /**
     * @param keys Keys to get
     * @param readThrough Read through flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param filter Optional filter.
     * @param expiry Expiry policy.
     * @return Get future.
     */
    IgniteInternalFuture<Map<K, V>> getDhtAllAsync(@Nullable Collection<? extends K> keys,
        boolean readThrough,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filter,
        @Nullable IgniteCacheExpiryPolicy expiry
        ) {
        return getAllAsync(keys,
            readThrough,
            null,
            /*don't check local tx. */false,
            subjId,
            taskName,
            deserializePortable,
            false,
            expiry,
            filter);
    }

    /**
     * @param reader Reader node ID.
     * @param msgId Message ID.
     * @param keys Keys to get.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param deserializePortable Deserialize portable flag.
     * @param filter Optional filter.
     * @param expiry Expiry policy.
     * @return DHT future.
     */
    public GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> getDhtAsync(UUID reader,
        long msgId,
        LinkedHashMap<? extends K, Boolean> keys,
        boolean readThrough,
        boolean reload,
        long topVer,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean deserializePortable,
        IgnitePredicate<Cache.Entry<K, V>>[] filter,
        @Nullable IgniteCacheExpiryPolicy expiry) {
        GridDhtGetFuture<K, V> fut = new GridDhtGetFuture<>(ctx,
            msgId,
            reader,
            keys,
            readThrough,
            reload,
            /*tx*/null,
            topVer,
            filter,
            subjId,
            taskNameHash,
            deserializePortable,
            expiry);

        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param req Get request.
     */
    protected void processNearGetRequest(final UUID nodeId, final GridNearGetRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);

        long ttl = req.accessTtl();

        final GetExpiryPolicy expiryPlc = GetExpiryPolicy.forTtl(ttl);

        IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
            getDhtAsync(nodeId,
                req.messageId(),
                req.keys(),
                req.readThrough(),
                req.reload(),
                req.topologyVersion(),
                req.subjectId(),
                req.taskNameHash(),
                false,
                req.filter(),
                expiryPlc);

        fut.listenAsync(new CI1<IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>>>() {
            @Override public void apply(IgniteInternalFuture<Collection<GridCacheEntryInfo<K, V>>> f) {
                GridNearGetResponse<K, V> res = new GridNearGetResponse<>(ctx.cacheId(),
                    req.futureId(),
                    req.miniId(),
                    req.version());

                GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
                    (GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>>)f;

                try {
                    Collection<GridCacheEntryInfo<K, V>> entries = fut.get();

                    res.entries(entries);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed processing get request: " + req, e);

                    res.error(e);
                }

                res.invalidPartitions(fut.invalidPartitions(), ctx.discovery().topologyVersion());

                try {
                    ctx.io().send(nodeId, res, ctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send get response to node (is node still alive?) [nodeId=" + nodeId +
                        ",req=" + req + ", res=" + res + ']', e);
                }

                sendTtlUpdateRequest(expiryPlc);
            }
        });
    }

    /**
     * @param expiryPlc Expiry policy.
     */
    public void sendTtlUpdateRequest(@Nullable final IgniteCacheExpiryPolicy expiryPlc) {
        if (expiryPlc != null && expiryPlc.entries() != null) {
            ctx.closures().runLocalSafe(new Runnable() {
                @SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
                @Override public void run() {
                    Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries = expiryPlc.entries();

                    assert entries != null && !entries.isEmpty();

                    Map<ClusterNode, GridCacheTtlUpdateRequest<K, V>> reqMap = new HashMap<>();

                    long topVer = ctx.discovery().topologyVersion();

                    for (Map.Entry<Object, IgniteBiTuple<byte[], GridCacheVersion>> e : entries.entrySet()) {
                        List<ClusterNode> nodes = ctx.affinity().nodes((K)e.getKey(), topVer);

                        for (int i = 0; i < nodes.size(); i++) {
                            ClusterNode node = nodes.get(i);

                            if (!node.isLocal()) {
                                GridCacheTtlUpdateRequest<K, V> req = reqMap.get(node);

                                if (req == null) {
                                    reqMap.put(node,
                                        req = new GridCacheTtlUpdateRequest<>(topVer, expiryPlc.forAccess()));

                                    req.cacheId(ctx.cacheId());
                                }

                                req.addEntry(e.getValue().get1(), e.getValue().get2());
                            }
                        }
                    }

                    Map<UUID, Collection<IgniteBiTuple<byte[], GridCacheVersion>>> rdrs = expiryPlc.readers();

                    if (rdrs != null) {
                        assert !rdrs.isEmpty();

                        for (Map.Entry<UUID, Collection<IgniteBiTuple<byte[], GridCacheVersion>>> e : rdrs.entrySet()) {
                            ClusterNode node = ctx.node(e.getKey());

                            if (node != null) {
                                GridCacheTtlUpdateRequest<K, V> req = reqMap.get(node);

                                if (req == null) {
                                    reqMap.put(node, req = new GridCacheTtlUpdateRequest<>(topVer,
                                        expiryPlc.forAccess()));

                                    req.cacheId(ctx.cacheId());
                                }

                                for (IgniteBiTuple<byte[], GridCacheVersion> t : e.getValue())
                                    req.addNearEntry(t.get1(), t.get2());
                            }
                        }
                    }

                    for (Map.Entry<ClusterNode, GridCacheTtlUpdateRequest<K, V>> req : reqMap.entrySet()) {
                        try {
                            ctx.io().send(req.getKey(), req.getValue(), ctx.ioPolicy());
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to send TTL update request.", e);
                        }
                    }
                }
            });
        }
    }

    /**
     * @param req Request.
     */
    private void processTtlUpdateRequest(GridCacheTtlUpdateRequest<K, V> req) {
        if (req.keys() != null)
            updateTtl(this, req.keys(), req.versions(), req.ttl());

        if (req.nearKeys() != null) {
            GridNearCacheAdapter<K, V> near = near();

            assert near != null;

            updateTtl(near, req.nearKeys(), req.nearVersions(), req.ttl());
        }
    }

    /**
     * @param cache Cache.
     * @param keys Entries keys.
     * @param vers Entries versions.
     * @param ttl TTL.
     */
    private void updateTtl(GridCacheAdapter<K, V> cache,
        List<K> keys,
        List<GridCacheVersion> vers,
        long ttl) {
        assert !F.isEmpty(keys);
        assert keys.size() == vers.size();

        int size = keys.size();

        boolean swap = cache.context().isSwapOrOffheapEnabled();

        for (int i = 0; i < size; i++) {
            try {
                GridCacheEntryEx<K, V> entry = null;

                try {
                    if (swap) {
                        entry = cache.entryEx(keys.get(i));

                        entry.unswap(true, false);
                    }
                    else
                        entry = cache.peekEx(keys.get(i));

                    if (entry != null)
                        entry.updateTtl(vers.get(i), ttl);
                }
                finally {
                    if (entry != null)
                        cache.context().evicts().touch(entry, -1L);
                }
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to unswap entry.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(int part) {
        return new PartitionEntrySet(part);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtCacheAdapter.class, this, super.toString());
    }

    /**
     *
     */
    private class PartitionEntrySet extends AbstractSet<Cache.Entry<K, V>> {
        /** */
        private int partId;

        /**
         * @param partId Partition id.
         */
        private PartitionEntrySet(int partId) {
            this.partId = partId;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Cache.Entry<K, V>> iterator() {
            final GridDhtLocalPartition<K, V> part = ctx.topology().localPartition(partId,
                ctx.discovery().topologyVersion(), false);

            Iterator<GridDhtCacheEntry<K, V>> partIt = part == null ? null : part.entries().iterator();

            return new PartitionEntryIterator<>(partIt);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            if (!(o instanceof Cache.Entry))
                return false;

            Cache.Entry<K, V> entry = (Cache.Entry<K, V>)o;

            K key = entry.getKey();
            V val = entry.getValue();

            if (val == null)
                return false;

            try {
                // Cannot use remove(key, val) since we may be in DHT cache and should go through near.
                return GridDhtCacheAdapter.this.remove(key, val);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean removeAll(Collection<?> c) {
            boolean rmv = false;

            for (Object o : c)
                rmv |= remove(o);

            return rmv;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            if (!(o instanceof Cache.Entry))
                return false;

            Cache.Entry<K, V> entry = (Cache.Entry<K, V>)o;

            return partId == ctx.affinity().partition(entry.getKey()) &&
                F.eq(entry.getValue(), peek(entry.getKey()));
        }

        /** {@inheritDoc} */
        @Override public int size() {
            GridDhtLocalPartition<K, V> part = ctx.topology().localPartition(partId,
                ctx.discovery().topologyVersion(), false);

            return part != null ? part.publicSize() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PartitionEntrySet.class, this, "super", super.toString());
        }
    }

    /** {@inheritDoc} */
    @Override public List<GridCacheClearAllRunnable<K, V>> splitClearLocally() {
        CacheDistributionMode mode = configuration().getDistributionMode();

        return (mode == PARTITIONED_ONLY || mode == NEAR_PARTITIONED) ? super.splitClearLocally() :
            Collections.<GridCacheClearAllRunnable<K, V>>emptyList();
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert entry.isDht();

        GridDhtLocalPartition<K, V> part = topology().localPartition(entry.partition(), -1, false);

        // Do not remove entry on replica topology. Instead, add entry to removal queue.
        // It will be cleared eventually.
        if (part != null) {
            try {
                part.onDeferredDelete(entry.key(), ver);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to enqueue deleted entry [key=" + entry.key() + ", ver=" + ver + ']', e);
            }
        }
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @return Local entries iterator.
     */
    public Iterator<Cache.Entry<K, V>> localEntriesIterator(final boolean primary, final boolean backup) {
        assert primary || backup;

        if (primary && backup)
            return iterator(map.entries0().iterator(), !ctx.keepPortable());
        else {
            final long topVer = ctx.affinity().affinityTopologyVersion();

            final Iterator<GridDhtLocalPartition<K, V>> partIt = topology().currentLocalPartitions().iterator();

            Iterator<GridCacheEntryEx<K, V>> it = new Iterator<GridCacheEntryEx<K, V>>() {
                private GridCacheEntryEx<K, V> next;

                private Iterator<GridDhtCacheEntry<K, V>> curIt;

                {
                    advance();
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                @Override public GridCacheEntryEx<K, V> next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    GridCacheEntryEx<K, V> e = next;

                    advance();

                    return e;
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException();
                }

                private void advance() {
                    next = null;

                    do {
                        if (curIt == null) {
                            while (partIt.hasNext()) {
                                GridDhtLocalPartition<K, V> part = partIt.next();

                                if (primary == part.primary(topVer)) {
                                    curIt = part.entries().iterator();

                                    break;
                                }
                            }
                        }

                        if (curIt != null) {
                            if (curIt.hasNext()) {
                                next = curIt.next();

                                break;
                            }
                            else
                                curIt = null;
                        }
                    }
                    while (partIt.hasNext());
                }
            };

            return iterator(it, !ctx.keepPortable());
        }
    }

    /**
     * Complex partition iterator for both partition and swap iteration.
     */
    private class PartitionEntryIterator<K, V> extends GridIteratorAdapter<Cache.Entry<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Next entry. */
        private Cache.Entry<K, V> entry;

        /** Last seen entry to support remove. */
        private Cache.Entry<K, V> last;

        /** Partition iterator. */
        private final Iterator<GridDhtCacheEntry<K, V>> partIt;

        /**
         * @param partIt Partition iterator.
         */
        private PartitionEntryIterator(@Nullable Iterator<GridDhtCacheEntry<K, V>> partIt) {
            this.partIt = partIt;

            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() {
            return entry != null;
        }

        /** {@inheritDoc} */
        @Override public Cache.Entry<K, V> nextX() throws IgniteCheckedException {
            if (!hasNext())
                throw new NoSuchElementException();

            last = entry;

            advance();

            return last;
        }

        /** {@inheritDoc} */
        @Override public void removeX() throws IgniteCheckedException {
            if (last == null)
                throw new IllegalStateException();

            ctx.grid().jcache(ctx.name()).remove(last.getKey(), last.getValue());
        }

        /**
         *
         */
        private void advance() {
            if (partIt != null) {
                while (partIt.hasNext()) {
                    GridDhtCacheEntry<K, V> next = partIt.next();

                    if (next.isInternal() || !next.visitable(CU.<K, V>empty()))
                        continue;

                    entry = next.wrapLazyValue();

                    return;
                }
            }

            entry = null;
        }
    }

    /**
     * Multi update future.
     */
    private static class MultiUpdateFuture extends GridFutureAdapter<IgniteUuid> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Topology version. */
        private long topVer;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public MultiUpdateFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         * @param topVer Topology version.
         */
        private MultiUpdateFuture(GridKernalContext ctx, long topVer) {
            super(ctx);

            this.topVer = topVer;
        }

        /**
         * @return Topology version.
         */
        private long topologyVersion() {
            return topVer;
        }
    }
}

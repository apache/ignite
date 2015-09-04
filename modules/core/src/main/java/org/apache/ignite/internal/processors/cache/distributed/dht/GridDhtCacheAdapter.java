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

import java.io.Externalizable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.CachePeekModes;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllRunnable;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTtlUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.dr.GridDrType.DR_LOAD;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;

/**
 * DHT cache adapter.
 */
public abstract class GridDhtCacheAdapter<K, V> extends GridDistributedCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology. */
    private GridDhtPartitionTopologyImpl top;

    /** Preloader. */
    protected GridCachePreloader preldr;

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

        top = new GridDhtPartitionTopologyImpl(ctx);
    }

    /**
     * Constructor used for near-only cache.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);

        top = new GridDhtPartitionTopologyImpl(ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry create(GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key,
                int hash,
                CacheObject val,
                GridCacheMapEntry next,
                int hdrId)
            {
                if (ctx.useOffheapEntry())
                    return new GridDhtOffHeapCacheEntry(ctx, topVer, key, hash, val, next, hdrId);

                return new GridDhtCacheEntry(ctx, topVer, key, hash, val, next, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.io().addHandler(ctx.cacheId(), GridCacheTtlUpdateRequest.class, new CI2<UUID, GridCacheTtlUpdateRequest>() {
            @Override public void apply(UUID nodeId, GridCacheTtlUpdateRequest req) {
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
    @Override public void onReconnected() {
        super.onReconnected();

        ctx.affinity().onReconnected();

        top.onReconnected();

        if (preldr != null)
            preldr.onReconnected();
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
    public GridDhtPartitionTopology topology() {
        return top;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader preloader() {
        return preldr;
    }

    /**
     * @return DHT preloader.
     */
    public GridDhtPreloader dhtPreloader() {
        assert preldr instanceof GridDhtPreloader;

        return (GridDhtPreloader)preldr;
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
    public AffinityTopologyVersion beginMultiUpdate() throws IgniteCheckedException {
        IgniteBiTuple<IgniteUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        if (tup != null)
            throw new IgniteCheckedException("Nested multi-update locks are not supported");

        top.readLock();

        GridDhtTopologyFuture topFut;

        AffinityTopologyVersion topVer;

        try {
            // While we are holding read lock, register lock future for partition release future.
            IgniteUuid lockId = IgniteUuid.fromUuid(ctx.localNodeId());

            topVer = top.topologyVersion();

            MultiUpdateFuture fut = new MultiUpdateFuture(topVer);

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
    @Nullable public IgniteInternalFuture<?> multiUpdateFinishFuture(AffinityTopologyVersion topVer) {
        GridCompoundFuture<IgniteUuid, Object> fut = null;

        for (MultiUpdateFuture multiFut : multiTxFuts.values()) {
            if (multiFut.topologyVersion().compareTo(topVer) <= 0) {
                if (fut == null)
                    fut = new GridCompoundFuture<>();

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
    @Nullable public GridDhtCacheEntry peekExx(KeyCacheObject key) {
        return (GridDhtCacheEntry)peekEx(key);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntryEx entryEx(KeyCacheObject key, boolean touch)
        throws GridDhtInvalidPartitionException {
        return super.entryEx(key, touch);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntryEx entryEx(KeyCacheObject key, AffinityTopologyVersion topVer) throws GridDhtInvalidPartitionException {
        return super.entryEx(key, topVer);
    }

    /**
     * @param key Key.
     * @return DHT entry.
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    public GridDhtCacheEntry entryExx(KeyCacheObject key) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry)entryEx(key);
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return DHT entry.
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    public GridDhtCacheEntry entryExx(KeyCacheObject key, AffinityTopologyVersion topVer) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry)entryEx(key, topVer);
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
    public GridCacheEntryEx entryExx(KeyCacheObject key, AffinityTopologyVersion topVer, boolean allowDetached, boolean touch) {
        try {
            return allowDetached && !ctx.affinity().localNode(key, topVer) ?
                createEntry(key) : entryEx(key, touch);
        }
        catch (GridDhtInvalidPartitionException e) {
            if (!allowDetached)
                throw e;

            return createEntry(key);
        }
    }

    /**
     * @param key Key for which entry should be returned.
     * @return Cache entry.
     */
    protected GridDistributedCacheEntry createEntry(KeyCacheObject key) {
        return new GridDhtDetachedCacheEntry(ctx, key, key.hashCode(), null, null, 0);
    }

    /** {@inheritDoc} */
    @Override public void localLoad(Collection<? extends K> keys, final ExpiryPolicy plc)
        throws IgniteCheckedException {
        if (ctx.store().isLocal()) {
            super.localLoad(keys, plc);

            return;
        }

        // Version for all loaded entries.
        final GridCacheVersion ver0 = ctx.shared().versions().nextForLoad(topology().topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        final ExpiryPolicy plc0 = plc != null ? plc : ctx.expiry();

        Collection<KeyCacheObject> keys0 = ctx.cacheKeysView(keys);

        ctx.store().loadAll(null, keys0, new CI2<KeyCacheObject, Object>() {
            @Override public void apply(KeyCacheObject key, Object val) {
                loadEntry(key, val, ver0, null, topVer, replicate, plc0);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(final IgniteBiPredicate<K, V> p, Object[] args) throws IgniteCheckedException {
        if (ctx.store().isLocal()) {
            super.localLoadCache(p, args);

            return;
        }

        // Version for all loaded entries.
        final GridCacheVersion ver0 = ctx.shared().versions().nextForLoad(topology().topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc0 = opCtx != null ? opCtx.expiry() : null;

        final ExpiryPolicy plc = plc0 != null ? plc0 : ctx.expiry();

        if (p != null)
            ctx.kernalContext().resource().injectGeneric(p);

        try {
            ctx.store().loadCache(new CI3<KeyCacheObject, Object, GridCacheVersion>() {
                @Override public void apply(KeyCacheObject key, Object val, @Nullable GridCacheVersion ver) {
                    assert ver == null;

                    loadEntry(key, val, ver0, p, topVer, replicate, plc);
                }
            }, args);

        }
        finally {
            if (p instanceof PlatformCacheEntryFilter)
                ((PlatformCacheEntryFilter)p).onClose();
        }
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
    private void loadEntry(KeyCacheObject key,
        Object val,
        GridCacheVersion ver,
        @Nullable IgniteBiPredicate<K, V> p,
        AffinityTopologyVersion topVer,
        boolean replicate,
        @Nullable ExpiryPolicy plc) {
        if (p != null && !p.apply(key.<K>value(ctx.cacheObjectContext(), false), (V)val))
            return;

        try {
            GridDhtLocalPartition part = top.localPartition(ctx.affinity().partition(key),
                AffinityTopologyVersion.NONE, true);

            // Reserve to make sure that partition does not get unloaded.
            if (part.reserve()) {
                GridCacheEntryEx entry = null;

                try {
                    long ttl = CU.ttlForLoad(plc);

                    if (ttl == CU.TTL_ZERO)
                        return;

                    CacheObject cacheVal = ctx.toCacheObject(val);

                    entry = entryEx(key, false);

                    entry.initialValue(cacheVal, ver, ttl, CU.EXPIRE_TIME_CALCULATE, false, topVer,
                        replicate ? DR_LOAD : DR_NONE);
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

        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        for (GridDhtLocalPartition p : topology().currentLocalPartitions()) {
            if (p.primary(topVer))
                sum += p.publicSize();
        }

        return sum;
    }

    /**
     * This method is used internally. Use
     * {@link #getDhtAsync(UUID, long, LinkedHashMap, boolean, boolean, AffinityTopologyVersion, UUID, int, IgniteCacheExpiryPolicy, boolean)}
     * method instead to retrieve DHT value.
     *
     * @param keys {@inheritDoc}
     * @param forcePrimary {@inheritDoc}
     * @param skipTx {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable GridCacheEntryEx entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        boolean skipVals,
        boolean canRemap
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        return getAllAsync(keys,
            opCtx == null || !opCtx.skipStore(),
            null,
            /*don't check local tx. */false,
            subjId,
            taskName,
            deserializePortable,
            forcePrimary,
            null,
            skipVals,
            canRemap);
    }

    /**
     * @param keys Keys to get
     * @param readThrough Read through flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @return Get future.
     */
    IgniteInternalFuture<Map<KeyCacheObject, CacheObject>> getDhtAllAsync(
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        @Nullable UUID subjId,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiry,
        boolean skipVals,
        boolean canRemap
        ) {
        return getAllAsync0(keys,
            readThrough,
            /*don't check local tx. */false,
            subjId,
            taskName,
            false,
            expiry,
            skipVals,
            /*keep cache objects*/true,
            canRemap);
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
     * @param expiry Expiry policy.
     * @return DHT future.
     */
    public GridDhtFuture<Collection<GridCacheEntryInfo>> getDhtAsync(UUID reader,
        long msgId,
        LinkedHashMap<KeyCacheObject, Boolean> keys,
        boolean readThrough,
        boolean reload,
        AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        int taskNameHash,
        @Nullable IgniteCacheExpiryPolicy expiry,
        boolean skipVals) {
        GridDhtGetFuture<K, V> fut = new GridDhtGetFuture<>(ctx,
            msgId,
            reader,
            keys,
            readThrough,
            reload,
            /*tx*/null,
            topVer,
            subjId,
            taskNameHash,
            expiry,
            skipVals);

        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param req Get request.
     */
    protected void processNearGetRequest(final UUID nodeId, final GridNearGetRequest req) {
        assert ctx.affinityNode();

        long ttl = req.accessTtl();

        final CacheExpiryPolicy expiryPlc = CacheExpiryPolicy.forAccess(ttl);

        IgniteInternalFuture<Collection<GridCacheEntryInfo>> fut =
            getDhtAsync(nodeId,
                req.messageId(),
                req.keys(),
                req.readThrough(),
                req.reload(),
                req.topologyVersion(),
                req.subjectId(),
                req.taskNameHash(),
                expiryPlc,
                req.skipValues());

        fut.listen(new CI1<IgniteInternalFuture<Collection<GridCacheEntryInfo>>>() {
            @Override public void apply(IgniteInternalFuture<Collection<GridCacheEntryInfo>> f) {
                GridNearGetResponse res = new GridNearGetResponse(ctx.cacheId(),
                    req.futureId(),
                    req.miniId(),
                    req.version());

                GridDhtFuture<Collection<GridCacheEntryInfo>> fut =
                    (GridDhtFuture<Collection<GridCacheEntryInfo>>)f;

                try {
                    Collection<GridCacheEntryInfo> entries = fut.get();

                    res.entries(entries);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed processing get request: " + req, e);

                    res.error(e);
                }

                if (!F.isEmpty(fut.invalidPartitions()))
                    res.invalidPartitions(fut.invalidPartitions(), ctx.shared().exchange().readyAffinityVersion());
                else
                    res.invalidPartitions(fut.invalidPartitions(), req.topologyVersion());

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
                    Map<KeyCacheObject, GridCacheVersion> entries = expiryPlc.entries();

                    assert entries != null && !entries.isEmpty();

                    Map<ClusterNode, GridCacheTtlUpdateRequest> reqMap = new HashMap<>();

                    AffinityTopologyVersion topVer = new AffinityTopologyVersion(ctx.discovery().topologyVersion());

                    for (Map.Entry<KeyCacheObject, GridCacheVersion> e : entries.entrySet()) {
                        List<ClusterNode> nodes = ctx.affinity().nodes(e.getKey(), topVer);

                        for (int i = 0; i < nodes.size(); i++) {
                            ClusterNode node = nodes.get(i);

                            if (!node.isLocal()) {
                                GridCacheTtlUpdateRequest req = reqMap.get(node);

                                if (req == null) {
                                    reqMap.put(node, req = new GridCacheTtlUpdateRequest(ctx.cacheId(),
                                        topVer,
                                        expiryPlc.forAccess()));
                                }

                                req.addEntry(e.getKey(), e.getValue());
                            }
                        }
                    }

                    Map<UUID, Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>>> rdrs = expiryPlc.readers();

                    if (rdrs != null) {
                        assert !rdrs.isEmpty();

                        for (Map.Entry<UUID, Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>>> e : rdrs.entrySet()) {
                            ClusterNode node = ctx.node(e.getKey());

                            if (node != null) {
                                GridCacheTtlUpdateRequest req = reqMap.get(node);

                                if (req == null) {
                                    reqMap.put(node, req = new GridCacheTtlUpdateRequest(ctx.cacheId(),
                                        topVer,
                                        expiryPlc.forAccess()));
                                }

                                for (IgniteBiTuple<KeyCacheObject, GridCacheVersion> t : e.getValue())
                                    req.addNearEntry(t.get1(), t.get2());
                            }
                        }
                    }

                    for (Map.Entry<ClusterNode, GridCacheTtlUpdateRequest> req : reqMap.entrySet()) {
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
    private void processTtlUpdateRequest(GridCacheTtlUpdateRequest req) {
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
        List<KeyCacheObject> keys,
        List<GridCacheVersion> vers,
        long ttl) {
        assert !F.isEmpty(keys);
        assert keys.size() == vers.size();

        int size = keys.size();

        boolean swap = cache.context().isSwapOrOffheapEnabled();

        for (int i = 0; i < size; i++) {
            try {
                GridCacheEntryEx entry = null;

                try {
                    if (swap) {
                        entry = cache.entryEx(keys.get(i));

                        entry.unswap(false);
                    }
                    else
                        entry = cache.peekEx(keys.get(i));

                    if (entry != null)
                        entry.updateTtl(vers.get(i), ttl);
                }
                finally {
                    if (entry != null)
                        cache.context().evicts().touch(entry, AffinityTopologyVersion.NONE);
                }
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to unswap entry.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys) {
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
            final GridDhtLocalPartition part = ctx.topology().localPartition(partId,
                ctx.discovery().topologyVersionEx(), false);

            Iterator<GridDhtCacheEntry> partIt = part == null ? null : part.entries().iterator();

            return new PartitionEntryIterator(partIt);
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

            try {
                return partId == ctx.affinity().partition(entry.getKey()) &&
                    F.eq(entry.getValue(), localPeek(entry.getKey(), CachePeekModes.ONHEAP_ONLY, null));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int size() {
            GridDhtLocalPartition part = ctx.topology().localPartition(partId,
                new AffinityTopologyVersion(ctx.discovery().topologyVersion()), false);

            return part != null ? part.publicSize() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PartitionEntrySet.class, this, "super", super.toString());
        }
    }

    /** {@inheritDoc} */
    @Override public List<GridCacheClearAllRunnable<K, V>> splitClearLocally() {
        return ctx.affinityNode() ? super.splitClearLocally() :
            Collections.<GridCacheClearAllRunnable<K, V>>emptyList();
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert entry.isDht();

        GridDhtLocalPartition part = topology().localPartition(entry.partition(), AffinityTopologyVersion.NONE,
            false);

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
     * @param expVer Expected topology version.
     * @param curVer Current topology version.
     * @return {@code True} if cache affinity changed and operation should be remapped.
     */
    protected final boolean needRemap(AffinityTopologyVersion expVer, AffinityTopologyVersion curVer) {
        if (expVer.equals(curVer))
            return false;

        Collection<ClusterNode> cacheNodes0 = ctx.discovery().cacheAffinityNodes(ctx.name(), expVer);
        Collection<ClusterNode> cacheNodes1 = ctx.discovery().cacheAffinityNodes(ctx.name(), curVer);

        return !cacheNodes0.equals(cacheNodes1);
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
            final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

            final Iterator<GridDhtLocalPartition> partIt = topology().currentLocalPartitions().iterator();

            Iterator<GridCacheEntryEx> it = new Iterator<GridCacheEntryEx>() {
                private GridCacheEntryEx next;

                private Iterator<GridDhtCacheEntry> curIt;

                {
                    advance();
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                @Override public GridCacheEntryEx next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    GridCacheEntryEx e = next;

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
                                GridDhtLocalPartition part = partIt.next();

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
    private class PartitionEntryIterator extends GridIteratorAdapter<Cache.Entry<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Next entry. */
        private Cache.Entry<K, V> entry;

        /** Last seen entry to support remove. */
        private Cache.Entry<K, V> last;

        /** Partition iterator. */
        private final Iterator<GridDhtCacheEntry> partIt;

        /**
         * @param partIt Partition iterator.
         */
        private PartitionEntryIterator(@Nullable Iterator<GridDhtCacheEntry> partIt) {
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

            ctx.grid().cache(ctx.name()).remove(last.getKey(), last.getValue());
        }

        /**
         *
         */
        private void advance() {
            if (partIt != null) {
                while (partIt.hasNext()) {
                    GridDhtCacheEntry next = partIt.next();

                    if (next.isInternal() || !next.visitable(CU.empty0()))
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
    @SuppressWarnings("TypeMayBeWeakened")
    private static class MultiUpdateFuture extends GridFutureAdapter<IgniteUuid> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Topology version. */
        private AffinityTopologyVersion topVer;

        /**
         * @param topVer Topology version.
         */
        private MultiUpdateFuture(@NotNull AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /**
         * @return Topology version.
         */
        private AffinityTopologyVersion topologyVersion() {
            return topVer;
        }
    }
}
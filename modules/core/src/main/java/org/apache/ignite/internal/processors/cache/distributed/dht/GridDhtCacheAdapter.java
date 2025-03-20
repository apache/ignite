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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.EntryGetWithTtlResult;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllRunnable;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.ReaderArguments;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTtlUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_LOAD;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.util.GridConcurrentFactory.newMap;

/**
 * DHT cache adapter.
 */
public abstract class GridDhtCacheAdapter<K, V> extends GridDistributedCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Force key futures. */
    private final ConcurrentMap<IgniteUuid, GridDhtForceKeysFuture<?, ?>> forceKeyFuts = newMap();

    /** */
    private volatile boolean stopping;

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            DiscoveryEvent e = (DiscoveryEvent)evt;

            ClusterNode loc = ctx.localNode();

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED : e;

            final ClusterNode n = e.eventNode();

            assert !loc.id().equals(n.id());

            for (GridDhtForceKeysFuture<?, ?> f : forceKeyFuts.values())
                f.onDiscoveryEvent(e);
        }
    };

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridDhtCacheAdapter() {
        // No-op.
    }

    /**
     * Adds future to future map.
     *
     * @param fut Future to add.
     * @return {@code False} if node cache is stopping and future was completed with error.
     */
    public boolean addFuture(GridDhtForceKeysFuture<?, ?> fut) {
        forceKeyFuts.put(fut.futureId(), fut);

        if (stopping) {
            fut.onDone(stopError());

            return false;
        }

        return true;
    }

    /**
     * Removes future from future map.
     *
     * @param fut Future to remove.
     */
    public void removeFuture(GridDhtForceKeysFuture<?, ?> fut) {
        forceKeyFuts.remove(fut.futureId(), fut);
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    protected final void processForceKeyResponse(ClusterNode node, GridDhtForceKeysResponse msg) {
        GridDhtForceKeysFuture<?, ?> f = forceKeyFuts.get(msg.futureId());

        if (f != null)
            f.onResult(msg);
        else if (log.isDebugEnabled())
            log.debug("Receive force key response for unknown future (is it duplicate?) [nodeId=" + node.id() +
                ", res=" + msg + ']');
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    protected final void processForceKeysRequest(final ClusterNode node, final GridDhtForceKeysRequest msg) {
        IgniteInternalFuture<?> fut = ctx.mvcc().finishKeys(msg.keys(), msg.cacheId(), msg.topologyVersion());

        if (fut.isDone())
            processForceKeysRequest0(node, msg);
        else
            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> t) {
                    processForceKeysRequest0(node, msg);
                }
            });
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    private void processForceKeysRequest0(ClusterNode node, GridDhtForceKeysRequest msg) {
        try {
            ClusterNode loc = ctx.localNode();

            GridDhtForceKeysResponse res = new GridDhtForceKeysResponse(
                ctx.cacheId(),
                msg.futureId(),
                msg.miniId()
            );

            GridDhtPartitionTopology top = ctx.topology();

            for (KeyCacheObject k : msg.keys()) {
                int p = ctx.affinity().partition(k);

                GridDhtLocalPartition locPart = top.localPartition(p, AffinityTopologyVersion.NONE, false);

                // If this node is no longer an owner.
                if (locPart == null && !top.owners(p).contains(loc)) {
                    res.addMissed(k);

                    continue;
                }

                GridCacheEntryEx entry;

                while (true) {
                    ctx.shared().database().checkpointReadLock();

                    try {
                        entry = ctx.dht().entryEx(k);

                        entry.unswap();

                        GridCacheEntryInfo info = entry.info();

                        if (info == null) {
                            assert entry.obsolete() : entry;

                            continue;
                        }

                        if (!info.isNew())
                            res.addInfo(info);

                        entry.touch();

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry: " + k);
                    }
                    catch (GridDhtInvalidPartitionException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Local node is no longer an owner: " + p);

                        res.addMissed(k);

                        break;
                    }
                    finally {
                        ctx.shared().database().checkpointReadUnlock();
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Sending force key response [node=" + node.id() + ", res=" + res + ']');

            ctx.io().send(node, res, ctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Received force key request form failed node (will ignore) [nodeId=" + node.id() +
                    ", req=" + msg + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to reply to force key request [nodeId=" + node.id() + ", req=" + msg + ']', e);
        }
    }

    /**
     *
     */
    public void dumpDebugInfo() {
        if (!forceKeyFuts.isEmpty()) {
            U.warn(log, "Pending force key futures [cache=" + ctx.name() + "]:");

            for (GridDhtForceKeysFuture fut : forceKeyFuts.values())
                U.warn(log, ">>> " + fut);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        super.onKernalStop();

        stopping = true;

        IgniteCheckedException err = stopError();

        for (GridDhtForceKeysFuture fut : forceKeyFuts.values())
            fut.onDone(err);

        ctx.gridEvents().removeLocalEventListener(discoLsnr);
    }

    /**
     * @return Node stop exception.
     */
    private IgniteCheckedException stopError() {
        return new NodeStoppingException("Operation has been cancelled (cache or node is stopping).");
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near get response.
     */
    protected final void processNearGetResponse(UUID nodeId, GridNearGetResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing near get response [nodeId=" + nodeId + ", res=" + res + ']');

        CacheGetFuture fut = (CacheGetFuture)ctx.mvcc().future(res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Near get response.
     */
    protected void processNearSingleGetResponse(UUID nodeId, GridNearSingleGetResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing near get response [nodeId=" + nodeId + ", res=" + res + ']');

        GridPartitionedSingleGetFuture fut = (GridPartitionedSingleGetFuture)ctx.mvcc()
            .future(new IgniteUuid(IgniteUuid.VM_ID, res.futureId()));

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param ctx Context.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx) {
        this(ctx, new GridCachePartitionedConcurrentMap(ctx.group()));
    }

    /**
     * Constructor used for near-only cache.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        assert !ctx.isRecoveryMode() : "Registering message handlers in recovery mode [cacheName=" + name() + ']';

        ctx.io().addCacheHandler(ctx.cacheId(), ctx.startTopologyVersion(), GridCacheTtlUpdateRequest.class,
            (CI2<UUID, GridCacheTtlUpdateRequest>)this::processTtlUpdateRequest);

        ctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        super.printMemoryStats();

        ctx.group().topology().printMemoryStats(1024);
    }

    /**
     * @return Near cache.
     */
    public abstract GridNearCacheAdapter<K, V> near();

    /**
     * @return Partition topology.
     */
    public GridDhtPartitionTopology topology() {
        return ctx.group().topology();
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader preloader() {
        return ctx.group().preloader();
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
    @Override public GridCacheEntryEx entryEx(KeyCacheObject key,
        AffinityTopologyVersion topVer) throws GridDhtInvalidPartitionException {
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
    public GridDhtCacheEntry entryExx(KeyCacheObject key,
        AffinityTopologyVersion topVer) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry)entryEx(key, topVer);
    }

    /**
     * @param key Key for which entry should be returned.
     * @return Cache entry.
     */
    protected GridDistributedCacheEntry createEntry(KeyCacheObject key) {
        return new GridDhtDetachedCacheEntry(ctx, key);
    }

    /** {@inheritDoc} */
    @Override public void localLoad(Collection<? extends K> keys, final ExpiryPolicy plc, final boolean keepBinary)
        throws IgniteCheckedException {
        if (ctx.store().isLocal()) {
            super.localLoad(keys, plc, keepBinary);

            return;
        }

        // Version for all loaded entries.
        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        final GridCacheVersion ver0 = ctx.shared().versions().nextForLoad(topVer.topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

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

        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        // Version for all loaded entries.
        final GridCacheVersion ver0 = ctx.shared().versions().nextForLoad(topVer.topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc0 = opCtx != null ? opCtx.expiry() : null;

        final ExpiryPolicy plc = plc0 != null ? plc0 : ctx.expiry();

        final IgniteBiPredicate<K, V> pred;

        if (p != null) {
            ctx.kernalContext().resource().injectGeneric(p);

            pred = SecurityUtils.sandboxedProxy(ctx.kernalContext(), IgniteBiPredicate.class, p);
        }
        else
            pred = null;

        try {
            ctx.store().loadCache(new CI3<KeyCacheObject, Object, GridCacheVersion>() {
                @Override public void apply(KeyCacheObject key, Object val, @Nullable GridCacheVersion ver) {
                    assert ver == null;

                    loadEntry(key, val, ver0, pred, topVer, replicate, plc);
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
            GridDhtLocalPartition part = ctx.group().topology().localPartition(ctx.affinity().partition(key),
                AffinityTopologyVersion.NONE, true);

            // Reserve to make sure that partition does not get unloaded.
            if (part.reserve()) {
                GridCacheEntryEx entry = null;

                ctx.shared().database().checkpointReadLock();

                try {
                    long ttl = CU.ttlForLoad(plc);

                    if (ttl == CU.TTL_ZERO)
                        return;

                    CacheObject cacheVal = ctx.toCacheObject(val);

                    entry = entryEx(key);

                    entry.initialValue(cacheVal,
                        ver,
                        ttl,
                        CU.EXPIRE_TIME_CALCULATE,
                        false,
                        topVer,
                        replicate ? DR_LOAD : DR_NONE,
                        true,
                        false);
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
                        entry.touch();

                    part.release();

                    ctx.shared().database().checkpointReadUnlock();
                }
            }
            else if (log.isDebugEnabled())
                log.debug("Will node load entry into cache (partition is invalid): " + part);
        }
        catch (GridDhtInvalidPartitionException e) {
            if (log.isDebugEnabled())
                log.debug(S.toString("Ignoring entry for partition that does not belong",
                    "key", key, true,
                    "val", val, true,
                    "err", e, false));
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return (int)sizeLong();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
        long sum = 0;

        for (GridDhtLocalPartition p : topology().currentLocalPartitions())
            sum += p.dataStore().cacheSize(ctx.cacheId());

        return sum;
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return (int)primarySizeLong();
    }

    /** {@inheritDoc} */
    @Override public long primarySizeLong() {
        long sum = 0;

        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        for (GridDhtLocalPartition p : topology().currentLocalPartitions()) {
            if (p.primary(topVer))
                sum += p.dataStore().cacheSize(ctx.cacheId());
        }

        return sum;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy,
        boolean skipVals,
        boolean needVer
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        warnIfUnordered(keys, BulkOperation.GET);

        return getAllAsync0(
            ctx.cacheKeysView(keys),
            null,
            opCtx == null || !opCtx.skipStore(),
            taskName,
            deserializeBinary,
            null,
            skipVals,
            /*keep cache objects*/false,
            opCtx != null && opCtx.recovery(),
            needVer,
            null);
    }

    /**
     * @param keys Keys.
     * @param readerArgs Near cache reader will be added if not null.
     * @param readThrough Read-through flag.
     * @param taskName Task name/
     * @param deserializeBinary Deserialize binary flag.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param txLbl Transaction label.
     * @return Future.
     */
    private <K1, V1> IgniteInternalFuture<Map<K1, V1>> getAllAsync0(
        @Nullable final Collection<KeyCacheObject> keys,
        @Nullable final ReaderArguments readerArgs,
        final boolean readThrough,
        final String taskName,
        final boolean deserializeBinary,
        @Nullable final IgniteCacheExpiryPolicy expiry,
        final boolean skipVals,
        final boolean keepCacheObjects,
        final boolean recovery,
        final boolean needVer,
        @Nullable String txLbl
    ) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.emptyMap());

        Map<KeyCacheObject, EntryGetResult> misses = null;

        Set<GridCacheEntryEx> newLocEntries = null;

        ctx.shared().database().checkpointReadLock();

        try {
            int keysSize = keys.size();

            GridDhtTopologyFuture topFut = ctx.shared().exchange().lastFinishedFuture();

            Throwable ex = topFut != null ? topFut.validateCache(ctx, recovery, /*read*/true, null, keys) : null;

            if (ex != null)
                return new GridFinishedFuture<>(ex);

            final Map<K1, V1> map = keysSize == 1
                ? (Map<K1, V1>)new IgniteBiTuple<>()
                : U.newHashMap(keysSize);

            final boolean storeEnabled = !skipVals && readThrough && ctx.readThrough();

            boolean readNoEntry = ctx.readNoEntry(expiry, readerArgs != null);

            for (KeyCacheObject key : keys) {
                while (true) {
                    try {
                        EntryGetResult res = null;

                        boolean evt = !skipVals;
                        boolean updateMetrics = !skipVals;

                        GridCacheEntryEx entry = null;

                        boolean skipEntry = readNoEntry;

                        if (readNoEntry) {
                            CacheDataRow row = ctx.offheap().read(ctx, key);

                            if (row != null) {
                                long expireTime = row.expireTime();

                                if (expireTime != 0) {
                                    if (expireTime > U.currentTimeMillis()) {
                                        res = new EntryGetWithTtlResult(row.value(),
                                            row.version(),
                                            false,
                                            expireTime,
                                            0);
                                    }
                                    else
                                        skipEntry = false;
                                }
                                else
                                    res = new EntryGetResult(row.value(), row.version(), false);
                            }

                            if (res != null) {
                                if (evt) {
                                    ctx.events().readEvent(key,
                                        null,
                                        txLbl,
                                        row.value(),
                                        taskName,
                                        !deserializeBinary);
                                }

                                if (updateMetrics && ctx.statisticsEnabled())
                                    ctx.cache().metrics0().onRead(true);
                            }
                            else if (storeEnabled)
                                skipEntry = false;
                        }

                        if (!skipEntry) {
                            boolean isNewLocEntry = this.map.getEntry(ctx, key) == null;

                            entry = entryEx(key);

                            if (entry == null) {
                                if (!skipVals && ctx.statisticsEnabled())
                                    ctx.cache().metrics0().onRead(false);

                                break;
                            }

                            if (isNewLocEntry) {
                                if (newLocEntries == null)
                                    newLocEntries = new HashSet<>();

                                newLocEntries.add(entry);
                            }

                            if (storeEnabled) {
                                res = entry.innerGetAndReserveForLoad(updateMetrics,
                                    evt,
                                    taskName,
                                    expiry,
                                    !deserializeBinary,
                                    readerArgs);

                                assert res != null;

                                if (res.value() == null) {
                                    if (misses == null)
                                        misses = new HashMap<>();

                                    misses.put(key, res);

                                    res = null;
                                }
                            }
                            else {
                                res = entry.innerGetVersioned(
                                    null,
                                    null,
                                    updateMetrics,
                                    evt,
                                    null,
                                    taskName,
                                    expiry,
                                    !deserializeBinary,
                                    readerArgs);

                                if (res == null)
                                    entry.touch();
                            }
                        }

                        if (res != null) {
                            ctx.addResult(map,
                                key,
                                res,
                                skipVals,
                                keepCacheObjects,
                                deserializeBinary,
                                true,
                                needVer);

                            if (entry != null)
                                entry.touch();

                            if (keysSize == 1)
                                // Safe to return because no locks are required in READ_COMMITTED mode.
                                return new GridFinishedFuture<>(map);
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in getAllAsync(..) method (will retry): " + key);
                    }
                }
            }

            if (storeEnabled && misses != null) {
                final Map<KeyCacheObject, EntryGetResult> loadKeys = misses;

                final Collection<KeyCacheObject> loaded = new HashSet<>();

                return new GridEmbeddedFuture(
                    ctx.closures().callLocalSafe(ctx.projectSafe(new GPC<Map<K1, V1>>() {
                        @Override public Map<K1, V1> call() throws Exception {
                            ctx.store().loadAll(null/*tx*/, loadKeys.keySet(), new CI2<KeyCacheObject, Object>() {
                                @Override public void apply(KeyCacheObject key, Object val) {
                                    EntryGetResult res = loadKeys.get(key);

                                    if (res == null || val == null)
                                        return;

                                    loaded.add(key);

                                    CacheObject cacheVal = ctx.toCacheObject(val);

                                    while (true) {
                                        GridCacheEntryEx entry = null;

                                        try {
                                            ctx.shared().database().ensureFreeSpace(ctx.dataRegion());
                                        }
                                        catch (IgniteCheckedException e) {
                                            // Wrap errors (will be unwrapped).
                                            throw new GridClosureException(e);
                                        }

                                        ctx.shared().database().checkpointReadLock();

                                        try {
                                            entry = entryEx(key);

                                            entry.unswap();

                                            GridCacheVersion newVer = nextVersion();

                                            EntryGetResult verVal = entry.versionedValue(
                                                cacheVal,
                                                res.version(),
                                                newVer,
                                                expiry,
                                                readerArgs);

                                            if (log.isDebugEnabled())
                                                log.debug("Set value loaded from store into entry [" +
                                                    "oldVer=" + res.version() +
                                                    ", newVer=" + verVal.version() + ", " +
                                                    "entry=" + entry + ']');

                                            // Don't put key-value pair into result map if value is null.
                                            if (verVal.value() != null) {
                                                ctx.addResult(map,
                                                    key,
                                                    verVal,
                                                    skipVals,
                                                    keepCacheObjects,
                                                    deserializeBinary,
                                                    true,
                                                    needVer);
                                            }
                                            else {
                                                ctx.addResult(
                                                    map,
                                                    key,
                                                    new EntryGetResult(cacheVal, res.version()),
                                                    skipVals,
                                                    keepCacheObjects,
                                                    deserializeBinary,
                                                    false,
                                                    needVer
                                                );
                                            }

                                            entry.touch();

                                            break;
                                        }
                                        catch (GridCacheEntryRemovedException ignore) {
                                            if (log.isDebugEnabled())
                                                log.debug("Got removed entry during getAllAsync (will retry): " +
                                                    entry);
                                        }
                                        catch (IgniteCheckedException e) {
                                            // Wrap errors (will be unwrapped).
                                            throw new GridClosureException(e);
                                        }
                                        finally {
                                            ctx.shared().database().checkpointReadUnlock();
                                        }
                                    }
                                }
                            });

                            clearReservationsIfNeeded(loadKeys, loaded);

                            return map;
                        }
                    }), true),
                    new C2<Map<K, V>, Exception, IgniteInternalFuture<Map<K, V>>>() {
                        @Override public IgniteInternalFuture<Map<K, V>> apply(Map<K, V> map, Exception e) {
                            if (e != null) {
                                clearReservationsIfNeeded(loadKeys, loaded);

                                return new GridFinishedFuture<>(e);
                            }

                            Collection<KeyCacheObject> notFound = new HashSet<>(loadKeys.keySet());

                            notFound.removeAll(loaded);

                            // Touch entries that were not found in store.
                            for (KeyCacheObject key : notFound) {
                                GridCacheEntryEx entry = peekEx(key);

                                if (entry != null)
                                    entry.touch();
                            }

                            // There were no misses.
                            return new GridFinishedFuture<>(Collections.emptyMap());
                        }
                    },
                    new C2<Map<K1, V1>, Exception, Map<K1, V1>>() {
                        @Override public Map<K1, V1> apply(Map<K1, V1> loaded, Exception e) {
                            if (e == null)
                                map.putAll(loaded);

                            return map;
                        }
                    }
                );
            }
            else
                // Misses can be non-zero only if store is enabled.
                assert misses == null;

            return new GridFinishedFuture<>(map);
        }
        catch (RuntimeException | AssertionError e) {
            if (misses != null) {
                for (KeyCacheObject key0 : misses.keySet()) {
                    GridCacheEntryEx entry = peekEx(key0);
                    if (entry != null)
                        entry.touch();
                }
            }

            if (newLocEntries != null) {
                for (GridCacheEntryEx entry : newLocEntries)
                    removeEntry(entry);
            }

            return new GridFinishedFuture<>(e);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
        finally {
            ctx.shared().database().checkpointReadUnlock();
        }
    }

    /**
     * @param loadKeys Keys to load.
     * @param loaded Actually loaded keys.
     */
    private void clearReservationsIfNeeded(Map<KeyCacheObject, EntryGetResult> loadKeys, Collection<KeyCacheObject> loaded) {
        if (loaded.size() != loadKeys.size()) {
            for (Map.Entry<KeyCacheObject, EntryGetResult> e : loadKeys.entrySet()) {
                if (loaded.contains(e.getKey()))
                    continue;

                GridCacheEntryEx entry = peekEx(e.getKey());

                if (entry != null) {
                    if (e.getValue().reserved())
                        entry.clearReserveForLoad(e.getValue().version());

                    entry.touch();
                }
            }
        }
    }

    /**
     * @param keys Keys to get
     * @param readerArgs Reader will be added if not null.
     * @param readThrough Read through flag.
     * @param taskName Task name.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @param txLbl Transaction label.
     * @return Get future.
     */
    IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> getDhtAllAsync(
        Collection<KeyCacheObject> keys,
        @Nullable final ReaderArguments readerArgs,
        boolean readThrough,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiry,
        boolean skipVals,
        boolean recovery,
        @Nullable String txLbl
    ) {
        return getAllAsync0(keys,
            readerArgs,
            readThrough,
            taskName,
            false,
            expiry,
            skipVals,
            /*keep cache objects*/true,
            recovery,
            /*need version*/true,
            txLbl);
    }

    /**
     * @param reader Reader node ID.
     * @param msgId Message ID.
     * @param keys Keys to get.
     * @param addReaders Add readers flag.
     * @param readThrough Read through flag.
     * @param topVer Topology version.
     * @param taskNameHash Task name hash code.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @param txLbl Transaction label.
     * @return DHT future.
     */
    public GridDhtFuture<Collection<GridCacheEntryInfo>> getDhtAsync(UUID reader,
        long msgId,
        Map<KeyCacheObject, Boolean> keys,
        boolean addReaders,
        boolean readThrough,
        AffinityTopologyVersion topVer,
        int taskNameHash,
        @Nullable IgniteCacheExpiryPolicy expiry,
        boolean skipVals,
        boolean recovery,
        @Nullable String txLbl
    ) {
        GridDhtGetFuture<K, V> fut = new GridDhtGetFuture<>(ctx,
            msgId,
            reader,
            keys,
            readThrough,
            topVer,
            taskNameHash,
            expiry,
            skipVals,
            recovery,
            addReaders,
            txLbl);

        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param msgId Message ID.
     * @param key Key.
     * @param addRdr Add reader flag.
     * @param readThrough Read through flag.
     * @param topVer Topology version flag.
     * @param taskNameHash Task name hash.
     * @param expiry Expiry.
     * @param skipVals Skip vals flag.
     * @param txLbl Transaction label.
     * @return Future for the operation.
     */
    GridDhtGetSingleFuture getDhtSingleAsync(
        UUID nodeId,
        long msgId,
        KeyCacheObject key,
        boolean addRdr,
        boolean readThrough,
        AffinityTopologyVersion topVer,
        int taskNameHash,
        @Nullable IgniteCacheExpiryPolicy expiry,
        boolean skipVals,
        boolean recovery,
        String txLbl
    ) {
        GridDhtGetSingleFuture fut = new GridDhtGetSingleFuture<>(
            ctx,
            msgId,
            nodeId,
            key,
            addRdr,
            readThrough,
            topVer,
            taskNameHash,
            expiry,
            skipVals,
            recovery,
            txLbl);

        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param req Get request.
     */
    protected void processNearSingleGetRequest(final UUID nodeId, final GridNearSingleGetRequest req) {
        assert ctx.affinityNode();

        final CacheExpiryPolicy expiryPlc = CacheExpiryPolicy.fromRemote(req.createTtl(), req.accessTtl());

        IgniteInternalFuture<GridCacheEntryInfo> fut =
            getDhtSingleAsync(
                nodeId,
                req.messageId(),
                req.key(),
                req.addReader(),
                req.readThrough(),
                req.topologyVersion(),
                req.taskNameHash(),
                expiryPlc,
                req.skipValues(),
                req.recovery(),
                req.txLabel());

        fut.listen(new CI1<IgniteInternalFuture<GridCacheEntryInfo>>() {
            @Override public void apply(IgniteInternalFuture<GridCacheEntryInfo> f) {
                GridNearSingleGetResponse res;

                GridDhtFuture<GridCacheEntryInfo> fut = (GridDhtFuture<GridCacheEntryInfo>)f;

                try {
                    GridCacheEntryInfo info = fut.get();

                    if (F.isEmpty(fut.invalidPartitions())) {
                        Message res0 = null;

                        if (info != null) {
                            if (req.needEntryInfo()) {
                                info.key(null);

                                res0 = info;
                            }
                            else if (req.needVersion())
                                res0 = new CacheVersionedValue(info.value(), info.version());
                            else
                                res0 = info.value();
                        }

                        res = new GridNearSingleGetResponse(
                            ctx.cacheId(),
                            req.futureId(),
                            null,
                            res0,
                            false,
                            req.addDeploymentInfo()
                        );

                        if (info != null && req.skipValues())
                            res.setContainsValue();
                    }
                    else {
                        AffinityTopologyVersion topVer = ctx.shared().exchange().lastTopologyFuture().initialVersion();

                        res = new GridNearSingleGetResponse(
                            ctx.cacheId(),
                            req.futureId(),
                            topVer,
                            null,
                            true,
                            req.addDeploymentInfo()
                        );
                    }
                }
                catch (NodeStoppingException ignored) {
                    return;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed processing get request: " + req, e);

                    res = new GridNearSingleGetResponse(ctx.cacheId(),
                        req.futureId(),
                        req.topologyVersion(),
                        null,
                        false,
                        req.addDeploymentInfo());

                    res.error(e);
                }

                try {
                    ctx.io().send(nodeId, res, ctx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send get response to node, node failed: " + nodeId);
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
     * @param nodeId Node ID.
     * @param req Get request.
     */
    protected void processNearGetRequest(final UUID nodeId, final GridNearGetRequest req) {
        assert ctx.affinityNode();

        final CacheExpiryPolicy expiryPlc = CacheExpiryPolicy.fromRemote(req.createTtl(), req.accessTtl());

        IgniteInternalFuture<Collection<GridCacheEntryInfo>> fut =
            getDhtAsync(nodeId,
                req.messageId(),
                req.keys(),
                req.addReaders(),
                req.readThrough(),
                req.topologyVersion(),
                req.taskNameHash(),
                expiryPlc,
                req.skipValues(),
                req.recovery(),
                req.txLabel());

        fut.listen(new CI1<IgniteInternalFuture<Collection<GridCacheEntryInfo>>>() {
            @Override public void apply(IgniteInternalFuture<Collection<GridCacheEntryInfo>> f) {
                GridNearGetResponse res = new GridNearGetResponse(ctx.cacheId(),
                    req.futureId(),
                    req.miniId(),
                    req.version(),
                    req.deployInfo() != null);

                GridDhtFuture<Collection<GridCacheEntryInfo>> fut =
                    (GridDhtFuture<Collection<GridCacheEntryInfo>>)f;

                try {
                    Collection<GridCacheEntryInfo> entries = fut.get();

                    res.entries(entries);
                }
                catch (NodeStoppingException ignored) {
                    return;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed processing get request: " + req, e);

                    res.error(e);
                }

                if (!F.isEmpty(fut.invalidPartitions())) {
                    AffinityTopologyVersion topVer = ctx.shared().exchange().lastTopologyFuture().initialVersion();

                    res.invalidPartitions(fut.invalidPartitions(), topVer);
                }

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
     * Initiates process of notifying all interested nodes that TTL was changed.
     * Directly sends requests to primary nodes and {@link IgniteCacheExpiryPolicy#readers()}.
     *
     * @param expiryPlc Expiry policy.
     */
    public void sendTtlUpdateRequest(@Nullable final IgniteCacheExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            sendTtlUpdateRequest(expiryPlc, ctx.shared().exchange().readyAffinityVersion(), null);
    }

    /**
     * @param expiryPlc Expiry policy.
     * @param topVer Topology version.
     * @param srcNodeId ID of the node that sent the initial ttl request.
     */
    private void sendTtlUpdateRequest(
        IgniteCacheExpiryPolicy expiryPlc,
        AffinityTopologyVersion topVer,
        @Nullable UUID srcNodeId
    ) {
        if (!F.isEmpty(expiryPlc.entries())) {
            Map<KeyCacheObject, GridCacheVersion> entries = Map.copyOf(expiryPlc.entries());

            Map<UUID, Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>>> rdrs =
                expiryPlc.readers() != null ? Map.copyOf(expiryPlc.readers()) : null;

            ctx.closures().runLocalSafe(new GridPlainRunnable() {
                @SuppressWarnings({"ForLoopReplaceableByForEach"})
                @Override public void run() {
                    Map<ClusterNode, GridCacheTtlUpdateRequest> reqMap = new HashMap<>();

                    for (Map.Entry<KeyCacheObject, GridCacheVersion> e : entries.entrySet()) {
                        ClusterNode primaryNode = ctx.affinity().primaryByKey(e.getKey(), topVer);

                        if (primaryNode.isLocal()) {
                            Collection<ClusterNode> nodes = ctx.affinity().backupsByKey(e.getKey(), topVer);

                            for (Iterator<ClusterNode> nodesIter = nodes.iterator(); nodesIter.hasNext(); ) {
                                ClusterNode node = nodesIter.next();

                                // There's no need to re-send ttl update request to the node
                                // that sent the initial ttl update request.
                                if (srcNodeId != null && srcNodeId.equals(node.id()))
                                    continue;

                                GridCacheTtlUpdateRequest req = reqMap.get(node);

                                if (req == null) {
                                    reqMap.put(node, req = new GridCacheTtlUpdateRequest(ctx.cacheId(),
                                        topVer,
                                        expiryPlc.forAccess()));
                                }

                                req.addEntry(e.getKey(), e.getValue());
                            }
                        }
                        else {
                            // There is no need to re-send ttl update requests if we are not on the primary node.
                            if (srcNodeId != null)
                                continue;

                            GridCacheTtlUpdateRequest req = reqMap.get(primaryNode);

                            if (req == null) {
                                reqMap.put(primaryNode, req = new GridCacheTtlUpdateRequest(ctx.cacheId(),
                                    topVer,
                                    expiryPlc.forAccess()));
                            }

                            req.addEntry(e.getKey(), e.getValue());
                        }
                    }

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
                            if (e instanceof ClusterTopologyCheckedException) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send TTC update request, node left: " + req.getKey());
                            }
                            else
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
    private void processTtlUpdateRequest(UUID srcNodeId, GridCacheTtlUpdateRequest req) {
        final CacheExpiryPolicy expiryPlc = CacheExpiryPolicy.fromRemote(CU.TTL_NOT_CHANGED, req.ttl());

        if (req.keys() != null)
            updateTtl(this, req.keys(), req.versions(), expiryPlc);

        if (req.nearKeys() != null) {
            GridNearCacheAdapter<K, V> near = near();

            assert near != null;

            updateTtl(near, req.nearKeys(), req.nearVersions(), expiryPlc);
        }

        sendTtlUpdateRequest(expiryPlc, req.topologyVersion(), srcNodeId);
    }

    /**
     * @param cache Cache.
     * @param keys Entries keys.
     * @param vers Entries versions.
     * @param expiryPlc Expiry policy.
     */
    private void updateTtl(
        GridCacheAdapter<K, V> cache,
        List<KeyCacheObject> keys,
        List<GridCacheVersion> vers,
        IgniteCacheExpiryPolicy expiryPlc
    ) {
        assert !F.isEmpty(keys);
        assert keys.size() == vers.size();

        int size = keys.size();

        for (int i = 0; i < size; i++) {
            try {
                GridCacheEntryEx entry = null;

                try {
                    ctx.shared().database().checkpointReadLock();

                    while (true) {
                        try {
                            entry = cache.entryEx(keys.get(i));

                            entry.unswap(false);

                            entry.updateTtl(vers.get(i), expiryPlc);

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry: " + entry);
                        }
                        catch (GridDhtInvalidPartitionException e) {
                            if (log.isDebugEnabled())
                                log.debug("Got GridDhtInvalidPartitionException: " + e);

                            break;
                        }
                    }
                }
                finally {
                    if (entry != null)
                        entry.touch();

                    ctx.shared().database().checkpointReadUnlock();
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
    @Override public String toString() {
        return S.toString(GridDhtCacheAdapter.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @Override public List<GridCacheClearAllRunnable<K, V>> splitClearLocally(boolean srv, boolean near,
        boolean readers) {
        return ctx.affinityNode() ? super.splitClearLocally(srv, near, readers) :
            Collections.<GridCacheClearAllRunnable<K, V>>emptyList();
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert entry.isDht();

        GridDhtLocalPartition part = topology().localPartition(entry.partition(), AffinityTopologyVersion.NONE,
            false);

        if (part != null)
            part.onDeferredDelete(entry.context().cacheId(), entry.key(), ver);
    }

    /**
     * @param rmtVer Topology version that the cache request was mapped to by the remote node.
     * @return {@code True} if cache affinity changed and operation should be remapped.
     */
    protected final boolean needRemap(AffinityTopologyVersion rmtVer) {
        return !ctx.affinity().isCompatibleWithCurrentTopologyVersion(rmtVer);
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param keepBinary Keep binary flag.
     * @return Local entries iterator.
     */
    public Iterator<Cache.Entry<K, V>> localEntriesIterator(final boolean primary,
        final boolean backup,
        final boolean keepBinary) {
        return localEntriesIterator(primary,
            backup,
            keepBinary,
            ctx.affinity().affinityTopologyVersion());
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param keepBinary Keep binary flag.
     * @param topVer Specified affinity topology version.
     * @return Local entries iterator.
     */
    private Iterator<Cache.Entry<K, V>> localEntriesIterator(final boolean primary,
        final boolean backup,
        final boolean keepBinary,
        final AffinityTopologyVersion topVer) {

        return iterator(localEntriesIteratorEx(primary, backup, topVer), !keepBinary);
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param topVer Specified affinity topology version.
     * @return Local entries iterator.
     */
    private Iterator<? extends GridCacheEntryEx> localEntriesIteratorEx(final boolean primary,
        final boolean backup,
        final AffinityTopologyVersion topVer) {
        assert primary || backup;

        if (primary && backup)
            return entries().iterator();
        else {
            final Iterator<GridDhtLocalPartition> partIt = topology().currentLocalPartitions().iterator();

            return new Iterator<GridCacheMapEntry>() {
                private GridCacheMapEntry next;

                private Iterator<GridCacheMapEntry> curIt;

                {
                    advance();
                }

                @Override public boolean hasNext() {
                    return next != null;
                }

                @Override public GridCacheMapEntry next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    GridCacheMapEntry e = next;

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
                                    curIt = part.entries(ctx.cacheId()).iterator();

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
        }
    }

    /**
     * Multi update future.
     */
    private static class MultiUpdateFuture extends GridFutureAdapter<IgniteUuid> {
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

    /**
     *
     */
    protected abstract class MessageHandler<M> implements IgniteBiInClosure<UUID, M> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(UUID nodeId, M msg) {
            ClusterNode node = ctx.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Received message from failed node [node=" + nodeId + ", msg=" + msg + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received message from node [node=" + nodeId + ", msg=" + msg + ']');

            onMessage(node, msg);
        }

        /**
         * @param node Node.
         * @param msg Message.
         */
        protected abstract void onMessage(ClusterNode node, M msg);
    }
}

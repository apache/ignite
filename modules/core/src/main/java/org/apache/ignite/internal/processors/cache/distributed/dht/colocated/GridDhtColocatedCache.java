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

package org.apache.ignite.internal.processors.cache.distributed.dht.colocated;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtEmbeddedFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFinishedFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTransactionalCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTransactionalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearUnlockRequest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Colocated cache.
 */
public class GridDhtColocatedCache<K, V> extends GridDhtTransactionalCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}
     */
    public GridDhtColocatedCache() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     */
    public GridDhtColocatedCache(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * Creates colocated cache with specified map.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    public GridDhtColocatedCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public boolean isColocated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.io().addCacheHandler(ctx.cacheId(), GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse>() {
            @Override public void apply(UUID nodeId, GridNearGetResponse res) {
                processNearGetResponse(nodeId, res);
            }
        });

        ctx.io().addCacheHandler(ctx.cacheId(), GridNearSingleGetResponse.class, new CI2<UUID, GridNearSingleGetResponse>() {
            @Override public void apply(UUID nodeId, GridNearSingleGetResponse res) {
                processNearSingleGetResponse(nodeId, res);
            }
        });

        ctx.io().addCacheHandler(ctx.cacheId(), GridNearLockResponse.class, new CI2<UUID, GridNearLockResponse>() {
            @Override public void apply(UUID nodeId, GridNearLockResponse res) {
                processLockResponse(nodeId, res);
            }
        });
    }

    /**
     * Gets or creates entry for given key and given topology version.
     *
     * @param key Key for entry.
     * @param topVer Topology version.
     * @param allowDetached Whether to allow detached entries. If {@code true} and node is not primary
     *      for given key, a new detached entry will be created. Otherwise, entry will be obtained from
     *      dht cache map.
     * @return Cache entry.
     * @throws GridDhtInvalidPartitionException If {@code allowDetached} is false and node is not primary
     *      for given key.
     */
    public GridDistributedCacheEntry entryExx(
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        boolean allowDetached
    ) {
        return allowDetached && !ctx.affinity().primaryByKey(ctx.localNode(), key, topVer) ?
            createEntry(key) : entryExx(key, topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        return ctx.mvcc().isLockedByThread(ctx.txKey(cacheKey), -1);
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        return ctx.mvcc().isLockedByThread(ctx.txKey(cacheKey), Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<V> getAsync(final K key,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        String taskName,
        final boolean deserializeBinary,
        final boolean skipVals,
        final boolean needVer) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (keyCheck)
            validateCacheKey(key);

        GridNearTxLocal tx = checkCurrentTx();

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        final boolean recovery = opCtx != null && opCtx.recovery();

        // Get operation bypass Tx in Mvcc mode.
        if (!ctx.mvccEnabled() && tx != null && !tx.implicit() && !skipTx) {
            return asyncOp(tx, new AsyncOp<V>() {
                @Override public IgniteInternalFuture<V> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                    IgniteInternalFuture<Map<Object, Object>> fut = tx.getAllAsync(ctx,
                        readyTopVer,
                        Collections.singleton(ctx.toCacheKeyObject(key)),
                        deserializeBinary,
                        skipVals,
                        false,
                        opCtx != null && opCtx.skipStore(),
                        recovery,
                        needVer);

                    return fut.chain(new CX1<IgniteInternalFuture<Map<Object, Object>>, V>() {
                        @Override public V applyx(IgniteInternalFuture<Map<Object, Object>> e)
                            throws IgniteCheckedException {
                            Map<Object, Object> map = e.get();

                            assert map.isEmpty() || map.size() == 1 : map.size();

                            if (skipVals) {
                                Boolean val = map.isEmpty() ? false : (Boolean)F.firstValue(map);

                                return (V)(val);
                            }

                            return (V)F.firstValue(map);
                        }
                    });
                }
            }, opCtx, /*retry*/false);
        }

        AffinityTopologyVersion topVer = tx == null ? ctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        MvccSnapshot mvccSnapshot = null;
        MvccQueryTracker mvccTracker = null;

        if (ctx.mvccEnabled()) {
            try {
                if (tx != null)
                    mvccSnapshot = MvccUtils.requestSnapshot(ctx, tx);
                else {
                    mvccTracker = MvccUtils.mvccTracker(ctx, null);

                    mvccSnapshot = mvccTracker.snapshot();
                }

                assert mvccSnapshot != null;
            }
            catch (IgniteCheckedException ex) {
                return new GridFinishedFuture<>(ex);
            }
        }

        GridPartitionedSingleGetFuture fut = new GridPartitionedSingleGetFuture(ctx,
            ctx.toCacheKeyObject(key),
            topVer,
            opCtx == null || !opCtx.skipStore(),
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            skipVals ? null : expiryPolicy(opCtx != null ? opCtx.expiry() : null),
            skipVals,
            needVer,
            /*keepCacheObjects*/false,
            opCtx != null && opCtx.recovery(),
            null,
            mvccSnapshot);

        fut.init();

        if(mvccTracker != null){
            final MvccQueryTracker mvccTracker0 = mvccTracker;

            fut.listen(new CI1<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> future) {
                    if(future.isDone())
                        mvccTracker0.onDone();
                }
            });
        }

        return (IgniteInternalFuture<V>)fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        String taskName,
        final boolean deserializeBinary,
        final boolean recovery,
        final boolean skipVals,
        final boolean needVer
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        GridNearTxLocal tx = checkCurrentTx();

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        if (!ctx.mvccEnabled() && tx != null && !tx.implicit() && !skipTx) {
            return asyncOp(tx, new AsyncOp<Map<K, V>>(keys) {
                /** {@inheritDoc} */
                @Override public IgniteInternalFuture<Map<K, V>> op(GridNearTxLocal tx,
                    AffinityTopologyVersion readyTopVer) {
                    return tx.getAllAsync(ctx,
                        readyTopVer,
                        ctx.cacheKeysView(keys),
                        deserializeBinary,
                        skipVals,
                        false,
                        opCtx != null && opCtx.skipStore(),
                        recovery,
                        needVer);
                }
            }, opCtx, /*retry*/false);
        }

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        MvccSnapshot mvccSnapshot = null;
        MvccQueryTracker mvccTracker = null;

        if (ctx.mvccEnabled()) {
            try {
                if (tx != null)
                    mvccSnapshot = MvccUtils.requestSnapshot(ctx, tx);
                else {
                    mvccTracker = MvccUtils.mvccTracker(ctx, null);

                    mvccSnapshot = mvccTracker.snapshot();
                }

                assert mvccSnapshot != null;
            }
            catch (IgniteCheckedException ex) {
                return new GridFinishedFuture<>(ex);
            }
        }

        AffinityTopologyVersion topVer = tx == null ? ctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

        IgniteInternalFuture<Map<K, V>> fut = loadAsync(
            ctx.cacheKeysView(keys),
            opCtx == null || !opCtx.skipStore(),
            forcePrimary ,
            topVer,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            skipVals ? null : expiryPolicy(opCtx != null ? opCtx.expiry() : null),
            skipVals,
            needVer,
            false,
            null,
            mvccSnapshot
        );

        if(mvccTracker != null){
            final MvccQueryTracker mvccTracker0 = mvccTracker;

            fut.listen(new CI1<IgniteInternalFuture<Map<K, V>>>() {
                /** {@inheritDoc} */
                @Override public void apply(IgniteInternalFuture<Map<K, V>> future) {
                    if(future.isDone())
                        mvccTracker0.onDone();
                }
            });
        }

        return fut;
    }

    /**
     * @param keys Keys to load.
     * @param readThrough Read through flag.
     * @param forcePrimary Force get from primary node flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer Need version.
     * @return Loaded values.
     */
    private IgniteInternalFuture<Map<K, V>> loadAsync(
        @Nullable Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean forcePrimary,
        AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        @Nullable String txLbl) {
        return loadAsync(keys,
            readThrough,
            forcePrimary,
            topVer, subjId,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            skipVals,
            needVer,
            false,
            txLbl,
            null);
    }

    /**
     * @param key Key to load.
     * @param readThrough Read through flag.
     * @param forcePrimary Force get from primary node flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObj Keep cache objects flag.
     * @param txLbl Transaction label.
     * @return Load future.
     */
    public final IgniteInternalFuture<Object> loadAsync(
        KeyCacheObject key,
        boolean readThrough,
        boolean forcePrimary,
        AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        boolean keepCacheObj,
        boolean recovery,
        @Nullable MvccSnapshot mvccSnapshot,
        @Nullable String txLbl
    ) {
        GridPartitionedSingleGetFuture fut = new GridPartitionedSingleGetFuture(ctx,
            ctx.toCacheKeyObject(key),
            topVer,
            readThrough,
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            expiryPlc,
            skipVals,
            needVer,
            keepCacheObj,
            recovery,
            txLbl,
            mvccSnapshot);

        fut.init();

        return fut;
    }

    /**
     * @param keys Keys to load.
     * @param readThrough Read through flag.
     * @param forcePrimary Force get from primary node flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObj Keep cache objects flag.
     * @param txLbl Transaction label.
     * @param mvccSnapshot Mvcc snapshot.
     * @return Load future.
     */
    public final IgniteInternalFuture<Map<K, V>> loadAsync(
        @Nullable Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean forcePrimary,
        AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        boolean keepCacheObj,
        @Nullable String txLbl,
        @Nullable MvccSnapshot mvccSnapshot
    ) {
        assert mvccSnapshot == null || ctx.mvccEnabled();

        if (keys == null || keys.isEmpty())
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (expiryPlc == null)
            expiryPlc = expiryPolicy(null);

        // Optimization: try to resolve value locally and escape 'get future' creation. Not applcable for MVCC,
        // because local node may contain a visible version which is no the most recent one.
        if (!ctx.mvccEnabled() && !forcePrimary && ctx.affinityNode()) {
            try {
                Map<K, V> locVals = null;

                boolean success = true;
                boolean readNoEntry = ctx.readNoEntry(expiryPlc, false);
                boolean evt = !skipVals;

                for (KeyCacheObject key : keys) {
                    if (readNoEntry) {
                        CacheDataRow row = mvccSnapshot != null ?
                            ctx.offheap().mvccRead(ctx, key, mvccSnapshot) :
                            ctx.offheap().read(ctx, key);

                        if (row != null) {
                            long expireTime = row.expireTime();

                            if (expireTime == 0 || expireTime > U.currentTimeMillis()) {
                                if (locVals == null)
                                    locVals = U.newHashMap(keys.size());

                                ctx.addResult(locVals,
                                    key,
                                    row.value(),
                                    skipVals,
                                    keepCacheObj,
                                    deserializeBinary,
                                    true,
                                    null,
                                    row.version(),
                                    0,
                                    0,
                                    needVer);

                                if (evt) {
                                    ctx.events().readEvent(key,
                                        null,
                                        txLbl,
                                        row.value(),
                                        subjId,
                                        taskName,
                                        !deserializeBinary);
                                }
                            }
                            else
                                success = false;
                        }
                        else
                            success = false;
                    }
                    else {
                        GridCacheEntryEx entry = null;

                        while (true) {
                            try {
                                entry = entryEx(key);

                                // If our DHT cache do has value, then we peek it.
                                if (entry != null) {
                                    boolean isNew = entry.isNewLocked();

                                    EntryGetResult getRes = null;
                                    CacheObject v = null;
                                    GridCacheVersion ver = null;

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
                                            mvccSnapshot,
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
                                            !deserializeBinary,
                                            mvccSnapshot);
                                    }

                                    // Entry was not in memory or in swap, so we remove it from cache.
                                    if (v == null) {
                                        GridCacheVersion obsoleteVer = context().versions().next();

                                        if (isNew && entry.markObsoleteIfEmpty(obsoleteVer))
                                            removeEntry(entry);

                                        success = false;
                                    }
                                    else {
                                        if (locVals == null)
                                            locVals = U.newHashMap(keys.size());

                                        ctx.addResult(locVals,
                                            key,
                                            v,
                                            skipVals,
                                            keepCacheObj,
                                            deserializeBinary,
                                            true,
                                            getRes,
                                            ver,
                                            0,
                                            0,
                                            needVer);
                                    }
                                }
                                else
                                    success = false;

                                break; // While.
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                // No-op, retry.
                            }
                            catch (GridDhtInvalidPartitionException ignored) {
                                success = false;

                                break; // While.
                            }
                            finally {
                                if (entry != null)
                                    entry.touch(topVer);
                            }
                        }
                    }

                    if (!success)
                        break;
                    else if (!skipVals && ctx.statisticsEnabled())
                        ctx.cache().metrics0().onRead(true);
                }

                if (success) {
                    sendTtlUpdateRequest(expiryPlc);

                    return new GridFinishedFuture<>(locVals);
                }
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }

        if (expiryPlc != null)
            expiryPlc.reset();

        // Either reload or not all values are available locally.
        GridPartitionedGetFuture<K, V> fut = new GridPartitionedGetFuture<>(
            ctx,
            keys,
            readThrough,
            forcePrimary,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            skipVals,
            needVer,
            keepCacheObj,
            txLbl,
            mvccSnapshot);

        fut.init(topVer);

        return fut;
    }

    /**
     * This is an entry point to pessimistic locking within transaction.
     *
     * {@inheritDoc}
     */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(
        Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable TransactionIsolation isolation,
        long createTtl,
        long accessTtl
    ) {
        assert tx == null || tx instanceof GridNearTxLocal : tx;

        GridNearTxLocal txx = (GridNearTxLocal)tx;

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        GridDhtColocatedLockFuture fut = new GridDhtColocatedLockFuture(ctx,
            keys,
            txx,
            isRead,
            retval,
            timeout,
            createTtl,
            accessTtl,
            CU.empty0(),
            opCtx != null && opCtx.skipStore(),
            opCtx != null && opCtx.isKeepBinary(),
            opCtx != null && opCtx.recovery());

        // Future will be added to mvcc only if it was mapped to remote nodes.
        fut.map();

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridNearTransactionalCache<K, V> near() {
        assert false : "Near cache is not available in colocated mode.";

        return null;
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys) {
        if (keys.isEmpty())
            return;

        try {
            GridCacheVersion ver = null;

            int keyCnt = -1;

            Map<ClusterNode, GridNearUnlockRequest> map = null;

            Collection<KeyCacheObject> locKeys = new ArrayList<>();

            for (K key : keys) {
                KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);
                IgniteTxKey txKey = ctx.txKey(cacheKey);

                GridDistributedCacheEntry entry = peekExx(cacheKey);

                GridCacheMvccCandidate lock =
                    ctx.mvcc().removeExplicitLock(Thread.currentThread().getId(), txKey, null);

                if (lock != null) {
                    final AffinityTopologyVersion topVer = lock.topologyVersion();

                    assert topVer.compareTo(AffinityTopologyVersion.ZERO) > 0;

                    // Send request to remove from remote nodes.
                    ClusterNode primary = ctx.affinity().primaryByKey(key, topVer);

                    if (primary == null) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to unlock keys (all partition nodes left the grid).");

                        continue;
                    }

                    if (map == null) {
                        Collection<ClusterNode> affNodes = CU.affinityNodes(ctx, topVer);

                        keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                        map = U.newHashMap(affNodes.size());
                    }

                    if (ver == null)
                        ver = lock.version();

                    if (!lock.reentry()) {
                        if (!ver.equals(lock.version()))
                            throw new IgniteCheckedException("Failed to unlock (if keys were locked separately, " +
                                "then they need to be unlocked separately): " + keys);

                        if (!primary.isLocal()) {
                            GridNearUnlockRequest req = map.get(primary);

                            if (req == null) {
                                map.put(primary, req = new GridNearUnlockRequest(ctx.cacheId(), keyCnt,
                                    ctx.deploymentEnabled()));

                                req.version(ver);
                            }

                            KeyCacheObject key0 = entry != null ? entry.key() : cacheKey;

                            req.addKey(key0, ctx);
                        }
                        else
                            locKeys.add(cacheKey);

                        if (log.isDebugEnabled())
                            log.debug("Removed lock (will distribute): " + lock);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Current thread still owns lock (or there are no other nodes)" +
                            " [lock=" + lock + ", curThreadId=" + Thread.currentThread().getId() + ']');
                }
            }

            if (ver == null)
                return;

            if (!locKeys.isEmpty())
                removeLocks(ctx.localNodeId(), ver, locKeys, true);

            for (Map.Entry<ClusterNode, GridNearUnlockRequest> mapping : map.entrySet()) {
                ClusterNode n = mapping.getKey();

                GridDistributedUnlockRequest req = mapping.getValue();

                assert !n.isLocal();

                if (!F.isEmpty(req.keys())) {
                    try {
                        // We don't wait for reply to this message.
                        ctx.io().send(n, req, ctx.ioPolicy());
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send unlock request (node has left the grid) [keys=" + req.keys() +
                                ", n=" + n + ", e=" + e + ']');
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send unlock request [keys=" + req.keys() + ", n=" + n + ']', e);
                    }
                }
            }
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /**
     * Removes locks regardless of whether they are owned or not for given
     * version and keys.
     *
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param keys Keys.
     */
    public void removeLocks(long threadId, GridCacheVersion ver, Collection<KeyCacheObject> keys) {
        if (keys.isEmpty())
            return;

        try {
            int keyCnt = -1;

            Map<ClusterNode, GridNearUnlockRequest> map = null;

            Collection<KeyCacheObject> locKeys = new LinkedList<>();

            for (KeyCacheObject key : keys) {
                IgniteTxKey txKey = ctx.txKey(key);

                GridCacheMvccCandidate lock = ctx.mvcc().removeExplicitLock(threadId, txKey, ver);

                if (lock != null) {
                    AffinityTopologyVersion topVer = lock.topologyVersion();

                    if (map == null) {
                        Collection<ClusterNode> affNodes = CU.affinityNodes(ctx, topVer);

                        keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                        map = U.newHashMap(affNodes.size());
                    }

                    ClusterNode primary = ctx.affinity().primaryByKey(key, topVer);

                    if (primary == null) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to remove locks (all partition nodes left the grid).");

                        continue;
                    }

                    if (!primary.isLocal()) {
                        // Send request to remove from remote nodes.
                        GridNearUnlockRequest req = map.get(primary);

                        if (req == null) {
                            map.put(primary, req = new GridNearUnlockRequest(ctx.cacheId(), keyCnt,
                                ctx.deploymentEnabled()));

                            req.version(ver);
                        }

                        GridCacheEntryEx entry = peekEx(key);

                        KeyCacheObject key0 = entry != null ? entry.key() : key;

                        req.addKey(key0, ctx);
                    }
                    else
                        locKeys.add(key);
                }
            }

            if (!locKeys.isEmpty())
                removeLocks(ctx.localNodeId(), ver, locKeys, true);

            if (map == null || map.isEmpty())
                return;

            IgnitePair<Collection<GridCacheVersion>> versPair = ctx.tm().versions(ver);

            Collection<GridCacheVersion> committed = versPair.get1();
            Collection<GridCacheVersion> rolledback = versPair.get2();

            for (Map.Entry<ClusterNode, GridNearUnlockRequest> mapping : map.entrySet()) {
                ClusterNode n = mapping.getKey();

                GridDistributedUnlockRequest req = mapping.getValue();

                if (!F.isEmpty(req.keys())) {
                    req.completedVersions(committed, rolledback);

                    try {
                        // We don't wait for reply to this message.
                        ctx.io().send(n, req, ctx.ioPolicy());
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send unlock request (node has left the grid) [keys=" + req.keys() +
                                ", n=" + n + ", e=" + e + ']');
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send unlock request [keys=" + req.keys() + ", n=" + n + ']', e);
                    }
                }
            }
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param tx Started colocated transaction (if any).
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param topVer Topology version.
     * @param keys Mapped keys.
     * @param txRead Tx read.
     * @param retval Return value flag.
     * @param timeout Lock timeout.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @param filter filter Optional filter.
     * @param skipStore Skip store flag.
     * @return Lock future.
     */
    IgniteInternalFuture<Exception> lockAllAsync(
        final GridCacheContext<?, ?> cacheCtx,
        @Nullable final GridNearTxLocal tx,
        final long threadId,
        final GridCacheVersion ver,
        final AffinityTopologyVersion topVer,
        final Collection<KeyCacheObject> keys,
        final boolean txRead,
        final boolean retval,
        final long timeout,
        final long createTtl,
        final long accessTtl,
        @Nullable final CacheEntryPredicate[] filter,
        final boolean skipStore,
        final boolean keepBinary
    ) {
        assert keys != null;

        IgniteInternalFuture<Object> keyFut = ctx.group().preloader().request(cacheCtx, keys, topVer);

        // Prevent embedded future creation if possible.
        if (keyFut == null || keyFut.isDone()) {
            // Check for exception.
            if (keyFut != null && keyFut.error() != null)
                return new GridFinishedFuture<>(keyFut.error());

            return lockAllAsync0(cacheCtx,
                tx,
                threadId,
                ver,
                topVer,
                keys,
                txRead,
                retval,
                timeout,
                createTtl,
                accessTtl,
                filter,
                skipStore,
                keepBinary);
        }
        else {
            return new GridEmbeddedFuture<>(keyFut,
                new C2<Object, Exception, IgniteInternalFuture<Exception>>() {
                    @Override public IgniteInternalFuture<Exception> apply(Object o, Exception exx) {
                        if (exx != null)
                            return new GridDhtFinishedFuture<>(exx);

                        return lockAllAsync0(cacheCtx,
                            tx,
                            threadId,
                            ver,
                            topVer,
                            keys,
                            txRead,
                            retval,
                            timeout,
                            createTtl,
                            accessTtl,
                            filter,
                            skipStore,
                            keepBinary);
                    }
                }
            );
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param tx Started colocated transaction (if any).
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param topVer Topology version.
     * @param keys Mapped keys.
     * @param txRead Tx read.
     * @param retval Return value flag.
     * @param timeout Lock timeout.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @param filter filter Optional filter.
     * @param skipStore Skip store flag.
     * @return Lock future.
     */
    private IgniteInternalFuture<Exception> lockAllAsync0(
        GridCacheContext<?, ?> cacheCtx,
        @Nullable final GridNearTxLocal tx,
        long threadId,
        final GridCacheVersion ver,
        AffinityTopologyVersion topVer,
        final Collection<KeyCacheObject> keys,
        final boolean txRead,
        boolean retval,
        final long timeout,
        final long createTtl,
        final long accessTtl,
        @Nullable final CacheEntryPredicate[] filter,
        boolean skipStore,
        boolean keepBinary) {
        int cnt = keys.size();

        if (tx == null) {
            GridDhtLockFuture fut = new GridDhtLockFuture(ctx,
                ctx.localNodeId(),
                ver,
                topVer,
                cnt,
                txRead,
                retval,
                timeout,
                tx,
                threadId,
                createTtl,
                accessTtl,
                filter,
                skipStore,
                keepBinary);

            // Add before mapping.
            if (!ctx.mvcc().addFuture(fut))
                throw new IllegalStateException("Duplicate future ID: " + fut);

            boolean timedout = false;

            for (KeyCacheObject key : keys) {
                if (timedout)
                    break;

                while (true) {
                    GridDhtCacheEntry entry = entryExx(key, topVer);

                    try {
                        fut.addEntry(key == null ? null : entry);

                        if (fut.isDone())
                            timedout = true;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry when adding lock (will retry): " + entry);
                    }
                    catch (GridDistributedLockCancelledException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to add entry [err=" + e + ", entry=" + entry + ']');

                        fut.onError(e);

                        return new GridDhtFinishedFuture<>(e);
                    }
                }
            }

            // This will send remote messages.
            fut.map();

            return new GridDhtEmbeddedFuture<>(
                new C2<Boolean, Exception, Exception>() {
                    @Override public Exception apply(Boolean b, Exception e) {
                        if (e != null)
                            e = U.unwrap(e);
                        else if (!b)
                            e = new GridCacheLockTimeoutException(ver);

                        return e;
                    }
                },
                fut);
        }
        else {
            // Handle implicit locks for pessimistic transactions.
            ctx.tm().txContext(tx);

            if (log.isDebugEnabled())
                log.debug("Performing colocated lock [tx=" + tx + ", keys=" + keys + ']');

            IgniteInternalFuture<GridCacheReturn> txFut = tx.lockAllAsync(cacheCtx,
                keys,
                retval,
                txRead,
                createTtl,
                accessTtl,
                skipStore,
                keepBinary);

            return new GridDhtEmbeddedFuture<>(
                new C2<GridCacheReturn, Exception, Exception>() {
                    @Override public Exception apply(GridCacheReturn ret,
                        Exception e) {
                        if (e != null)
                            e = U.unwrap(e);

                        assert !tx.empty();

                        return e;
                    }
                },
                txFut);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processLockResponse(UUID nodeId, GridNearLockResponse res) {
        if (txLockMsgLog.isDebugEnabled())
            txLockMsgLog.debug("Received near lock response [txId=" + res.version() + ", node=" + nodeId + ']');

        assert nodeId != null;
        assert res != null;

        GridDhtColocatedLockFuture fut = (GridDhtColocatedLockFuture)ctx.mvcc().
            <Boolean>versionedFuture(res.version(), res.futureId());

        if (fut != null)
            fut.onResult(nodeId, res);
        else {
            if (txLockMsgLog.isDebugEnabled()) {
                txLockMsgLog.debug("Received near lock response for unknown future [txId=" + res.version() +
                    ", node=" + nodeId +
                    ", res=" + res + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtColocatedCache.class, this, super.toString());
    }
}

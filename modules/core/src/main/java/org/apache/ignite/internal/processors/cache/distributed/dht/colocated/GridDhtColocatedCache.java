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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

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
                    return new GridDhtColocatedOffHeapCacheEntry(ctx, topVer, key, hash, val, next, hdrId);

                return new GridDhtColocatedCacheEntry(ctx, topVer, key, hash, val, next, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.io().addHandler(ctx.cacheId(), GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse>() {
            @Override public void apply(UUID nodeId, GridNearGetResponse res) {
                processGetResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridNearLockResponse.class, new CI2<UUID, GridNearLockResponse>() {
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
        return allowDetached && !ctx.affinity().primary(ctx.localNode(), key, topVer) ?
            createEntry(key) : entryExx(key, topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        return ctx.mvcc().isLockedByThread(cacheKey, -1);
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        return ctx.mvcc().isLockedByThread(cacheKey, Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx entry,
        @Nullable UUID subjId,
        String taskName,
        final boolean deserializePortable,
        final boolean skipVals,
        boolean canRemap
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        IgniteTxLocalAdapter tx = ctx.tm().threadLocalTx(ctx);

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        if (tx != null && !tx.implicit() && !skipTx) {
            return asyncOp(tx, new AsyncOp<Map<K, V>>(keys) {
                @Override public IgniteInternalFuture<Map<K, V>> op(IgniteTxLocalAdapter tx) {
                    return tx.getAllAsync(ctx,
                        ctx.cacheKeysView(keys),
                        entry,
                        deserializePortable,
                        skipVals,
                        false,
                        opCtx != null && opCtx.skipStore());
                }
            });
        }

        AffinityTopologyVersion topVer = tx == null ?
            (canRemap ? ctx.affinity().affinityTopologyVersion() : ctx.shared().exchange().readyAffinityVersion()) :
            tx.topologyVersion();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        return loadAsync(
            ctx.cacheKeysView(keys),
            opCtx == null || !opCtx.skipStore(),
            false,
            forcePrimary,
            topVer,
            subjId,
            taskName,
            deserializePortable,
            skipVals ? null : expiryPolicy(opCtx != null ? opCtx.expiry() : null),
            skipVals,
            canRemap);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx entryExSafe(
        KeyCacheObject key,
        AffinityTopologyVersion topVer
    ) {
        try {
            return ctx.affinity().localNode(key, topVer) ? entryEx(key) : null;
        }
        catch (GridDhtInvalidPartitionException ignored) {
            return null;
        }
    }

    /**
     * @param keys Keys to load.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param forcePrimary Force get from primary node flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @return Loaded values.
     */
    public IgniteInternalFuture<Map<K, V>> loadAsync(
        @Nullable Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean reload,
        boolean forcePrimary,
        AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean canRemap
    ) {
        if (keys == null || keys.isEmpty())
            return new GridFinishedFuture<>(Collections.<K, V>emptyMap());

        if (expiryPlc == null)
            expiryPlc = expiryPolicy(null);

        // Optimisation: try to resolve value locally and escape 'get future' creation.
        if (!reload && !forcePrimary) {
            Map<K, V> locVals = U.newHashMap(keys.size());

            boolean success = true;

            // Optimistically expect that all keys are available locally (avoid creation of get future).
            for (KeyCacheObject key : keys) {
                GridCacheEntryEx entry = null;

                while (true) {
                    try {
                        entry = ctx.isSwapOrOffheapEnabled() ? entryEx(key) : peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            CacheObject v = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /**update-metrics*/false,
                                /*event*/!skipVals,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                expiryPlc);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                GridCacheVersion obsoleteVer = context().versions().next();

                                if (isNew && entry.markObsoleteIfEmpty(obsoleteVer))
                                    removeIfObsolete(key);

                                success = false;
                            }
                            else
                                ctx.addResult(locVals, key, v, skipVals, false, deserializePortable, true);
                        }
                        else
                            success = false;

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // No-op, retry.
                    }
                    catch (GridCacheFilterFailedException ignored) {
                        // No-op, skip the key.
                        break;
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        success = false;

                        break; // While.
                    }
                    catch (IgniteCheckedException e) {
                        return new GridFinishedFuture<>(e);
                    }
                    finally {
                        if (entry != null)
                            context().evicts().touch(entry, topVer);
                    }
                }

                if (!success)
                    break;
                else if (!skipVals && ctx.config().isStatisticsEnabled())
                    ctx.cache().metrics0().onRead(true);
            }

            if (success) {
                sendTtlUpdateRequest(expiryPlc);

                return new GridFinishedFuture<>(locVals);
            }
        }

        if (expiryPlc != null)
            expiryPlc.reset();

        // Either reload or not all values are available locally.
        GridPartitionedGetFuture<K, V> fut = new GridPartitionedGetFuture<>(
            ctx,
            keys,
            topVer,
            readThrough,
            reload,
            forcePrimary,
            subjId,
            taskName,
            deserializePortable,
            expiryPlc,
            skipVals,
            canRemap);

        fut.init();

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
            accessTtl,
            CU.empty0(),
            opCtx != null && opCtx.skipStore());

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

                GridDistributedCacheEntry entry = peekExx(cacheKey);

                GridCacheMvccCandidate lock =
                    ctx.mvcc().removeExplicitLock(Thread.currentThread().getId(), cacheKey, null);

                if (lock != null) {
                    final AffinityTopologyVersion topVer = lock.topologyVersion();

                    assert topVer.compareTo(AffinityTopologyVersion.ZERO) > 0;

                    // Send request to remove from remote nodes.
                    ClusterNode primary = ctx.affinity().primary(key, topVer);

                    if (primary == null) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to unlock keys (all partition nodes left the grid).");

                        continue;
                    }

                    if (map == null) {
                        Collection<ClusterNode> affNodes = CU.allNodes(ctx, topVer);

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
                                map.put(primary, req = new GridNearUnlockRequest(ctx.cacheId(), keyCnt));

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
                GridCacheMvccCandidate lock = ctx.mvcc().removeExplicitLock(threadId, key, ver);

                if (lock != null) {
                    AffinityTopologyVersion topVer = lock.topologyVersion();

                    if (map == null) {
                        Collection<ClusterNode> affNodes = CU.allNodes(ctx, topVer);

                        keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                        map = U.newHashMap(affNodes.size());
                    }

                    ClusterNode primary = ctx.affinity().primary(key, topVer);

                    if (primary == null) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to remove locks (all partition nodes left the grid).");

                        continue;
                    }

                    if (!primary.isLocal()) {
                        // Send request to remove from remote nodes.
                        GridNearUnlockRequest req = map.get(primary);

                        if (req == null) {
                            map.put(primary, req = new GridNearUnlockRequest(ctx.cacheId(), keyCnt));

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

            Collection<GridCacheVersion> committed = ctx.tm().committedVersions(ver);
            Collection<GridCacheVersion> rolledback = ctx.tm().rolledbackVersions(ver);

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
        final long accessTtl,
        @Nullable final CacheEntryPredicate[] filter,
        final boolean skipStore
    ) {
        assert keys != null;

        IgniteInternalFuture<Object> keyFut = ctx.dht().dhtPreloader().request(keys, topVer);

        // Prevent embedded future creation if possible.
        if (keyFut.isDone()) {
            try {
                // Check for exception.
                keyFut.get();

                return lockAllAsync0(cacheCtx,
                    tx,
                    threadId,
                    ver,
                    topVer,
                    keys,
                    txRead,
                    retval,
                    timeout,
                    accessTtl,
                    filter,
                    skipStore);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
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
                            accessTtl,
                            filter,
                            skipStore);
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
        final long accessTtl,
        @Nullable final CacheEntryPredicate[] filter,
        boolean skipStore) {
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
                accessTtl,
                filter,
                skipStore);

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
                            log.debug("Got lock request for cancelled lock (will ignore): " +
                                entry);

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
                tx.implicit(),
                txRead,
                accessTtl,
                skipStore);

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
     * @param nodeId Sender ID.
     * @param res Response.
     */
    private void processGetResponse(UUID nodeId, GridNearGetResponse res) {
        GridPartitionedGetFuture<K, V> fut = (GridPartitionedGetFuture<K, V>)ctx.mvcc().<Map<K, V>>future(
            res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processLockResponse(UUID nodeId, GridNearLockResponse res) {
        assert nodeId != null;
        assert res != null;

        GridDhtColocatedLockFuture fut = (GridDhtColocatedLockFuture)ctx.mvcc().
            <Boolean>future(res.version(), res.futureId());

        if (fut != null)
            fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtColocatedCache.class, this, super.toString());
    }
}

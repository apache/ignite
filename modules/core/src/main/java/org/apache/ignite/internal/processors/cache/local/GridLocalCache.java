/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.local;

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.EntryGetWithTtlResult;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheLocalConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.GridCachePreloaderAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Local cache implementation.
 */
public class GridLocalCache<K, V> extends GridCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCachePreloader preldr;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridLocalCache() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     */
    public GridLocalCache(GridCacheContext<K, V> ctx) {
        super(ctx);

        preldr = new GridCachePreloaderAdapter(ctx.group());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (map == null)
            map = new GridCacheLocalConcurrentMap(ctx, entryFactory(), DFLT_START_CACHE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader preloader() {
        return preldr;
    }

    /**
     * @return Entry factory.
     */
    private GridCacheMapEntryFactory entryFactory() {
        return new GridCacheMapEntryFactory() {
            @Override public GridCacheMapEntry create(
                GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key
            ) {
                return new GridLocalCacheEntry(ctx, key);
            }
        };
    }

    /**
     * @param key Key of entry.
     * @return Cache entry.
     */
    @Nullable private GridLocalCacheEntry peekExx(KeyCacheObject key) {
        return (GridLocalCacheEntry)peekEx(key);
    }

    /**
     * @param key Key of entry.
     * @return Cache entry.
     */
    GridLocalCacheEntry entryExx(KeyCacheObject key) {
        return (GridLocalCacheEntry)entryEx(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> txLockAsync(Collection<KeyCacheObject> keys,
        long timeout,
        IgniteTxLocalEx tx,
        boolean isRead,
        boolean retval,
        TransactionIsolation isolation,
        boolean invalidate,
        long createTtl,
        long accessTtl) {
        return lockAllAsync(keys, timeout, tx, CU.empty0());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout) {
        IgniteTxLocalEx tx = ctx.tm().localTx();

        return lockAllAsync(ctx.cacheKeysView(keys), timeout, tx, CU.empty0());
    }

    /**
     * @param keys Keys.
     * @param timeout Timeout.
     * @param tx Transaction.
     * @param filter Filter.
     * @return Future.
     */
    public IgniteInternalFuture<Boolean> lockAllAsync(Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        CacheEntryPredicate[] filter) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(true);

        GridLocalLockFuture<K, V> fut = new GridLocalLockFuture<>(ctx, keys, tx, this, timeout, filter);

        try {
            if (!fut.addEntries(keys))
                return fut;

            if (!ctx.mvcc().addFuture(fut))
                fut.onError(new IgniteCheckedException("Duplicate future ID (internal error): " + fut));

            // Must have future added prior to checking locks.
            fut.checkLocks();

            return fut;
        }
        catch (IgniteCheckedException e) {
            fut.onError(e);

            return fut;
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(
        Collection<? extends K> keys
    ) throws IgniteCheckedException {
        for (K key : keys) {
            GridLocalCacheEntry entry = peekExx(ctx.toCacheKeyObject(key));

            if (entry != null && ctx.isAll(entry, CU.empty0())) {
                entry.releaseLocal();

                entry.touch();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return ctx.closures().callLocalSafe(new GridPlainCallable<Void>() {
            @Override public Void call() throws Exception {
                removeAll();

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }

    /**
     * @param fut Clears future from cache.
     */
    void onFutureDone(GridLocalLockFuture fut) {
        if (ctx.mvcc().removeVersionedFuture(fut)) {
            if (log().isDebugEnabled())
                log().debug("Explicitly removed future from map of futures: " + fut);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        PeekModes modes = parsePeekModes(peekModes, true);

        modes.primary = true;
        modes.backup = true;

        if (modes.offheap)
            return ctx.offheap().cacheEntriesCount(ctx.cacheId());
        else if (modes.heap)
            return size();
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int part, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return localSizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int part) throws IgniteCheckedException {
        ctx.offheap().preloadPartition(part);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> preloadPartitionAsync(int part) throws IgniteCheckedException {
        return ctx.closures().callLocalSafe(new GridPlainCallable<Void>() {
            @Override public Void call() throws Exception {
                preloadPartition(part);

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int part) throws IgniteCheckedException {
        ctx.offheap().preloadPartition(part);

        return true;
    }

    /**
     * @param keys Keys.
     * @param forcePrimary Force primary.
     * @param skipTx Skip tx.
     * @param subjId Subj Id.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param recovery Recovery mode flag.
     * @param skipVals Skip values.
     * @param needVer Need version.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    @Override protected IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        boolean skipVals,
        final boolean needVer
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        boolean readThrough = opCtx == null || !opCtx.skipStore();
        boolean checkTx = !skipTx;

        @Nullable IgniteCacheExpiryPolicy expiry = skipVals ? null : expiryPolicy(opCtx != null ? opCtx.expiry() : null);

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (keyCheck)
            validateCacheKeys(keys);

        return getAllAsync0(ctx.cacheKeysView(keys),
            readThrough,
            checkTx,
            subjId,
            taskName,
            deserializeBinary,
            expiry,
            skipVals,
            opCtx != null && opCtx.recovery(),
            needVer); // TODO IGNITE-7371
    }


    /**
     * @param keys Keys.
     * @param readThrough Read-through flag.
     * @param checkTx Check local transaction flag.
     * @param subjId Subject ID.
     * @param taskName Task name/
     * @param deserializeBinary Deserialize binary flag.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @return Future.
     */
    protected final <K1, V1> IgniteInternalFuture<Map<K1, V1>> getAllAsync0(
        @Nullable final Collection<KeyCacheObject> keys,
        final boolean readThrough,
        boolean checkTx,
        @Nullable final UUID subjId,
        final String taskName,
        final boolean deserializeBinary,
        @Nullable final IgniteCacheExpiryPolicy expiry,
        final boolean skipVals,
        final boolean recovery,
        final boolean needVer
    ) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.emptyMap());

        GridNearTxLocal tx = null;

        if (checkTx) {
            try {
                checkJta();
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }

            tx = checkCurrentTx();
        }

        if (tx == null || tx.implicit()) {
            Map<KeyCacheObject, EntryGetResult> misses = null;

            Set<GridCacheEntryEx> newLocalEntries = null;

            try {
                int keysSize = keys.size();

                GridDhtTopologyFuture topFut = ctx.shared().exchange().lastFinishedFuture();

                Throwable ex = topFut != null ? topFut.validateCache(ctx, recovery, /*read*/true, null, keys) : null;

                if (ex != null)
                    return new GridFinishedFuture<>(ex);

                final Map<K1, V1> map = keysSize == 1 ?
                    (Map<K1, V1>)new IgniteBiTuple<>() :
                    U.newHashMap(keysSize);

                final boolean storeEnabled = !skipVals && readThrough && ctx.readThrough();

                boolean readNoEntry = ctx.readNoEntry(expiry, false);

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
                                            null,
                                            row.value(),
                                            subjId,
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
                                boolean isNewLocalEntry = this.map.getEntry(ctx, key) == null;

                                entry = entryEx(key);

                                if (entry == null) {
                                    if (!skipVals && ctx.statisticsEnabled())
                                        ctx.cache().metrics0().onRead(false);

                                    break;
                                }

                                if (isNewLocalEntry) {
                                    if (newLocalEntries == null)
                                        newLocalEntries = new HashSet<>();

                                    newLocalEntries.add(entry);
                                }

                                if (storeEnabled) {
                                    res = entry.innerGetAndReserveForLoad(updateMetrics,
                                        evt,
                                        subjId,
                                        taskName,
                                        expiry,
                                        !deserializeBinary,
                                        null);

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
                                        subjId,
                                        null,
                                        taskName,
                                        expiry,
                                        !deserializeBinary,
                                        null);

                                    if (res == null)
                                        entry.touch();
                                }
                            }

                            if (res != null) {
                                ctx.addResult(map,
                                    key,
                                    res,
                                    skipVals,
                                    false,
                                    deserializeBinary,
                                    true,
                                    needVer);

                                if (entry != null && (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED)))
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

                    final IgniteTxLocalAdapter tx0 = tx;

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

                                                GridCacheVersion newVer = ctx.versions().next();

                                                EntryGetResult verVal = entry.versionedValue(
                                                    cacheVal,
                                                    res.version(),
                                                    newVer,
                                                    expiry,
                                                    null);

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
                                                        false,
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
                                                        false,
                                                        deserializeBinary,
                                                        false,
                                                        needVer
                                                    );
                                                }

                                                if (tx0 == null || (!tx0.implicit() &&
                                                    tx0.isolation() == READ_COMMITTED))
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

                                clearReservationsIfNeeded(loadKeys, loaded, tx0);

                                return map;
                            }
                        }), true),
                        new C2<Map<K, V>, Exception, IgniteInternalFuture<Map<K, V>>>() {
                            @Override public IgniteInternalFuture<Map<K, V>> apply(Map<K, V> map, Exception e) {
                                if (e != null) {
                                    clearReservationsIfNeeded(loadKeys, loaded, tx0);

                                    return new GridFinishedFuture<>(e);
                                }

                                if (tx0 == null || (!tx0.implicit() && tx0.isolation() == READ_COMMITTED)) {
                                    Collection<KeyCacheObject> notFound = new HashSet<>(loadKeys.keySet());

                                    notFound.removeAll(loaded);

                                    // Touch entries that were not found in store.
                                    for (KeyCacheObject key : notFound) {
                                        GridCacheEntryEx entry = peekEx(key);

                                        if (entry != null)
                                            entry.touch();
                                    }
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

                if (newLocalEntries != null) {
                    for (GridCacheEntryEx entry : newLocalEntries)
                        removeEntry(entry);
                }

                return new GridFinishedFuture<>(e);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }
        else {
            return asyncOp(tx, new AsyncOp<Map<K1, V1>>(keys) {
                @Override public IgniteInternalFuture<Map<K1, V1>> op(GridNearTxLocal tx,
                    AffinityTopologyVersion readyTopVer) {
                    return tx.getAllAsync(ctx,
                        readyTopVer,
                        keys,
                        deserializeBinary,
                        skipVals,
                        false,
                        !readThrough,
                        recovery,
                        needVer);
                }
            }, ctx.operationContextPerCall(), /*retry*/false);
        }
    }
}

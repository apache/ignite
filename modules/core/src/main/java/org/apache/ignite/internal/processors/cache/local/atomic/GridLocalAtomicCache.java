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

package org.apache.ignite.internal.processors.cache.local.atomic;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheInvokeResult;
import org.apache.ignite.internal.processors.cache.CacheLazyEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.CacheStorePartialUpdateException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFilterFailedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.GridCachePreloaderAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.local.GridLocalCacheEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Non-transactional local cache.
 */
public class GridLocalAtomicCache<K, V> extends GridCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unsafe instance. */
    private static final sun.misc.Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private GridCachePreloader preldr;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridLocalAtomicCache() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     */
    public GridLocalAtomicCache(GridCacheContext<K, V> ctx) {
        super(ctx, ctx.config().getStartSize());

        preldr = new GridCachePreloaderAdapter(ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory() {
            @Override public GridCacheMapEntry create(
                GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key,
                int hash,
                CacheObject val,
                @Nullable GridCacheMapEntry next,
                int hdrId
            ) {
                return new GridLocalCacheEntry(ctx, key, hash, val, next, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader preloader() {
        return preldr;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V getAndPut(K key, V val, @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        return (V)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(val),
            null,
            expiryPerCall(),
            true,
            false,
            filter,
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @Override public boolean put(K key, V val, CacheEntryPredicate[] filter) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        boolean res = (Boolean)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(val),
            null,
            expiryPerCall(),
            false,
            false,
            filter,
            ctx.writeThrough(),
            ctx.readThrough());

        if (statsEnabled)
            metrics0().addPutTimeNanos(System.nanoTime() - start);

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndPutAsync0(K key, V val, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key", val, "val");

        return updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            true,
            false,
            filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> putAsync0(K key, V val, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key", val, "val");

        return updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            false,
            false,
            filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V getAndPutIfAbsent(K key, V val) throws IgniteCheckedException {
        return getAndPut(key, val, ctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutIfAbsentAsync(K key, V val) {
        return getAndPutAsync(key, val, ctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws IgniteCheckedException {
        return put(key, val, ctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, ctx.noValArray());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V getAndReplace(K key, V val) throws IgniteCheckedException {
        return getAndPut(key, val, ctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndReplaceAsync(K key, V val) {
        return getAndPutAsync(key, val, ctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws IgniteCheckedException {
        return put(key, val, ctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V val) {
        return putAsync(key, val, ctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        A.notNull(oldVal, "oldVal");

        return put(key, newVal, ctx.equalsValArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return putAsync(key, newVal, ctx.equalsValArray(oldVal));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheReturn replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        return (GridCacheReturn)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(newVal),
            null,
            expiryPerCall(),
            true,
            true,
            ctx.equalsValArray(oldVal),
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheReturn removex(K key, V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        return (GridCacheReturn)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            true,
            true,
            ctx.equalsValArray(val),
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn> removexAsync(K key, V val) {
        A.notNull(key, "key", val, "val");

        return removeAllAsync0(F.asList(key), true, true, ctx.equalsValArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<GridCacheReturn> replacexAsync(K key, V oldVal, V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        return updateAllAsync0(F.asMap(key, newVal),
            null,
            null,
            true,
            true,
            ctx.equalsValArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        updateAllInternal(UPDATE,
            m.keySet(),
            m.values(),
            null,
            expiryPerCall(),
            false,
            false,
            CU.empty0(),
            ctx.writeThrough(),
            ctx.readThrough());

        if (statsEnabled)
            metrics0().addPutTimeNanos(System.nanoTime() - start);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(Map<? extends K, ? extends V> m) {
        return updateAllAsync0(m,
            null,
            null,
            false,
            false,
            CU.empty0()).chain(RET2NULL);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V getAndRemove(K key) throws IgniteCheckedException {
        return (V)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            true,
            false,
            CU.empty0(),
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(K key) {
        return removeAllAsync0(Collections.singletonList(key), true, false, CU.empty0());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void removeAll(Collection<? extends K> keys) throws IgniteCheckedException {
        updateAllInternal(DELETE,
            keys,
            null,
            null,
            expiryPerCall(),
            false,
            false,
            CU.empty0(),
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(Collection<? extends K> keys) {
        return removeAllAsync0(keys, false, false, CU.empty0()).chain(RET2NULL);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean remove(K key) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        Boolean rmv = (Boolean)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            false,
            false,
            CU.empty0(),
            ctx.writeThrough(),
            ctx.readThrough());

        if (statsEnabled && rmv)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);

        return rmv;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key");

        return removeAllAsync0(Collections.singletonList(key), false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        return (Boolean)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            false,
            false,
            ctx.equalsValArray(val),
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        return removeAsync(key, ctx.equalsValArray(val));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return ctx.closures().callLocalSafe(new Callable<Void>() {
            @Override public Void call() throws Exception {
                removeAll();

                return null;
            }
        });
    }

    /** {@inheritDoc} */

    @SuppressWarnings("unchecked")
    @Override @Nullable public V get(K key, boolean deserializePortable) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        Map<K, V> m = getAllInternal(Collections.singleton(key),
            ctx.isSwapOrOffheapEnabled(),
            ctx.readThrough(),
            taskName,
            deserializePortable,
            false);

        assert m.isEmpty() || m.size() == 1 : m.size();

        return m.get(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final Map<K, V> getAll(Collection<? extends K> keys, boolean deserializePortable)
        throws IgniteCheckedException {
        A.notNull(keys, "keys");

        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllInternal(keys,
            ctx.isSwapOrOffheapEnabled(),
            ctx.readThrough(),
            taskName,
            deserializePortable,
            false);
    }


    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx entry,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializePortable,
        final boolean skipVals,
        boolean canRemap
    ) {
        A.notNull(keys, "keys");

        final boolean swapOrOffheap = ctx.isSwapOrOffheapEnabled();
        final boolean storeEnabled = ctx.readThrough();

        return asyncOp(new Callable<Map<K, V>>() {
            @Override public Map<K, V> call() throws Exception {
                return getAllInternal(keys, swapOrOffheap, storeEnabled, taskName, deserializePortable, skipVals);
            }
        });
    }

    /**
     * Entry point to all public API get methods.
     *
     * @param keys Keys to remove.
     * @param swapOrOffheap {@code True} if swap of off-heap storage are enabled.
     * @param storeEnabled Store enabled flag.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable .
     * @param skipVals Skip value flag.
     * @return Key-value map.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private Map<K, V> getAllInternal(@Nullable Collection<? extends K> keys,
        boolean swapOrOffheap,
        boolean storeEnabled,
        String taskName,
        boolean deserializePortable,
        boolean skipVals
    ) throws IgniteCheckedException {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return Collections.emptyMap();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        UUID subjId = ctx.subjectIdPerCall(null, opCtx);

        Map<K, V> vals = new HashMap<>(keys.size(), 1.0f);

        if (keyCheck)
            validateCacheKeys(keys);

        final IgniteCacheExpiryPolicy expiry = expiryPolicy(opCtx != null ? opCtx.expiry() : null);

        boolean success = true;

        for (K key : keys) {
            if (key == null)
                throw new NullPointerException("Null key.");

            GridCacheEntryEx entry = null;

            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            while (true) {
                try {
                    entry = swapOrOffheap ? entryEx(cacheKey) : peekEx(cacheKey);

                    if (entry != null) {
                        CacheObject v = entry.innerGet(null,
                            /*swap*/swapOrOffheap,
                            /*read-through*/false,
                            /*fail-fast*/false,
                            /*unmarshal*/true,
                            /**update-metrics*/true,
                            /**event*/!skipVals,
                            /**temporary*/false,
                            subjId,
                            null,
                            taskName,
                            expiry);

                        if (v != null)
                            ctx.addResult(vals, cacheKey, v, skipVals, false, deserializePortable, true);
                        else
                            success = false;
                    }
                    else {
                        if (!storeEnabled && configuration().isStatisticsEnabled() && !skipVals)
                            metrics0().onRead(false);

                        success = false;
                    }

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // No-op, retry.
                }
                catch (GridCacheFilterFailedException ignored) {
                    // No-op, skip the key.
                    break;
                }
                finally {
                    if (entry != null)
                        ctx.evicts().touch(entry, ctx.affinity().affinityTopologyVersion());
                }

                if (!success && storeEnabled)
                    break;
            }
        }

        if (success || !storeEnabled)
            return vals;

        return getAllAsync(
            keys,
            opCtx == null || !opCtx.skipStore(),
            null,
            false,
            subjId,
            taskName,
            deserializePortable,
            /*force primary*/false,
            expiry,
            skipVals,
            /*can remap*/true).get();
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        EntryProcessorResult<T> res = invokeAsync(key, entryProcessor, args).get();

        return res != null ? res : new CacheInvokeResult<T>();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKeys(keys);

        Map<? extends K, EntryProcessor> invokeMap = F.viewAsMap(keys, new C1<K, EntryProcessor>() {
            @Override public EntryProcessor apply(K k) {
                return entryProcessor;
            }
        });

        return (Map<K, EntryProcessorResult<T>>)updateAllInternal(TRANSFORM,
            invokeMap.keySet(),
            invokeMap.values(),
            args,
            expiryPerCall(),
            false,
            false,
            null,
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws EntryProcessorException {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        Map<? extends K, EntryProcessor> invokeMap =
            Collections.singletonMap(key, (EntryProcessor)entryProcessor);

        IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut = updateAllAsync0(null,
            invokeMap,
            args,
            false,
            false,
            null);

        return fut.chain(new CX1<IgniteInternalFuture<Map<K, EntryProcessorResult<T>>>, EntryProcessorResult<T>>() {
            @Override public EntryProcessorResult<T> applyx(IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> fut)
                throws IgniteCheckedException {
                Map<K, EntryProcessorResult<T>> resMap = fut.get();

                if (resMap != null) {
                    assert resMap.isEmpty() || resMap.size() == 1 : resMap.size();

                    return resMap.isEmpty() ? null : resMap.values().iterator().next();
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKeys(keys);

        Map<? extends K, EntryProcessor> invokeMap = F.viewAsMap(keys, new C1<K, EntryProcessor>() {
            @Override public EntryProcessor apply(K k) {
                return entryProcessor;
            }
        });

        return updateAllAsync0(null,
            invokeMap,
            args,
            true,
            false,
            null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        return (Map<K, EntryProcessorResult<T>>)updateAllInternal(TRANSFORM,
            map.keySet(),
            map.values(),
            args,
            expiryPerCall(),
            false,
            false,
            null,
            ctx.writeThrough(),
            ctx.readThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        return updateAllAsync0(null,
            map,
            args,
            true,
            false,
            null);
    }

    /**
     * Entry point for public API update methods.
     *
     * @param map Put map. Either {@code map} or {@code invokeMap} should be passed.
     * @param invokeMap Transform map. Either {@code map} or {@code invokeMap} should be passed.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter for atomic updates.
     * @return Completion future.
     */
    private IgniteInternalFuture updateAllAsync0(
        @Nullable final Map<? extends K, ? extends V> map,
        @Nullable final Map<? extends K, ? extends EntryProcessor> invokeMap,
        @Nullable final Object[] invokeArgs,
        final boolean retval,
        final boolean rawRetval,
        @Nullable final CacheEntryPredicate[] filter
    ) {
        final GridCacheOperation op = invokeMap != null ? TRANSFORM : UPDATE;

        final Collection<? extends K> keys =
            map != null ? map.keySet() : invokeMap != null ? invokeMap.keySet() : null;

        final Collection<?> vals = map != null ? map.values() : invokeMap != null ? invokeMap.values() : null;

        final boolean writeThrough = ctx.writeThrough();

        final boolean readThrough = ctx.readThrough();

        final ExpiryPolicy expiry = expiryPerCall();

        IgniteInternalFuture fut = asyncOp(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return updateAllInternal(op,
                    keys,
                    vals,
                    invokeArgs,
                    expiry,
                    retval,
                    rawRetval,
                    filter,
                    writeThrough,
                    readThrough);
            }
        });

        if (ctx.config().isStatisticsEnabled())
            fut.listen(new UpdatePutTimeStatClosure(metrics0(), System.nanoTime()));

        return fut;
    }

    /**
     * Entry point for public API remove methods.
     *
     * @param keys Keys to remove.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter.
     * @return Completion future.
     */
    private IgniteInternalFuture removeAllAsync0(
        @Nullable final Collection<? extends K> keys,
        final boolean retval,
        final boolean rawRetval,
        @Nullable final CacheEntryPredicate[] filter
    ) {
        final boolean writeThrough = ctx.writeThrough();

        final boolean readThrough = ctx.readThrough();

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        final ExpiryPolicy expiryPlc = expiryPerCall();

        IgniteInternalFuture fut = asyncOp(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return updateAllInternal(DELETE,
                    keys,
                    null,
                    null,
                    expiryPlc,
                    retval,
                    rawRetval,
                    filter,
                    writeThrough,
                    readThrough);
            }
        });

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<>(metrics0(), start));

        return fut;
    }

    /**
     * Entry point for all public update methods (put, remove, invoke).
     *
     * @param op Operation.
     * @param keys Keys.
     * @param vals Values.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param expiryPlc Expiry policy.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter.
     * @param writeThrough Write through.
     * @param readThrough Read through.
     * @return Update result.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private Object updateAllInternal(GridCacheOperation op,
        Collection<? extends K> keys,
        @Nullable Iterable<?> vals,
        @Nullable Object[] invokeArgs,
        @Nullable ExpiryPolicy expiryPlc,
        boolean retval,
        boolean rawRetval,
        CacheEntryPredicate[] filter,
        boolean writeThrough,
        boolean readThrough) throws IgniteCheckedException {
        if (keyCheck)
            validateCacheKeys(keys);

        if (op == DELETE)
            ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);
        else
            ctx.checkSecurity(SecurityPermission.CACHE_PUT);

        String taskName = ctx.kernalContext().job().currentTaskName();

        GridCacheVersion ver = ctx.versions().next();

        UUID subjId = ctx.subjectIdPerCall(null);

        if (writeThrough && keys.size() > 1) {
            return updateWithBatch(op,
                keys,
                vals,
                invokeArgs,
                expiryPlc,
                ver,
                filter,
                subjId,
                taskName);
        }

        Iterator<?> valsIter = vals != null ? vals.iterator() : null;

        IgniteBiTuple<Boolean, ?> res = null;

        CachePartialUpdateCheckedException err = null;

        boolean intercept = ctx.config().getInterceptor() != null;

        for (K key : keys) {
            if (key == null)
                throw new NullPointerException("Null key.");

            Object val = valsIter != null ? valsIter.next() : null;

            if (val == null && op != DELETE)
                throw new NullPointerException("Null value.");

            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            if (op == UPDATE)
                val = ctx.toCacheObject(val);

            while (true) {
                GridCacheEntryEx entry = null;

                try {
                    entry = entryEx(cacheKey);

                    GridTuple3<Boolean, Object, EntryProcessorResult<Object>> t = entry.innerUpdateLocal(
                        ver,
                        val == null ? DELETE : op,
                        val,
                        invokeArgs,
                        writeThrough,
                        readThrough,
                        retval,
                        expiryPlc,
                        true,
                        true,
                        filter,
                        intercept,
                        subjId,
                        taskName);

                    if (op == TRANSFORM) {
                        if (t.get3() != null) {
                            Map<K, EntryProcessorResult> computedMap;

                            if (res == null) {
                                computedMap = U.newHashMap(keys.size());

                                res = new IgniteBiTuple<>(true, computedMap);
                            }
                            else
                                computedMap = (Map<K, EntryProcessorResult>)res.get2();

                            computedMap.put(key, t.get3());
                        }
                    }
                    else if (res == null)
                        res = new T2(t.get1(), t.get2());

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while updating (will retry): " + key);

                    entry = null;
                }
                catch (IgniteCheckedException e) {
                    if (err == null)
                        err = partialUpdateException();

                    err.add(F.asList(key), e);

                    U.error(log, "Failed to update key : " + key, e);

                    break;
                }
                finally {
                    if (entry != null)
                        ctx.evicts().touch(entry, ctx.affinity().affinityTopologyVersion());
                }
            }
        }

        if (err != null)
            throw err;

        Object ret = res == null ? null : rawRetval ? new GridCacheReturn(ctx,  true, res.get2(), res.get1()) :
            (retval || op == TRANSFORM) ? res.get2() : res.get1();

        if (op == TRANSFORM && ret == null)
            ret = Collections.emptyMap();

        return ret;
    }

    /**
     * Updates entries using batched write-through.
     *
     * @param op Operation.
     * @param keys Keys.
     * @param vals Values.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param expiryPlc Expiry policy.
     * @param ver Cache version.
     * @param filter Optional filter.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @throws CachePartialUpdateCheckedException If update failed.
     * @return Results map for invoke operation.
     */
    @SuppressWarnings({"ForLoopReplaceableByForEach", "unchecked"})
    private Map<K, EntryProcessorResult> updateWithBatch(
        GridCacheOperation op,
        Collection<? extends K> keys,
        @Nullable Iterable<?> vals,
        @Nullable Object[] invokeArgs,
        @Nullable ExpiryPolicy expiryPlc,
        GridCacheVersion ver,
        @Nullable CacheEntryPredicate[] filter,
        UUID subjId,
        String taskName
    ) throws IgniteCheckedException {
        List<GridCacheEntryEx> locked = lockEntries(keys);

        try {
            int size = locked.size();

            Map<Object, Object> putMap = null;

            Collection<Object> rmvKeys = null;

            List<CacheObject> writeVals = null;

            Map<K, EntryProcessorResult> invokeResMap =
                op == TRANSFORM ? U.<K, EntryProcessorResult>newHashMap(size) : null;

            List<GridCacheEntryEx> filtered = new ArrayList<>(size);

            CachePartialUpdateCheckedException err = null;

            Iterator<?> valsIter = vals != null ? vals.iterator() : null;

            boolean intercept = ctx.config().getInterceptor() != null;

            for (int i = 0; i < size; i++) {
                GridCacheEntryEx entry = locked.get(i);

                Object val = valsIter != null ? valsIter.next() : null;

                if (val == null && op != DELETE)
                    throw new NullPointerException("Null value.");

                try {
                    try {
                        if (!ctx.isAllLocked(entry, filter)) {
                            if (log.isDebugEnabled())
                                log.debug("Entry did not pass the filter (will skip write) [entry=" + entry +
                                    ", filter=" + Arrays.toString(filter) + ']');

                            continue;
                        }
                    }
                    catch (IgniteCheckedException e) {
                        if (err == null)
                            err = partialUpdateException();

                        err.add(F.asList(entry.key()), e);

                        continue;
                    }

                    if (op == TRANSFORM) {
                        EntryProcessor<Object, Object, Object> entryProcessor =
                            (EntryProcessor<Object, Object, Object>)val;

                        CacheObject old = entry.innerGet(null,
                            /*swap*/true,
                            /*read-through*/true,
                            /*fail-fast*/false,
                            /*unmarshal*/true,
                            /**update-metrics*/true,
                            /**event*/true,
                            /**temporary*/true,
                            subjId,
                            entryProcessor,
                            taskName,
                            null);

                        Object oldVal = null;

                        CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry<>(ctx, entry.key(), old,
                            entry.version());

                        CacheObject updated;
                        Object updatedVal = null;
                        CacheInvokeResult invokeRes = null;

                        try {
                            Object computed = entryProcessor.process(invokeEntry, invokeArgs);

                            updatedVal = ctx.unwrapTemporary(invokeEntry.getValue());

                            updated = ctx.toCacheObject(updatedVal);

                            if (computed != null)
                                invokeRes = CacheInvokeResult.fromResult(ctx.unwrapTemporary(computed));
                        }
                        catch (Exception e) {
                            invokeRes = CacheInvokeResult.fromError(e);

                            updated = old;
                        }

                        if (invokeRes != null)
                            invokeResMap.put((K)entry.key().value(ctx.cacheObjectContext(), false), invokeRes);

                        if (updated == null) {
                            if (intercept) {
                                IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor()
                                    .onBeforeRemove(new CacheLazyEntry(ctx, entry.key(), invokeEntry.key(),
                                        old, oldVal));

                                if (ctx.cancelRemove(interceptorRes))
                                    continue;
                            }

                            // Update previous batch.
                            if (putMap != null) {
                                err = updatePartialBatch(
                                    filtered,
                                    ver,
                                    writeVals,
                                    putMap,
                                    null,
                                    expiryPlc,
                                    err,
                                    subjId,
                                    taskName);

                                putMap = null;
                                writeVals = null;

                                filtered = new ArrayList<>();
                            }

                            // Start collecting new batch.
                            if (rmvKeys == null)
                                rmvKeys = new ArrayList<>(size);

                            rmvKeys.add(entry.key().value(ctx.cacheObjectContext(), false));
                        }
                        else {
                            if (intercept) {
                                Object interceptorVal = ctx.config().getInterceptor()
                                    .onBeforePut(new CacheLazyEntry(ctx, entry.key(), invokeEntry.getKey(),
                                        old, oldVal), updatedVal);

                                if (interceptorVal == null)
                                    continue;

                                updated = ctx.toCacheObject(ctx.unwrapTemporary(interceptorVal));
                            }

                            // Update previous batch.
                            if (rmvKeys != null) {
                                err = updatePartialBatch(
                                    filtered,
                                    ver,
                                    null,
                                    null,
                                    rmvKeys,
                                    expiryPlc,
                                    err,
                                    subjId,
                                    taskName);

                                rmvKeys = null;

                                filtered = new ArrayList<>();
                            }

                            if (putMap == null) {
                                putMap = new LinkedHashMap<>(size, 1.0f);
                                writeVals = new ArrayList<>(size);
                            }

                            putMap.put(CU.value(entry.key(), ctx, false), CU.value(updated, ctx, false));
                            writeVals.add(updated);
                        }
                    }
                    else if (op == UPDATE) {
                        CacheObject cacheVal = ctx.toCacheObject(val);

                        if (intercept) {
                            CacheObject old = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/ctx.loadPreviousValue(),
                                /*fail-fast*/false,
                                /*unmarshal*/true,
                                /**update-metrics*/true,
                                /**event*/true,
                                /**temporary*/true,
                                subjId,
                                null,
                                taskName,
                                null);

                            Object interceptorVal = ctx.config().getInterceptor().onBeforePut(new CacheLazyEntry(
                                ctx, entry.key(), old), val);

                            if (interceptorVal == null)
                                continue;

                            cacheVal = ctx.toCacheObject(ctx.unwrapTemporary(interceptorVal));
                        }

                        if (putMap == null) {
                            putMap = new LinkedHashMap<>(size, 1.0f);
                            writeVals = new ArrayList<>(size);
                        }

                        putMap.put(CU.value(entry.key(), ctx, false), CU.value(cacheVal, ctx, false));
                        writeVals.add(cacheVal);
                    }
                    else {
                        assert op == DELETE;

                        if (intercept) {
                            CacheObject old = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/ctx.loadPreviousValue(),
                                /*fail-fast*/false,
                                /*unmarshal*/true,
                                /**update-metrics*/true,
                                /**event*/true,
                                /**temporary*/true,
                                subjId,
                                null,
                                taskName,
                                null);

                            IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor()
                                .onBeforeRemove(new CacheLazyEntry(ctx, entry.key(), old));

                            if (ctx.cancelRemove(interceptorRes))
                                continue;
                        }

                        if (rmvKeys == null)
                            rmvKeys = new ArrayList<>(size);

                        rmvKeys.add(entry.key().value(ctx.cacheObjectContext(), false));
                    }

                    filtered.add(entry);
                }
                catch (IgniteCheckedException e) {
                    if (err == null)
                        err = partialUpdateException();

                    err.add(F.asList(entry.key()), e);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    assert false : "Entry cannot become obsolete while holding lock.";
                }
                catch (GridCacheFilterFailedException ignore) {
                    assert false : "Filter should never fail with failFast=false and empty filter.";
                }
            }

            // Store final batch.
            if (putMap != null || rmvKeys != null) {
                err = updatePartialBatch(
                    filtered,
                    ver,
                    writeVals,
                    putMap,
                    rmvKeys,
                    expiryPlc,
                    err,
                    subjId,
                    taskName);
            }
            else
                assert filtered.isEmpty();

            if (err != null)
                throw err;

            return invokeResMap;
        }
        finally {
            unlockEntries(locked);
        }
    }

    /**
     * @param entries Entries to update.
     * @param ver Cache version.
     * @param writeVals Cache values.
     * @param putMap Values to put.
     * @param rmvKeys Keys to remove.
     * @param expiryPlc Expiry policy.
     * @param err Optional partial update exception.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @return Partial update exception.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ForLoopReplaceableByForEach"})
    @Nullable private CachePartialUpdateCheckedException updatePartialBatch(
        List<GridCacheEntryEx> entries,
        final GridCacheVersion ver,
        @Nullable List<CacheObject> writeVals,
        @Nullable Map<Object, Object> putMap,
        @Nullable Collection<Object> rmvKeys,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable CachePartialUpdateCheckedException err,
        UUID subjId,
        String taskName
    ) {
        assert putMap == null ^ rmvKeys == null;
        GridCacheOperation op;

        CacheStorePartialUpdateException storeErr = null;

        try {
            if (putMap != null) {
                try {
                    ctx.store().putAll(null, F.viewReadOnly(putMap, new C1<Object, IgniteBiTuple<Object, GridCacheVersion>>() {
                        @Override public IgniteBiTuple<Object, GridCacheVersion> apply(Object v) {
                            return F.t(v, ver);
                        }
                    }));
                }
                catch (CacheStorePartialUpdateException e) {
                    storeErr = e;
                }

                op = UPDATE;
            }
            else {
                try {
                    ctx.store().removeAll(null, rmvKeys);
                }
                catch (CacheStorePartialUpdateException e) {
                    storeErr = e;
                }

                op = DELETE;
            }
        }
        catch (IgniteCheckedException e) {
            if (err == null)
                err = partialUpdateException();

            err.add(putMap != null ? putMap.keySet() : rmvKeys, e);

            return err;
        }

        boolean intercept = ctx.config().getInterceptor() != null;

        for (int i = 0; i < entries.size(); i++) {
            GridCacheEntryEx entry = entries.get(i);

            assert Thread.holdsLock(entry);

            if (entry.obsolete() ||
                (storeErr != null && storeErr.failedKeys().contains(entry.key().value(ctx.cacheObjectContext(), false))))
                continue;

            try {
                // We are holding java-level locks on entries at this point.
                CacheObject writeVal = op == UPDATE ? writeVals.get(i) : null;

                assert writeVal != null || op == DELETE : "null write value found.";

                GridTuple3<Boolean, Object, EntryProcessorResult<Object>> t = entry.innerUpdateLocal(
                    ver,
                    op,
                    writeVal,
                    null,
                    false,
                    false,
                    false,
                    expiryPlc,
                    true,
                    true,
                    null,
                    false,
                    subjId,
                    taskName);

                if (intercept) {
                    if (op == UPDATE)
                        ctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(ctx, entry.key(), writeVal));
                    else
                        ctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(ctx, entry.key(), t.get2()));
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                assert false : "Entry cannot become obsolete while holding lock.";
            }
            catch (IgniteCheckedException e) {
                if (err == null)
                    err = partialUpdateException();

                err.add(Collections.singleton(entry.key()), e);
            }
        }

        return err;
    }

    /**
     * Acquires java-level locks on cache entries.
     *
     * @param keys Keys to lock.
     * @return Collection of locked entries.
     */
    private List<GridCacheEntryEx> lockEntries(Collection<? extends K> keys) {
        List<GridCacheEntryEx> locked = new ArrayList<>(keys.size());

        boolean nullKeys = false;

        while (true) {
            for (K key : keys) {
                if (key == null) {
                    nullKeys = true;

                    break;
                }

                GridCacheEntryEx entry = entryEx(ctx.toCacheKeyObject(key));

                locked.add(entry);
            }

            if (nullKeys)
                break;

            for (int i = 0; i < locked.size(); i++) {
                GridCacheEntryEx entry = locked.get(i);

                UNSAFE.monitorEnter(entry);

                if (entry.obsolete()) {
                    // Unlock all locked.
                    for (int j = 0; j <= i; j++)
                        UNSAFE.monitorExit(locked.get(j));

                    // Clear entries.
                    locked.clear();

                    // Retry.
                    break;
                }
            }

            if (!locked.isEmpty())
                return locked;
        }

        assert nullKeys;

        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        for (GridCacheEntryEx entry : locked)
            ctx.evicts().touch(entry, topVer);

        throw new NullPointerException("Null key.");
    }

    /**
     * Releases java-level locks on cache entries.
     *
     * @param locked Locked entries.
     */
    private void unlockEntries(Iterable<GridCacheEntryEx> locked) {
        for (GridCacheEntryEx entry : locked)
            UNSAFE.monitorExit(entry);

        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        for (GridCacheEntryEx entry : locked)
            ctx.evicts().touch(entry, topVer);
    }

    /**
     * @return New partial update exception.
     */
    private static CachePartialUpdateCheckedException partialUpdateException() {
        return new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> txLockAsync(Collection<KeyCacheObject> keys,
        long timeout,
        IgniteTxLocalEx tx,
        boolean isRead,
        boolean retval,
        TransactionIsolation isolation,
        boolean invalidate,
        long accessTtl) {
        return new GridFinishedFuture<>(new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys,
        long timeout) {
        return new GridFinishedFuture<>(new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void unlockAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        throw new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)");
    }

    /**
     * @return Expiry policy.
     */
    @Nullable private ExpiryPolicy expiryPerCall() {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy expiry = opCtx != null ? opCtx.expiry() : null;

        if (expiry == null)
            expiry = ctx.expiry();

        return expiry;
    }

    /**
     * @param op Operation closure.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected IgniteInternalFuture asyncOp(final Callable<?> op) {
        IgniteInternalFuture fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteInternalFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                IgniteInternalFuture f = new GridEmbeddedFuture(fut,
                    new C2<Object, Exception, IgniteInternalFuture>() {
                        @Override public IgniteInternalFuture apply(Object t, Exception e) {
                            return ctx.closures().callLocalSafe(op);
                        }
                    });

                saveFuture(holder, f);

                return f;
            }

            IgniteInternalFuture f = ctx.closures().callLocalSafe(op);

            saveFuture(holder, f);

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }
}
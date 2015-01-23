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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.local.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.GridCacheFlag.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Non-transactional local cache.
 */
public class GridLocalAtomicCache<K, V> extends GridCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unsafe instance. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private GridCachePreloader<K,V> preldr;

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

        preldr = new GridCachePreloaderAdapter<>(ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, @Nullable GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridLocalCacheEntry<K, V>(ctx, key, hash, val, next, ttl, hdrId) {
                    @Override public GridCacheEntry<K, V> wrapFilterLocked() throws IgniteCheckedException {
                        assert Thread.holdsLock(this);

                        return new GridCacheFilterEvaluationEntry<>(key, rawGetOrUnmarshalUnlocked(true), this);
                    }
                };
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader<K, V> preloader() {
        return preldr;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V put(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        ctx.denyOnLocalRead();

        return (V)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(val),
            null,
            expiryPerCall(),
            true,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean putx(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        ctx.denyOnLocalRead();

        return (Boolean)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(val),
            null,
            expiryPerCall(),
            false,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        ctx.denyOnLocalRead();

        return (Boolean)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(val),
            null,
            expiryPerCall(),
            false,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<V> putAsync(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key", val, "val");

        ctx.denyOnLocalRead();

        return updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            true,
            false,
            filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<Boolean> putxAsync(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key", val, "val");

        ctx.denyOnLocalRead();

        return updateAllAsync0(F0.asMap(key, val),
            null,
            null,
            false,
            false,
            filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V putIfAbsent(K key, V val) throws IgniteCheckedException {
        return put(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws IgniteCheckedException {
        return putx(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return putxAsync(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V replace(K key, V val) throws IgniteCheckedException {
        return put(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> replaceAsync(K key, V val) {
        return putAsync(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws IgniteCheckedException {
        return putx(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replacexAsync(K key, V val) {
        return putxAsync(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return putx(key, newVal, ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return putxAsync(key, newVal, ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return (GridCacheReturn<V>)updateAllInternal(UPDATE,
            Collections.singleton(key),
            Collections.singleton(newVal),
            null,
            expiryPerCall(),
            true,
            true,
            ctx.equalsPeekArray(oldVal),
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheReturn<V> removex(K key, V val) throws IgniteCheckedException {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return (GridCacheReturn<V>)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            true,
            true,
            ctx.equalsPeekArray(val),
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<GridCacheReturn<V>> removexAsync(K key, V val) {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return removeAllAsync0(F.asList(key), true, true, ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal) {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return updateAllAsync0(F.asMap(key, newVal),
            null,
            null,
            true,
            true,
            ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        ctx.denyOnLocalRead();

        updateAllInternal(UPDATE,
            m.keySet(),
            m.values(),
            null,
            expiryPerCall(),
            false,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        ctx.denyOnLocalRead();

        return updateAllAsync0(m,
            null,
            null,
            false,
            false,
            filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V remove(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        ctx.denyOnLocalRead();

        return (V)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            true,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<V> removeAsync(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnLocalRead();

        return removeAllAsync0(Collections.singletonList(key), true, false, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void removeAll(Collection<? extends K> keys,
        IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        ctx.denyOnLocalRead();

        updateAllInternal(DELETE,
            keys,
            null,
            null,
            expiryPerCall(),
            false,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllAsync(Collection<? extends K> keys,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        ctx.denyOnLocalRead();

        return removeAllAsync0(keys, false, false, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean removex(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return (Boolean)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            false,
            false,
            filter,
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<Boolean> removexAsync(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return removeAllAsync0(Collections.singletonList(key), false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        A.notNull(key, "key");

        ctx.denyOnLocalRead();

        return (Boolean)updateAllInternal(DELETE,
            Collections.singleton(key),
            null,
            null,
            expiryPerCall(),
            false,
            false,
            ctx.equalsPeekArray(val),
            ctx.writeThrough());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V val) {
        return removexAsync(key, ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void removeAll(IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        removeAll(keySet(filter));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllAsync(IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return removeAllAsync(keySet(filter), filter);
    }

    /** {@inheritDoc} */

    @SuppressWarnings("unchecked")
    @Override @Nullable public V get(K key, boolean deserializePortable,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) throws IgniteCheckedException {
        ctx.denyOnFlag(LOCAL);

        String taskName = ctx.kernalContext().job().currentTaskName();

        Map<K, V> m = getAllInternal(Collections.singleton(key),
            filter != null ? new IgnitePredicate[]{filter} : null,
            ctx.isSwapOrOffheapEnabled(),
            ctx.readThrough(),
            ctx.hasFlag(CLONE),
            taskName,
            deserializePortable);

        return m.get(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final Map<K, V> getAll(Collection<? extends K> keys, boolean deserializePortable,
        IgnitePredicate<GridCacheEntry<K, V>> filter)
        throws IgniteCheckedException {
        ctx.denyOnFlag(LOCAL);

        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllInternal(keys,
            filter != null ? new IgnitePredicate[]{filter} : null,
            ctx.isSwapOrOffheapEnabled(),
            ctx.readThrough(),
            ctx.hasFlag(CLONE),
            taskName,
            deserializePortable);
    }


    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        final String taskName,
        final boolean deserializePortable,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>[] filter
    ) {
        ctx.denyOnFlag(LOCAL);

        final boolean swapOrOffheap = ctx.isSwapOrOffheapEnabled();
        final boolean storeEnabled = ctx.readThrough();
        final boolean clone = ctx.hasFlag(CLONE);

        return asyncOp(new Callable<Map<K, V>>() {
            @Override public Map<K, V> call() throws Exception {
                return getAllInternal(keys, filter, swapOrOffheap, storeEnabled, clone, taskName, deserializePortable);
            }
        });
    }

    /**
     * Entry point to all public API get methods.
     *
     * @param keys Keys to remove.
     * @param filter Filter.
     * @param swapOrOffheap {@code True} if swap of off-heap storage are enabled.
     * @param storeEnabled Store enabled flag.
     * @param clone {@code True} if returned values should be cloned.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable .
     * @return Key-value map.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private Map<K, V> getAllInternal(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        boolean swapOrOffheap,
        boolean storeEnabled,
        boolean clone,
        String taskName,
        boolean deserializePortable) throws IgniteCheckedException {
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return Collections.emptyMap();

        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        UUID subjId = ctx.subjectIdPerCall(null, prj);

        ExpiryPolicy expiryPlc = prj != null ? prj.expiry() : null;

        if (expiryPlc == null)
            expiryPlc = ctx.expiry();

        Map<K, V> vals = new HashMap<>(keys.size(), 1.0f);

        if (keyCheck)
            validateCacheKeys(keys);

        final GetExpiryPolicy expiry = accessExpiryPolicy(expiryPlc);

        boolean success = true;

        for (K key : keys) {
            if (key == null)
                continue;

            GridCacheEntryEx<K, V> entry = null;

            while (true) {
                try {
                    entry = swapOrOffheap ? entryEx(key) : peekEx(key);

                    if (entry != null) {
                        V v = entry.innerGet(null,
                            /*swap*/swapOrOffheap,
                            /*read-through*/false,
                            /*fail-fast*/false,
                            /*unmarshal*/true,
                            /**update-metrics*/true,
                            /**event*/true,
                            /**temporary*/false,
                            subjId,
                            null,
                            taskName,
                            filter,
                            expiry);

                        if (v != null)
                            vals.put(key, v);
                        else
                            success = false;
                    }
                    else {
                        if (!storeEnabled)
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

        if (success || !storeEnabled) {
            if (!clone)
                return vals;

            Map<K, V> map = new GridLeanMap<>();

            for (Map.Entry<K, V> e : vals.entrySet())
                map.put(e.getKey(), ctx.cloneValue(e.getValue()));

            return map;
        }

        return getAllAsync(keys, true, null, false, subjId, taskName, deserializePortable, false, expiry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return invokeAsync(key, entryProcessor, args).get();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return invokeAllAsync(keys, entryProcessor, args).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws EntryProcessorException {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnLocalRead();

        Map<? extends K, EntryProcessor> invokeMap =
            Collections.singletonMap(key, (EntryProcessor)entryProcessor);

        IgniteFuture<Map<K, EntryProcessorResult<T>>> fut = updateAllAsync0(null,
            invokeMap,
            args,
            true,
            false,
            null);

        return fut.chain(new CX1<IgniteFuture<Map<K, EntryProcessorResult<T>>>, EntryProcessorResult<T>>() {
            @Override public EntryProcessorResult<T> applyx(IgniteFuture<Map<K, EntryProcessorResult<T>>> fut)
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
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKeys(keys);

        ctx.denyOnLocalRead();

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
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        return invokeAllAsync(map, args).get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        ctx.denyOnLocalRead();

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
    private IgniteFuture updateAllAsync0(
        @Nullable final Map<? extends K, ? extends V> map,
        @Nullable final Map<? extends K, ? extends EntryProcessor> invokeMap,
        @Nullable final Object[] invokeArgs,
        final boolean retval,
        final boolean rawRetval,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>[] filter
    ) {
        final GridCacheOperation op = invokeMap != null ? TRANSFORM : UPDATE;

        final Collection<? extends K> keys =
            map != null ? map.keySet() : invokeMap != null ? invokeMap.keySet() : null;

        final Collection<?> vals = map != null ? map.values() : invokeMap != null ? invokeMap.values() : null;

        final boolean writeThrough = ctx.writeThrough();

        final ExpiryPolicy expiry = expiryPerCall();

        return asyncOp(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return updateAllInternal(op,
                    keys,
                    vals,
                    invokeArgs,
                    expiry,
                    retval,
                    rawRetval,
                    filter,
                    writeThrough);
            }
        });
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
    private IgniteFuture removeAllAsync0(
        @Nullable final Collection<? extends K> keys,
        final boolean retval,
        final boolean rawRetval,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>[] filter
    ) {
        final boolean writeThrough = ctx.writeThrough();

        final ExpiryPolicy expiryPlc = expiryPerCall();

        return asyncOp(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return updateAllInternal(DELETE,
                    keys,
                    null,
                    null,
                    expiryPlc,
                    retval,
                    rawRetval,
                    filter,
                    writeThrough);
            }
        });
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
     * @param storeEnabled Store enabled flag.
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
        IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        boolean storeEnabled) throws IgniteCheckedException {
        if (keyCheck)
            validateCacheKeys(keys);

        if (op == DELETE)
            ctx.checkSecurity(GridSecurityPermission.CACHE_REMOVE);
        else
            ctx.checkSecurity(GridSecurityPermission.CACHE_PUT);

        String taskName = ctx.kernalContext().job().currentTaskName();

        GridCacheVersion ver = ctx.versions().next();

        UUID subjId = ctx.subjectIdPerCall(null);

        if (storeEnabled && keys.size() > 1) {
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

        GridCachePartialUpdateException err = null;

        boolean intercept = ctx.config().getInterceptor() != null;

        for (K key : keys) {
            Object val = valsIter != null ? valsIter.next() : null;

            if (key == null)
                continue;

            while (true) {
                GridCacheEntryEx<K, V> entry = null;

                try {
                    entry = entryEx(key);

                    GridTuple3<Boolean, V, EntryProcessorResult<Object>> t = entry.innerUpdateLocal(
                        ver,
                        val == null ? DELETE : op,
                        val,
                        invokeArgs,
                        storeEnabled,
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

        Object ret = res == null ? null : rawRetval ?
            new GridCacheReturn<>(res.get2(), res.get1()) : retval ? res.get2() : res.get1();

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
     * @throws GridCachePartialUpdateException If update failed.
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
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        UUID subjId,
        String taskName
    ) throws IgniteCheckedException {
        List<GridCacheEntryEx<K, V>> locked = lockEntries(keys);

        try {
            int size = locked.size();

            Map<K, V> putMap = null;

            Collection<K> rmvKeys = null;

            Map<K, EntryProcessorResult> invokeResMap =
                op == TRANSFORM ? U.<K, EntryProcessorResult>newHashMap(size) : null;

            List<GridCacheEntryEx<K, V>> filtered = new ArrayList<>(size);

            GridCachePartialUpdateException err = null;

            Iterator<?> valsIter = vals != null ? vals.iterator() : null;

            boolean intercept = ctx.config().getInterceptor() != null;

            for (int i = 0; i < size; i++) {
                GridCacheEntryEx<K, V> entry = locked.get(i);

                Object val = valsIter != null ? valsIter.next() : null;

                if (val == null && op != DELETE)
                    continue;

                try {
                    try {
                        if (!ctx.isAll(entry.wrapFilterLocked(), filter)) {
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
                        EntryProcessor<K, V, ?> entryProcessor = (EntryProcessor<K, V, ?>)val;

                        V old = entry.innerGet(null,
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
                            CU.<K, V>empty(),
                            null);

                        CacheInvokeEntry<K, V> invokeEntry = new CacheInvokeEntry<>(entry.key(), old);

                        V updated;
                        CacheInvokeResult invokeRes = null;

                        try {
                            Object computed = entryProcessor.process(invokeEntry, invokeArgs);

                            updated = ctx.unwrapTemporary(invokeEntry.getValue());

                            if (computed != null)
                                invokeRes = new CacheInvokeResult<>(ctx.unwrapTemporary(computed));
                        }
                        catch (Exception e) {
                            invokeRes = new CacheInvokeResult<>(e);

                            updated = old;
                        }

                        if (invokeRes != null)
                            invokeResMap.put(entry.key(), invokeRes);

                        if (updated == null) {
                            if (intercept) {
                                IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor().onBeforeRemove(
                                    entry.key(), old);

                                if (ctx.cancelRemove(interceptorRes))
                                    continue;
                            }

                            // Update previous batch.
                            if (putMap != null) {
                                err = updatePartialBatch(
                                    filtered,
                                    ver,
                                    putMap,
                                    null,
                                    expiryPlc,
                                    err,
                                    subjId,
                                    taskName);

                                putMap = null;

                                filtered = new ArrayList<>();
                            }

                            // Start collecting new batch.
                            if (rmvKeys == null)
                                rmvKeys = new ArrayList<>(size);

                            rmvKeys.add(entry.key());
                        }
                        else {
                            if (intercept) {
                                updated = (V)ctx.config().getInterceptor().onBeforePut(entry.key(), old, updated);

                                if (updated == null)
                                    continue;
                            }

                            // Update previous batch.
                            if (rmvKeys != null) {
                                err = updatePartialBatch(
                                    filtered,
                                    ver,
                                    null,
                                    rmvKeys,
                                    expiryPlc,
                                    err,
                                    subjId,
                                    taskName);

                                rmvKeys = null;

                                filtered = new ArrayList<>();
                            }

                            if (putMap == null)
                                putMap = new LinkedHashMap<>(size, 1.0f);

                            putMap.put(entry.key(), ctx.<V>unwrapTemporary(updated));
                        }
                    }
                    else if (op == UPDATE) {
                        if (intercept) {
                            V old = entry.innerGet(null,
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
                                CU.<K, V>empty(),
                                null);

                            val = ctx.config().getInterceptor().onBeforePut(entry.key(), old, val);

                            if (val == null)
                                continue;

                            val = ctx.unwrapTemporary(val);
                        }

                        if (putMap == null)
                            putMap = new LinkedHashMap<>(size, 1.0f);

                        putMap.put(entry.key(), (V)val);
                    }
                    else {
                        assert op == DELETE;

                        if (intercept) {
                            V old = entry.innerGet(null,
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
                                CU.<K, V>empty(),
                                null);

                            IgniteBiTuple<Boolean, ?> interceptorRes = ctx.config().getInterceptor().onBeforeRemove(
                                entry.key(), old);

                            if (ctx.cancelRemove(interceptorRes))
                                continue;
                        }

                        if (rmvKeys == null)
                            rmvKeys = new ArrayList<>(size);

                        rmvKeys.add(entry.key());
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
     * @param putMap Values to put.
     * @param rmvKeys Keys to remove.
     * @param expiryPlc Expiry policy.
     * @param err Optional partial update exception.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @return Partial update exception.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ForLoopReplaceableByForEach"})
    @Nullable private GridCachePartialUpdateException updatePartialBatch(
        List<GridCacheEntryEx<K, V>> entries,
        final GridCacheVersion ver,
        @Nullable Map<K, V> putMap,
        @Nullable Collection<K> rmvKeys,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable GridCachePartialUpdateException err,
        UUID subjId,
        String taskName
    ) {
        assert putMap == null ^ rmvKeys == null;
        GridCacheOperation op;

        CacheStorePartialUpdateException storeErr = null;

        try {
            if (putMap != null) {
                try {
                    ctx.store().putAllToStore(null, F.viewReadOnly(putMap, new C1<V, IgniteBiTuple<V, GridCacheVersion>>() {
                        @Override public IgniteBiTuple<V, GridCacheVersion> apply(V v) {
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
                    ctx.store().removeAllFromStore(null, rmvKeys);
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
            GridCacheEntryEx<K, V> entry = entries.get(i);

            assert Thread.holdsLock(entry);

            if (entry.obsolete() || (storeErr != null && storeErr.failedKeys().contains(entry.key())))
                continue;

            try {
                // We are holding java-level locks on entries at this point.
                V writeVal = op == UPDATE ? putMap.get(entry.key()) : null;

                assert writeVal != null || op == DELETE : "null write value found.";

                GridTuple3<Boolean, V, EntryProcessorResult<Object>> t = entry.innerUpdateLocal(
                    ver,
                    op,
                    writeVal,
                    null,
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
                        ctx.config().getInterceptor().onAfterPut(entry.key(), writeVal);
                    else
                        ctx.config().getInterceptor().onAfterRemove(entry.key(), t.get2());
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
    private List<GridCacheEntryEx<K, V>> lockEntries(Collection<? extends K> keys) {
        List<GridCacheEntryEx<K, V>> locked = new ArrayList<>(keys.size());

        while (true) {
            for (K key : keys) {
                if (key == null)
                    continue;

                GridCacheEntryEx<K, V> entry = entryEx(key);

                locked.add(entry);
            }

            for (int i = 0; i < locked.size(); i++) {
                GridCacheEntryEx<K, V> entry = locked.get(i);

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
    }

    /**
     * Releases java-level locks on cache entries.
     *
     * @param locked Locked entries.
     */
    private void unlockEntries(Iterable<GridCacheEntryEx<K, V>> locked) {
        for (GridCacheEntryEx<K, V> entry : locked)
            UNSAFE.monitorExit(entry);

        long topVer = ctx.affinity().affinityTopologyVersion();

        for (GridCacheEntryEx<K, V> entry : locked)
            ctx.evicts().touch(entry, topVer);
    }

    /**
     * @return New partial update exception.
     */
    private static GridCachePartialUpdateException partialUpdateException() {
        return new GridCachePartialUpdateException("Failed to update keys (retry update if possible).");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> txLockAsync(Collection<? extends K> keys,
        long timeout,
        IgniteTxLocalEx<K, V> tx,
        boolean isRead,
        boolean retval,
        IgniteTxIsolation isolation,
        boolean invalidate,
        long accessTtl,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return new GridFinishedFutureEx<>(new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys,
        long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return new GridFinishedFutureEx<>(new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        throw new UnsupportedOperationException("Locks are not supported for " +
            "CacheAtomicityMode.ATOMIC mode (use CacheAtomicityMode.TRANSACTIONAL instead)");
    }

    /**
     * @return Expiry policy.
     */
    @Nullable private ExpiryPolicy expiryPerCall() {
        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        ExpiryPolicy expiry = prj != null ? prj.expiry() : null;

        if (expiry == null)
            expiry = ctx.expiry();

        return expiry;
    }

    /**
     * @param op Operation closure.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected IgniteFuture asyncOp(final Callable<?> op) {
        IgniteFuture fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                IgniteFuture f = new GridEmbeddedFuture(fut,
                    new C2<Object, Exception, IgniteFuture>() {
                        @Override public IgniteFuture apply(Object t, Exception e) {
                            return ctx.closures().callLocalSafe(op);
                        }
                    }, ctx.kernalContext());

                saveFuture(holder, f);

                return f;
            }

            IgniteFuture f = ctx.closures().callLocalSafe(op);

            saveFuture(holder, f);

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }
}

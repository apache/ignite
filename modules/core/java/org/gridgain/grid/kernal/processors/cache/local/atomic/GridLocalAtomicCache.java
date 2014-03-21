/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local.atomic;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheFlag.CLONE;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Non-transactional local cache.
 */
public class GridLocalAtomicCache<K, V> extends GridCacheAdapter<K, V> {
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
                    @Override public GridCacheEntry<K, V> wrapFilterLocked() throws GridException {
                        assert Thread.holdsLock(this);

                        return new GridCacheFilterEvaluationEntry<>(key, rawGetOrUnmarshal(), this);
                    }
                };
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
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
    @Override public GridFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        final boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        return asyncOp(new CO<GridFuture>() {
            @Override public GridFuture<Map<K, V>> apply() {
                return ctx.closures().callLocalSafe(new Callable<Map<K, V>>() {
                    @Override public Map<K, V> call() throws Exception {
                        return getAllInternal(keys, filter);
                    }
                });
            }
        });
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V put(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        A.notNull(key, "key", val, "val");

        return (V)updateAllInternal(UPDATE, Collections.singleton(key), Collections.singleton(val),
            ttl, true, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return (Boolean)updateAllInternal(UPDATE, Collections.singleton(key), Collections.singleton(val),
            ttl, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key,
        V val,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        return (Boolean)updateAllInternal(UPDATE, Collections.singleton(key), Collections.singleton(val),
            -1, false, false, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<V> putAsync(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return updateAllAsync0(F0.asMap(key, val), null, true, false, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<Boolean> putxAsync(K key,
        V val,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return updateAllAsync0(F0.asMap(key, val), null, false, false, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V putIfAbsent(K key, V val) throws GridException {
        return put(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws GridException {
        return putx(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return putxAsync(key, val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V replace(K key, V val) throws GridException {
        return put(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<V> replaceAsync(K key, V val) {
        return putAsync(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws GridException {
        return putx(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replacexAsync(K key, V val) {
        return putxAsync(key, val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws GridException {
        return putx(key, newVal, ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return putxAsync(key, newVal, ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws GridException {
        return (GridCacheReturn<V>)updateAllInternal(UPDATE, Collections.singleton(key), Collections.singleton(newVal),
            0, true, true, ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheReturn<V> removex(K key, V val) throws GridException {
        return (GridCacheReturn<V>)updateAllInternal(DELETE, Collections.singleton(key), null, 0, true, true,
            ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<GridCacheReturn<V>> removexAsync(K key, V val) {
        return removeAllAsync0(F.asList(key), null, null, true, true, ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal) {
        return updateAllAsync0(F.asMap(key, newVal), null, true, true, null, 0,
            ctx.equalsPeekArray(oldVal));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        updateAllInternal(UPDATE, m.keySet(), m.values(), 0, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return updateAllAsync0(m, null, false, false, null, 0, filter);
    }

    /** {@inheritDoc} */
    @Override public void transform(K key, GridClosure<V, V> transformer) throws GridException {
        updateAllInternal(TRANSFORM, Collections.singleton(key), Collections.singleton(transformer), -1, false, false,
            null);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAsync(K key,
        GridClosure<V, V> transformer,
        @Nullable GridCacheEntryEx<K, V> entry,
        long ttl) {
        return updateAllAsync0(null, Collections.singletonMap(key, transformer), false, false, entry, ttl, null);
    }

    /** {@inheritDoc} */
    @Override public void transformAll(@Nullable Map<? extends K, ? extends GridClosure<V, V>> m) throws GridException {
        if (F.isEmpty(m))
            return;

        updateAllInternal(TRANSFORM, m.keySet(), m.values(), 0, false, false, null);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> transformAllAsync(@Nullable Map<? extends K, ? extends GridClosure<V, V>> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<Object>(ctx.kernalContext());

        return updateAllAsync0(null, m, false, false, null, 0, null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public V remove(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return (V)updateAllInternal(DELETE, Collections.singleton(key), null, 0, true, false, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<V> removeAsync(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return removeAllAsync0(Collections.singletonList(key), null, entry, true, false, filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        updateAllInternal(DELETE, keys, null, 0, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllAsync(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return removeAllAsync0(keys, null, null, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        return (Boolean)updateAllInternal(DELETE, Collections.singleton(key), null, 0, false, false, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridFuture<Boolean> removexAsync(K key,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return removeAllAsync0(Collections.singletonList(key), null, entry, false, false, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws GridException {
        return (Boolean)updateAllInternal(DELETE, Collections.singleton(key), null, 0, false, false,
            ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> removeAsync(K key, V val) {
        return removexAsync(key, ctx.equalsPeekArray(val));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        removeAll(keySet(filter));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> removeAllAsync(GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return removeAllAsync(keySet(filter), filter);
    }

    /**
     * Entry point to all public API get methods.
     *
     * @param keys Keys to remove.
     * @param filter Filter.
     * @return Get future.
     */
    @SuppressWarnings("ConstantConditions")
    private Map<K, V> getAllInternal(@Nullable Collection<? extends K> keys,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        Map<K, V> vals = new HashMap<>(keys.size(), 1.0f);

        boolean swapOrOffheap = ctx.isSwapOrOffheapEnabled();

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
                            /*read-through*/true,
                            /*fail-fast*/false,
                            /*unmarshal*/true,
                            /**update-metrics*/true,
                            /**event*/true,
                            filter);

                        if (v != null)
                            vals.put(key, v);
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
                        ctx.evicts().touch(entry);
                }
            }
        }

        if (!ctx.hasFlag(CLONE))
            return vals;

        Map<K, V> map = new GridLeanMap<>();

        for (Map.Entry<K, V> e : vals.entrySet())
            map.put(e.getKey(), ctx.cloneValue(e.getValue()));

        return map;
    }

    /**
     * Entry point for public API put/transform methods.
     *
     * @param map Put map. Either {@code map}, {@code transformMap} or {@code drMap} should be passed.
     * @param transformMap Transform map. Either {@code map}, {@code transformMap} or {@code drMap} should be passed.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param cached Cached cache entry for key. May be passed if and only if map size is {@code 1}.
     * @param ttl Entry time-to-live.
     * @param filter Cache entry filter for atomic updates.
     * @return Completion future.
     */
    private GridFuture updateAllAsync0(
        @Nullable final Map<? extends K, ? extends V> map,
        @Nullable final Map<? extends K, ? extends GridClosure<V, V>> transformMap,
        final boolean retval,
        final boolean rawRetval,
        @Nullable GridCacheEntryEx<K, V> cached,
        final long ttl,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        final GridCacheOperation op = transformMap != null ? TRANSFORM : UPDATE;
        final Collection<? extends K> keys = map != null ? map.keySet() : transformMap != null ? transformMap.keySet() : null;
        final Collection<?> vals = map != null ? map.values() : transformMap != null ? transformMap.values() : null;

        return asyncOp(new CO<GridFuture>() {
            @Override public GridFuture apply() {
                return ctx.closures().callLocalSafe(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return updateAllInternal(op, keys, vals, ttl, retval, rawRetval, filter);
                    }
                });
            }
        });
    }

    /**
     * Entry point for public API remove methods.
     *
     * @param keys Keys to remove.
     * @param drMap DR map.
     * @param cached Cached cache entry for key. May be passed if and only if keys size is {@code 1}.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter.
     * @return Completion future.
     */
    private GridFuture removeAllAsync0(
        @Nullable final Collection<? extends K> keys,
        @Nullable final Map<? extends K, GridCacheVersion> drMap,
        @Nullable GridCacheEntryEx<K, V> cached,
        final boolean retval,
        final boolean rawRetval,
        final @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        return asyncOp(new CO<GridFuture>() {
            @Override
            public GridFuture apply() {
                return ctx.closures().callLocalSafe(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        return updateAllInternal(DELETE, keys, null, 0, retval, rawRetval, filter);
                    }
                });
            }
        });
    }

    /**
     * Entry point for all public update methods (put, remove, transform).
     *
     * @param op Operation.
     * @param keys Keys.
     * @param vals Values.
     * @param ttl Time to live.
     * @param retval Return value required flag.
     * @param rawRetval Return {@code GridCacheReturn} instance.
     * @param filter Cache entry filter.
     * @return Update result.
     */
    private Object updateAllInternal(GridCacheOperation op,
        Collection<? extends K> keys,
        @Nullable Collection<?> vals,
        long ttl,
        boolean retval,
        boolean rawRetval,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        Iterator<?> valsIter = vals != null ? vals.iterator() : null;

        GridBiTuple<Boolean, V> res = null;

        GridCachePartialUpdateException err = null;

        GridCacheVersion ver = ctx.versions().next();

        for (K key : keys) {
            Object val = valsIter != null ? valsIter.next() : null;

            if (key == null)
                continue;

            while (true) {
                GridCacheEntryEx<K, V> entry = null;

                try {
                    entry = entryEx(key);

                    GridBiTuple<Boolean, V> t = entry.localUpdate(
                        ver,
                        val == null ? DELETE : op,
                        val,
                        storeEnabled(),
                        retval,
                        ttl,
                        true,
                        true,
                        filter);

                    if (res == null)
                        res = t;

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while updating (will retry): " + key);

                    entry = null;
                }
                catch (GridException e) {
                    if (err == null)
                        err = new GridCachePartialUpdateException("Failed to update keys " +
                            "(retry update if possible).");

                    err.add(F.asList(key), e);

                    U.error(log, "Failed to update key : " + key, e);

                    break;
                }
                finally {
                    if (entry != null)
                        ctx.evicts().touch(entry);
                }
            }
        }

        if (err != null)
            throw err;

        return res == null ? null : rawRetval ?
            new GridCacheReturn<>(res.get2(), res.get1()) : retval ? res.get2() : res.get1();
    }

    /**
     * @return {@code True} if store enabled.
     */
    private boolean storeEnabled() {
        return ctx.isStoreEnabled() && ctx.config().getStore() != null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> txLockAsync(Collection<? extends K> keys,
        long timeout,
        GridCacheTxLocalEx<K, V> tx,
        boolean isRead,
        boolean retval,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return new GridFinishedFutureEx<>(new UnsupportedOperationException("Locks are not supported for " +
            "GridCacheAtomicityMode.ATOMIC mode (use GridCacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean syncCommit,
        boolean syncRollback,
        boolean swapEnabled,
        boolean storeEnabled,
        int txSize,
        @Nullable Object grpLockKey,
        boolean partLock) {
        throw new UnsupportedOperationException("Transactions are not supported for " +
            "GridCacheAtomicityMode.ATOMIC mode (use GridCacheAtomicityMode.TRANSACTIONAL instead)");
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys,
        long timeout,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) {
        return new GridFinishedFutureEx<>(new UnsupportedOperationException("Locks are not supported for " +
            "GridCacheAtomicityMode.ATOMIC mode (use GridCacheAtomicityMode.TRANSACTIONAL instead)"));
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        throw new UnsupportedOperationException("Locks are not supported for " +
            "GridCacheAtomicityMode.ATOMIC mode (use GridCacheAtomicityMode.TRANSACTIONAL instead)");
    }

    /**
     * @param op Operation closure.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected GridFuture asyncOp(final CO<GridFuture> op) {
        GridFuture fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            GridFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                GridFuture f = new GridEmbeddedFuture(fut,
                    new C2<Object, Exception, GridFuture>() {
                        @Override public GridFuture apply(Object t, Exception e) {
                            return op.apply();
                        }
                    }, ctx.kernalContext());

                saveFuture(holder, f);

                return f;
            }

            GridFuture f = op.apply();

            saveFuture(holder, f);

            return f;
        }
        finally {
            holder.unlock();
        }
    }
}

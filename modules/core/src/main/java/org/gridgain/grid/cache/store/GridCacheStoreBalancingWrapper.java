/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Cache store wrapper that ensures that there will be no more that one thread loading value from underlying store.
 */
public class GridCacheStoreBalancingWrapper<K, V> implements GridCacheStore<K, V> {
    /** */
    public static final int DFLT_LOAD_ALL_THRESHOLD = 5;

    /** Delegate store. */
    private GridCacheStore<K, V> delegate;

    /** Pending cache store loads. */
    private ConcurrentMap<K, LoadFuture> pendingLoads = new ConcurrentHashMap8<>();

    /** Load all threshold. */
    private int loadAllThreshold = DFLT_LOAD_ALL_THRESHOLD;

    /**
     * @param delegate Delegate store.
     */
    public GridCacheStoreBalancingWrapper(GridCacheStore<K, V> delegate) {
        this.delegate = delegate;
    }

    /**
     * @param delegate Delegate store.
     * @param loadAllThreshold Load all threshold.
     */
    public GridCacheStoreBalancingWrapper(GridCacheStore<K, V> delegate, int loadAllThreshold) {
        this.delegate = delegate;
        this.loadAllThreshold = loadAllThreshold;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(@Nullable GridCacheTx tx, K key) throws IgniteCheckedException {
        LoadFuture fut = pendingLoads.get(key);

        if (fut != null)
            return fut.get(key);

        fut = new LoadFuture();

        LoadFuture old = pendingLoads.putIfAbsent(key, fut);

        if (old != null)
            return old.get(key);

        try {
            V val = delegate.load(tx, key);

            fut.onComplete(key, val);

            return val;
        }
        catch (Throwable e) {
            fut.onError(key, e);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws IgniteCheckedException {
        delegate.loadCache(clo, args);
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable GridCacheTx tx, Collection<? extends K> keys, final IgniteBiInClosure<K, V> c)
        throws IgniteCheckedException {
        if (keys.size() > loadAllThreshold) {
            delegate.loadAll(tx, keys, c);

            return;
        }

        Collection<K> needLoad = null;
        Map<K, LoadFuture> pending = null;
        LoadFuture span = null;

        for (K key : keys) {
            LoadFuture fut = pendingLoads.get(key);

            if (fut != null) {
                if (pending == null)
                    pending = new HashMap<>();

                pending.put(key, fut);
            }
            else {
                // Try to concurrently add pending future.
                if (span == null)
                    span = new LoadFuture();

                LoadFuture old = pendingLoads.putIfAbsent(key, span);

                if (old != null) {
                    if (pending == null)
                        pending = new HashMap<>();

                    pending.put(key, old);
                }
                else {
                    if (needLoad == null)
                        needLoad = new ArrayList<>(keys.size());

                    needLoad.add(key);
                }
            }
        }

        if (needLoad != null) {
            assert !needLoad.isEmpty();
            assert span != null;

            final ConcurrentMap<K, V> loaded = new ConcurrentHashMap8<>();

            try {
                delegate.loadAll(tx, needLoad, new CI2<K, V>() {
                    @Override public void apply(K k, V v) {
                        if (v != null) {
                            loaded.put(k, v);

                            c.apply(k, v);
                        }
                    }
                });

                span.onComplete(needLoad, loaded);
            }
            catch (Throwable e) {
                span.onError(needLoad, e);

                throw e;
            }
        }

        if (pending != null) {
            for (Map.Entry<K, LoadFuture> e : pending.entrySet()) {
                K key = e.getKey();

                c.apply(key, e.getValue().get(key));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, K key, V val) throws IgniteCheckedException {
        delegate.put(tx, key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable GridCacheTx tx, Map<? extends K, ? extends V> map) throws IgniteCheckedException {
        delegate.putAll(tx, map);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, K key) throws IgniteCheckedException {
        delegate.remove(tx, key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable GridCacheTx tx, Collection<? extends K> keys) throws IgniteCheckedException {
        delegate.removeAll(tx, keys);
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws IgniteCheckedException {
        delegate.txEnd(tx, commit);
    }

    /**
     *
     */
    private class LoadFuture extends GridFutureAdapter<Map<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Collection of keys for pending cleanup. */
        private volatile Collection<K> keys;

        /**
         *
         */
        public LoadFuture() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Map<K, V> res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                assert keys != null;

                for (K key : keys)
                    pendingLoads.remove(key, this);

                return true;
            }

            return false;
        }

        /**
         * @param key Key.
         * @param val Loaded value.
         */
        public void onComplete(K key, V val) {
            onComplete(Collections.singletonList(key), F.asMap(key, val));
        }

        /**
         * @param keys Keys.
         * @param res Loaded values.
         */
        public void onComplete(Collection<K> keys, Map<K, V> res) {
            this.keys = keys;

            onDone(res);
        }

        /**
         * @param key Key.
         * @param err Error.
         */
        public void onError(K key, Throwable err) {

        }

        /**
         * @param keys Keys.
         * @param err Error.
         */
        public void onError(Collection<K> keys, Throwable err) {
            this.keys = keys;

            onDone(err);
        }

        /**
         * Gets value loaded for key k.
         *
         * @param key Key to load.
         * @return Loaded value (possibly {@code null}).
         * @throws IgniteCheckedException If load failed.
         */
        public V get(K key) throws IgniteCheckedException {
            return get().get(key);
        }
    }
}

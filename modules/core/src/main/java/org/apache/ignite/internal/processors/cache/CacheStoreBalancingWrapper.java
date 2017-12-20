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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Cache store wrapper that ensures that there will be no more that one thread loading value from underlying store.
 */
public class CacheStoreBalancingWrapper<K, V> implements CacheStore<K, V> {
    /** */
    public static final int DFLT_LOAD_ALL_THRESHOLD = CacheConfiguration.DFLT_CONCURRENT_LOAD_ALL_THRESHOLD;

    /** Delegate store. */
    private CacheStore<K, V> delegate;

    /** Pending cache store loads. */
    private ConcurrentMap<K, LoadFuture> pendingLoads = new ConcurrentHashMap8<>();

    /** Load all threshold. */
    private int loadAllThreshold = DFLT_LOAD_ALL_THRESHOLD;

    /**
     * @param delegate Delegate store.
     */
    public CacheStoreBalancingWrapper(CacheStore<K, V> delegate) {
        this.delegate = delegate;
    }

    /**
     * @param delegate Delegate store.
     * @param loadAllThreshold Load all threshold.
     */
    public CacheStoreBalancingWrapper(CacheStore<K, V> delegate, int loadAllThreshold) {
        this.delegate = delegate;
        this.loadAllThreshold = loadAllThreshold;
    }

    /**
     * @return Load all threshold.
     */
    public int loadAllThreshold() {
        return loadAllThreshold;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(K key) {
        LoadFuture fut = pendingLoads.get(key);

        try {
            if (fut != null)
                return fut.get(key);

            fut = new LoadFuture();

            LoadFuture old = pendingLoads.putIfAbsent(key, fut);

            if (old != null)
                return old.get(key);
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException(e);
        }

        try {
            V val = delegate.load(key);

            fut.onComplete(key, val);

            return val;
        }
        catch (Throwable e) {
            fut.onError(key, e);

            if (e instanceof Error)
                throw e;

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) {
        delegate.loadCache(clo, args);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        assert false;

        return delegate.loadAll(keys);
    }

    /**
     * @param keys Keys to load.
     * @param c Closure for loaded values.
     */
    public void loadAll(Collection<? extends K> keys, final IgniteBiInClosure<K, V> c) {
        assert keys.size() <= loadAllThreshold : loadAllThreshold;

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

            try {
                Map<K, V> loaded = delegate.loadAll(needLoad);

                if (loaded != null) {
                    for (Map.Entry<K, V> e : loaded.entrySet())
                        c.apply(e.getKey(), e.getValue());
                }

                span.onComplete(needLoad, loaded);
            }
            catch (Throwable e) {
                span.onError(needLoad, e);

                if (e instanceof Error)
                    throw e;

                throw e;
            }
        }

        if (pending != null) {
            try {
                for (Map.Entry<K, LoadFuture> e : pending.entrySet()) {
                    K key = e.getKey();

                    c.apply(key, e.getValue().get(key));
                }
            }
            catch (IgniteCheckedException e) {
                throw new CacheLoaderException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) {
        delegate.write(entry);
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        delegate.writeAll(entries);
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        delegate.delete(key);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        delegate.deleteAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        delegate.sessionEnd(commit);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStoreBalancingWrapper.class, this);
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
            this.keys = Collections.singletonList(key);

            onDone(err);
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

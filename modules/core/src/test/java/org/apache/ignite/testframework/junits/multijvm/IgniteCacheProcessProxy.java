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

package org.apache.ignite.testframework.junits.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.CacheManager;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Ignite cache proxy for ignite instance at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteCacheProcessProxy<K, V> implements IgniteCache<K, V> {
    /** Compute. */
    private final transient IgniteCompute compute;

    /** Cache name. */
    private final String cacheName;

    /** Grid id. */
    private final UUID gridId;

    /** With async. */
    private final boolean isAsync;

    /** Ignite proxy. */
    private final transient IgniteProcessProxy igniteProxy;

    /**
     * @param name Name.
     * @param proxy Ignite Process Proxy.
     */
    public IgniteCacheProcessProxy(String name, IgniteProcessProxy proxy) {
        this(name, false, proxy);
    }

    /**
     * @param name Name.
     * @param async
     * @param proxy Ignite Process Proxy.
     */
    public IgniteCacheProcessProxy(String name, boolean async, IgniteProcessProxy proxy) {
        cacheName = name;
        isAsync = async;
        gridId = proxy.getId();
        igniteProxy = proxy;
        compute = proxy.remoteCompute();
    }

    /**
     * Returns cache instance. Method to be called from closure at another JVM.
     *
     * @return Cache.
     */
    private IgniteCache<Object, Object> cache() {
        IgniteCache cache = Ignition.ignite(gridId).cache(cacheName);

        if (isAsync)
            cache = cache.withAsync();

        return cache;
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync() {
        return new IgniteCacheProcessProxy<>(cacheName, true, igniteProxy);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return isAsync;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        // Return fake future. Future should be called in the same place where operation done.
        return new IgniteFinishedFutureImpl<>();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(final Class<C> clazz) {
        final Class cl = clazz;

        return (C)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getConfiguration(cl);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Entry<K, V> randomEntry() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    @Override public IgniteCache<K, V> withNoRetries() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepPortable() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable final IgniteBiPredicate<K, V> p, @Nullable final Object... args) throws CacheException {
        final IgniteBiPredicate pCopy = p;

        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().localLoadCache(pCopy, args);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(final K key, final V val) throws CacheException {
        return (V)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getAndPutIfAbsent(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Collection<? extends K> keys) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(final K key, final boolean byCurrThread) {
        return compute.call(new IgniteCallable<Boolean>() {
            @Override public Boolean call() throws Exception {
                return cache().isLocalLocked(key, byCurrThread);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterable<Entry<K, V>> localEntries(final CachePeekMode... peekModes) throws CacheException {
        return (Iterable<Entry<K, V>>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                Collection<Entry> res = new ArrayList<>();

                for (Entry e : cache().localEntries(peekModes))
                    res.add(e);

                return res;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void localEvict(final Collection<? extends K> keys) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().localEvict(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V localPeek(final K key, final CachePeekMode... peekModes) {
        return (V)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().localPeek(key, peekModes);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public int size(final CachePeekMode... peekModes) throws CacheException {
        return (int)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().size(peekModes);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int localSize(final CachePeekMode... peekModes) {
        return (int)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().localSize(peekModes);
            }
        });
    }

    /** {@inheritDoc} */
    @Override  public <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public V get(final K key) {
        return (V)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().get(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(final Set<? extends K> keys) {
        return (Map<K, V>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getAll(keys);
            }
        });
    }

    @Override public Map<K, V> getAllOutTx(final Set<? extends K> keys) {
        return (Map<K, V>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getAllOutTx(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(final K key) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().containsKey(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override  public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionLsnr) {
        throw new UnsupportedOperationException("Oparetion can't be supported automatically.");
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(final Set<? extends K> keys) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().containsKeys(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void put(final K key, final V val) {;
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().put(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(final K key, final V val) {
        return (V)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getAndPut(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void putAll(final Map<? extends K, ? extends V> map) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().putAll(map);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(final K key, final V val) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().putIfAbsent(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final K key) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().remove(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final K key, final V oldVal) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().remove(key, oldVal);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(final K key) {
        return (V)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getAndRemove(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean replace(final K key, final V oldVal, final V newVal) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().replace(key, oldVal, newVal);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean replace(final K key, final V val) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().replace(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(final K key, final V val) {
        return (V)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getAndReplace(key, val);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void removeAll(final Set<? extends K> keys) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().removeAll(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                IgniteCache<Object, Object> cache = cache();

                cache.removeAll();

                if (isAsync)
                    cache.future().get();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().clear();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void clear(final K key) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().clear(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void clearAll(final Set<? extends K> keys) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().clearAll(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void localClear(final K key) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().localClear(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(final Set<? extends K> keys) {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().localClearAll(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(final K key, final EntryProcessor<K, V, T> entryProcessor, final Object... arguments) {
        return (T)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().invoke(key,
                    (EntryProcessor<Object, Object, Object>)entryProcessor, arguments);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(final K key, final CacheEntryProcessor<K, V, T> entryProcessor, final Object... arguments) {
        return (T)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().invoke(key,
                    (CacheEntryProcessor<Object, Object, Object>)entryProcessor, arguments);
            }
        });
    }

    /** {@inheritDoc} */
    @Override  public <T> Map<K, EntryProcessorResult<T>> invokeAll(final Set<? extends K> keys, final EntryProcessor<K, V, T> entryProcessor,
        final Object... args) {
        return (Map<K, EntryProcessorResult<T>>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().invokeAll(keys,
                    (EntryProcessor<Object, Object, Object>)entryProcessor, args);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return (String)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().getName();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().close();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        compute.run(new IgniteRunnable() {
            @Override public void run() {
                cache().destroy();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return cache().isClosed();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(final Class<T> clazz) {
        if (Ignite.class.equals(clazz))
            return (T)igniteProxy;

        try {
            return (T)compute.call(new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    return cache().unwrap(clazz);
                }
            });
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Looks like class " + clazz + " is unmarshallable. Exception type:" + e.getClass(), e);
        }
    }

    /** {@inheritDoc} */
    @Override  public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override  public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        final Collection<Entry<K, V>> col = (Collection<Entry<K, V>>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                Collection res = new ArrayList();

                for (Object o : cache())
                    res.add(o);

                return res;
            }
        });

        return col.iterator();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebalance() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        throw new UnsupportedOperationException("Method should be supported.");
    }
}

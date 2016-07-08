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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite cache proxy for ignite instance at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteCacheProcessProxy<K, V> implements IgniteCache<K, V> {
    /** Compute. */
    private final transient IgniteCompute compute;

    /** Cache name. */
    private final String cacheName;

    /** With async. */
    private final boolean isAsync;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Ignite proxy. */
    private final transient IgniteProcessProxy igniteProxy;

    /**
     * @param name Name.
     * @param proxy Ignite Process Proxy.
     */
    public IgniteCacheProcessProxy(String name, IgniteProcessProxy proxy) {
        this(name, false, null, proxy);
    }

    /**
     * @param name Name.
     * @param async Async flag.
     * @param plc Expiry policy.
     * @param proxy Ignite Process Proxy.
     */
    private IgniteCacheProcessProxy(String name, boolean async, ExpiryPolicy plc, IgniteProcessProxy proxy) {
        cacheName = name;
        isAsync = async;
        expiryPlc = plc;
        igniteProxy = proxy;
        compute = proxy.remoteCompute();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync() {
        return new IgniteCacheProcessProxy<>(cacheName, true, null, igniteProxy);
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
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return compute.call(new GetConfigurationTask<>(cacheName, isAsync, clazz));
    }

    /** {@inheritDoc} */
    @Override public Entry<K, V> randomEntry() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        return new IgniteCacheProcessProxy<>(cacheName, isAsync, plc, igniteProxy);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withNoRetries() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws CacheException {
        compute.call(new LocalLoadCacheTask<>(cacheName, isAsync, p, args));
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        return compute.call(new GetAndPutIfAbsentTask<>(cacheName, isAsync, key, val));
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
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        return compute.call(new IsLocalLockedTask<>(cacheName, isAsync, key, byCurrThread));
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        return compute.call(new LocalEntriesTask<K, V>(cacheName, isAsync, peekModes));
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        compute.call(new LocalEvictTask<>(cacheName, isAsync, keys));
    }

    /** {@inheritDoc} */
    @Override public V localPeek(K key, CachePeekMode... peekModes) {
        return compute.call(new LocalPeekTask<K, V>(cacheName, isAsync, key, peekModes));
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        return compute.call(new SizeTask(cacheName, isAsync, peekModes, false));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        return compute.call(new SizeLongTask(cacheName, isAsync, peekModes, false));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode... peekModes) throws CacheException {
        return compute.call(new PartitionSizeLongTask(cacheName, isAsync, peekModes, partition, false));
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        return compute.call(new SizeTask(cacheName, isAsync, peekModes, true));
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        return compute.call(new SizeLongTask(cacheName, isAsync, peekModes, true));
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode... peekModes) {
        return compute.call(new PartitionSizeLongTask(cacheName, isAsync, peekModes, partition, true));
    }

    /** {@inheritDoc} */
    @Override  public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args)
    {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        return compute.call(new GetTask<K, V>(cacheName, isAsync, key));
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) {
        return compute.call(new GetEntryTask<K, V>(cacheName, isAsync, key));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        return compute.call(new GetAllTask<K, V>(cacheName, isAsync, keys));
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) {
        return compute.call(new GetEntriesTask<K, V>(cacheName, isAsync, keys));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        return compute.call(new GetAllOutTxTask<K, V>(cacheName, isAsync, keys));
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return compute.call(new ContainsKeyTask<>(cacheName, isAsync, key));
    }

    /** {@inheritDoc} */
    @Override  public void loadAll(Set<? extends K> keys, boolean replaceExistVals, CompletionListener completionLsnr) {
        throw new UnsupportedOperationException("Oparetion can't be supported automatically.");
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        return compute.call(new ContainsKeysTask<>(cacheName, isAsync, keys));
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        compute.call(new PutTask<>(cacheName, isAsync, expiryPlc, key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return compute.call(new GetAndPutTask<>(cacheName, isAsync, key, val));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        compute.call(new PutAllTask<>(cacheName, isAsync, map));
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return compute.call(new PutIfAbsentTask<>(cacheName, isAsync, key, val));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return compute.call(new RemoveTask<>(cacheName, isAsync, key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        return compute.call(new RemoveIfExistsTask<>(cacheName, isAsync, key, oldVal));
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return compute.call(new GetAndRemoveTask<K, V>(cacheName, isAsync, key));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return compute.call(new ReplaceIfExistsTask<>(cacheName, isAsync, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return compute.call(new ReplaceTask<>(cacheName, isAsync, key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return compute.call(new GetAndReplaceTask<>(cacheName, isAsync, key, val));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        compute.call(new RemoveAllKeysTask<>(cacheName, isAsync, keys));
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        compute.call(new RemoveAllTask<K, V>(cacheName, isAsync));
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        compute.call(new ClearTask(cacheName, isAsync));
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        compute.call(new ClearKeyTask<>(cacheName, isAsync, false, key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        compute.call(new ClearAllKeys<>(cacheName, isAsync, false, keys));
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        compute.call(new ClearKeyTask<>(cacheName, isAsync, true, key));
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        compute.call(new ClearAllKeys<>(cacheName, isAsync, true, keys));
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> processor, Object... args) {
        return compute.call(new InvokeTask<>(cacheName, isAsync, key, processor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> processor, Object... args) {
        return compute.call(new InvokeTask<>(cacheName, isAsync, key, processor, args));
    }

    /** {@inheritDoc} */
    @Override  public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> processor,
        Object... args)
    {
        return compute.call(new InvokeAllTask<>(cacheName, isAsync, keys, processor, args));
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return compute.call(new GetNameTask(cacheName, isAsync));
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() {
        compute.call(new CloseTask(cacheName, isAsync));
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        compute.call(new DestroyTask(cacheName, isAsync));
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return compute.call(new IsClosedTask(cacheName, isAsync));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> clazz) {
        if (Ignite.class.equals(clazz))
            return (T)igniteProxy;

        try {
            return compute.call(new UnwrapTask<>(cacheName, isAsync, clazz));
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Looks like class " + clazz +
                " is unmarshallable. Exception type:" + e.getClass(), e);
        }
    }

    /** {@inheritDoc} */
    @Override  public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override  public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        return compute.call(new IteratorTask<K, V>(cacheName, isAsync)).iterator();
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

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /**
     *
     */
    private static class GetConfigurationTask<K, V, C extends Configuration<K, V>> extends CacheTaskAdapter<K, V, C> {
        /** Clazz. */
        private final Class<C> clazz;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param clazz Clazz.
         */
        public GetConfigurationTask(String cacheName, boolean async, Class<C> clazz) {
            super(cacheName, async, null);
            this.clazz = clazz;
        }

        /** {@inheritDoc} */
        @Override public C call() throws Exception {
            return cache().getConfiguration(clazz);
        }
    }

    /**
     *
     */
    private static class LocalLoadCacheTask<K, V> extends CacheTaskAdapter<K, V, Void> {
        /** Predicate. */
        private final IgniteBiPredicate<K, V> p;

        /** Args. */
        private final Object[] args;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param p P.
         * @param args Args.
         */
        public LocalLoadCacheTask(String cacheName, boolean async, IgniteBiPredicate<K, V> p, Object[] args) {
            super(cacheName, async, null);
            this.p = p;
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().localLoadCache(p, args);

            return null;
        }
    }

    /**
     *
     */
    private static class GetAndPutIfAbsentTask<K, V> extends CacheTaskAdapter<K, V, V> {
        /** Key. */
        private final K key;

        /** Value. */
        private final V val;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param val Value.
         */
        public GetAndPutIfAbsentTask(String cacheName, boolean async, K key, V val) {
            super(cacheName, async, null);
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public V call() throws Exception {
            return cache().getAndPutIfAbsent(key, val);
        }
    }

    /**
     *
     */
    private static class IsLocalLockedTask<K> extends CacheTaskAdapter<K, Void, Boolean> {
        /** Key. */
        private final K key;

        /** By current thread. */
        private final boolean byCurrThread;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param byCurrThread By current thread.
         */
        public IsLocalLockedTask(String cacheName, boolean async, K key, boolean byCurrThread) {
            super(cacheName, async, null);
            this.key = key;
            this.byCurrThread = byCurrThread;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().isLocalLocked(key, byCurrThread);
        }
    }

    /**
     *
     */
    private static class LocalEntriesTask<K, V> extends CacheTaskAdapter<K, V, Iterable<Entry<K, V>>> {
        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param peekModes Peek modes.
         */
        public LocalEntriesTask(String cacheName, boolean async, CachePeekMode[] peekModes) {
            super(cacheName, async, null);
            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Override public Iterable<Entry<K, V>> call() throws Exception {
            Collection<Entry<K, V>> res = new ArrayList<>();

            for (Entry<K, V> e : cache().localEntries(peekModes))
                res.add(new CacheEntryImpl<>(e.getKey(), e.getValue()));

            return res;
        }
    }

    /**
     *
     */
    private static class LocalEvictTask<K> extends CacheTaskAdapter<K, Void, Void> {
        /** Keys. */
        private final Collection<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public LocalEvictTask(String cacheName, boolean async, Collection<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().localEvict(keys);

            return null;
        }
    }

    /**
     *
     */
    private static class LocalPeekTask<K, V> extends CacheTaskAdapter<K, V, V> {
        /** Key. */
        private final K key;

        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param peekModes Peek modes.
         */
        public LocalPeekTask(String cacheName, boolean async, K key, CachePeekMode[] peekModes) {
            super(cacheName, async, null);
            this.key = key;
            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Override public V call() throws Exception {
            return cache().localPeek(key, peekModes);
        }
    }

    /**
     *
     */
    private static class SizeTask extends CacheTaskAdapter<Void, Void, Integer> {
        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /** Local. */
        private final boolean loc;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param peekModes Peek modes.
         * @param loc Local.
         */
        public SizeTask(String cacheName, boolean async, CachePeekMode[] peekModes, boolean loc) {
            super(cacheName, async, null);
            this.loc = loc;
            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return loc ? cache().localSize(peekModes) : cache().size(peekModes);
        }
    }

    /**
     *
     */
    private static class SizeLongTask extends CacheTaskAdapter<Void, Void, Long> {
        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /** Local. */
        private final boolean loc;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param peekModes Peek modes.
         * @param loc Local.
         */
        public SizeLongTask(String cacheName, boolean async, CachePeekMode[] peekModes, boolean loc) {
            super(cacheName, async, null);
            this.loc = loc;
            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Override public Long call() throws Exception {
            return loc ? cache().localSizeLong(peekModes) : cache().sizeLong(peekModes);
        }
    }

    /**
     *
     */
    private static class PartitionSizeLongTask extends CacheTaskAdapter<Void, Void, Long> {

        /** Partition. */
        int partition;

        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /** Local. */
        private final boolean loc;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param peekModes Peek modes.
         * @param partition partition.
         * @param loc Local.
         */
        public PartitionSizeLongTask(String cacheName, boolean async, CachePeekMode[] peekModes, int partition, boolean loc) {
            super(cacheName, async, null);
            this.loc = loc;
            this.peekModes = peekModes;
            this.partition = partition;
        }

        /** {@inheritDoc} */
        @Override public Long call() throws Exception {
            return loc ? cache().localSizeLong(partition, peekModes) : cache().sizeLong(partition, peekModes);
        }
    }

    /**
     *
     */
    private static class GetTask<K, V> extends CacheTaskAdapter<K, V, V> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         */
        public GetTask(String cacheName, boolean async, K key) {
            super(cacheName, async, null);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public V call() throws Exception {
            return cache().get(key);
        }
    }

    /**
     *
     */
    private static class GetEntryTask<K, V> extends CacheTaskAdapter<K, V, CacheEntry<K, V>> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         */
        public GetEntryTask(String cacheName, boolean async, K key) {
            super(cacheName, async, null);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public CacheEntry<K, V> call() throws Exception {
            return cache().getEntry(key);
        }
    }

    /**
     *
     */
    private static class RemoveAllTask<K, V> extends CacheTaskAdapter<K, V, Void> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public RemoveAllTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            IgniteCache<K, V> cache = cache();

            cache.removeAll();

            if (async)
                cache.future().get();

            return null;
        }
    }

    /**
     *
     */
    private static class PutTask<K, V> extends CacheTaskAdapter<K, V, Void> {
        /** Key. */
        private final K key;

        /** Value. */
        private final V val;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param expiryPlc Expiry policy.
         * @param key Key.
         * @param val Value.
         */
        public PutTask(String cacheName, boolean async, ExpiryPolicy expiryPlc, K key, V val) {
            super(cacheName, async, expiryPlc);
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().put(key, val);

            return null;
        }
    }

    /**
     *
     */
    private static class ContainsKeyTask<K> extends CacheTaskAdapter<K, Object, Boolean> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         */
        public ContainsKeyTask(String cacheName, boolean async, K key) {
            super(cacheName, async, null);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().containsKey(key);
        }
    }

    /**
     *
     */
    private static class ClearTask extends CacheTaskAdapter<Object, Object, Void> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public ClearTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().clear();

            return null;
        }
    }

    /**
     *
     */
    private static class IteratorTask<K, V> extends CacheTaskAdapter<K, V, Collection<Entry<K, V>>> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public IteratorTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public Collection<Entry<K, V>> call() throws Exception {
            Collection<Entry<K, V>> res = new ArrayList<>();

            for (Entry<K, V> o : cache())
                res.add(o);

            return res;
        }
    }

    /**
     *
     */
    private static class ReplaceTask<K, V> extends CacheTaskAdapter<K, V, Boolean> {
        /** Key. */
        private final K key;

        /** Value. */
        private final V val;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param val Value.
         */
        public ReplaceTask(String cacheName, boolean async, K key, V val) {
            super(cacheName, async, null);
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().replace(key, val);
        }
    }

    /**
     *
     */
    private static class GetNameTask extends CacheTaskAdapter<Void, Void, String> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public GetNameTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public String call() throws Exception {
            return cache().getName();
        }
    }

    /**
     *
     */
    private static class RemoveTask<K> extends CacheTaskAdapter<K, Void, Boolean> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         */
        public RemoveTask(String cacheName, boolean async, K key) {
            super(cacheName, async, null);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().remove(key);
        }
    }

    /**
     *
     */
    private static class PutAllTask<K, V> extends CacheTaskAdapter<K, V, Void> {
        /** Map. */
        private final Map<? extends K, ? extends V> map;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param map Map.
         */
        public PutAllTask(String cacheName, boolean async, Map<? extends K, ? extends V> map) {
            super(cacheName, async, null);
            this.map = map;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().putAll(map);

            return null;
        }
    }

    /**
     *
     */
    private static class RemoveAllKeysTask<K> extends CacheTaskAdapter<K, Void, Void> {
        /** Keys. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public RemoveAllKeysTask(String cacheName, boolean async, Set<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().removeAll(keys);

            return null;
        }
    }

    /**
     *
     */
    private static class GetAllTask<K, V> extends CacheTaskAdapter<K, V, Map<K, V>> {
        /** Keys. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public GetAllTask(String cacheName, boolean async, Set<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Map<K, V> call() throws Exception {
            return cache().getAll(keys);
        }
    }

    /**
     *
     */
    private static class GetEntriesTask<K, V> extends CacheTaskAdapter<K, V, Collection<CacheEntry<K, V>> > {
        /** Keys. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public GetEntriesTask(String cacheName, boolean async, Set<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Collection<CacheEntry<K, V>>  call() throws Exception {
            return cache().getEntries(keys);
        }
    }

    /**
     *
     */
    private static class GetAllOutTxTask<K, V> extends CacheTaskAdapter<K, V, Map<K, V>> {
        /** Keys. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public GetAllOutTxTask(String cacheName, boolean async, Set<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Map<K, V> call() throws Exception {
            return cache().getAllOutTx(keys);
        }
    }

    /**
     *
     */
    private static class ContainsKeysTask<K, V> extends CacheTaskAdapter<K, V, Boolean> {
        /** Keys. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public ContainsKeysTask(String cacheName, boolean async, Set<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().containsKeys(keys);
        }
    }

    /**
     *
     */
    private static class GetAndPutTask<K, V> extends CacheTaskAdapter<K, V, V> {
        /** Key. */
        private final K key;

        /** Value. */
        private final V val;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param val Value.
         */
        public GetAndPutTask(String cacheName, boolean async, K key, V val) {
            super(cacheName, async, null);
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public V call() throws Exception {
            return cache().getAndPut(key, val);
        }
    }

    /**
     *
     */
    private static class PutIfAbsentTask<K, V> extends CacheTaskAdapter<K, V, Boolean> {
        /** Key. */
        private final K key;

        /** Value. */
        private final V val;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param val Value.
         */
        public PutIfAbsentTask(String cacheName, boolean async, K key, V val) {
            super(cacheName, async, null);
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().putIfAbsent(key, val);
        }
    }

    /**
     *
     */
    private static class RemoveIfExistsTask<K, V> extends CacheTaskAdapter<K, V, Boolean> {
        /** Key. */
        private final K key;

        /** Old value. */
        private final V oldVal;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param oldVal Old value.
         */
        public RemoveIfExistsTask(String cacheName, boolean async, K key, V oldVal) {
            super(cacheName, async, null);
            this.key = key;
            this.oldVal = oldVal;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().remove(key, oldVal);
        }
    }

    /**
     *
     */
    private static class GetAndRemoveTask<K, V> extends CacheTaskAdapter<K, V, V> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         */
        public GetAndRemoveTask(String cacheName, boolean async, K key) {
            super(cacheName, async, null);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public V call() throws Exception {
            return cache().getAndRemove(key);
        }
    }

    /**
     *
     */
    private static class ReplaceIfExistsTask<K, V> extends CacheTaskAdapter<K, V, Boolean> {
        /** Key. */
        private final K key;

        /** Old value. */
        private final V oldVal;

        /** New value. */
        private final V newVal;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param oldVal Old value.
         * @param newVal New value.
         */
        public ReplaceIfExistsTask(String cacheName, boolean async, K key, V oldVal, V newVal) {
            super(cacheName, async, null);
            this.key = key;
            this.oldVal = oldVal;
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().replace(key, oldVal, newVal);
        }
    }

    /**
     *
     */
    private static class GetAndReplaceTask<K, V> extends CacheTaskAdapter<K, V, V> {
        /** Key. */
        private final K key;

        /** Value. */
        private final V val;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param val Value.
         */
        public GetAndReplaceTask(String cacheName, boolean async, K key, V val) {
            super(cacheName, async, null);
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public V call() throws Exception {
            return cache().getAndReplace(key, val);
        }
    }

    /**
     *
     */
    private static class ClearKeyTask<K> extends CacheTaskAdapter<K, Void, Void> {
        /** Key. */
        private final K key;

        /** Local. */
        private final boolean loc;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         */
        public ClearKeyTask(String cacheName, boolean async, boolean loc, K key) {
            super(cacheName, async, null);
            this.key = key;
            this.loc = loc;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            if (loc)
                cache().localClear(key);
            else
                cache().clear(key);

            return null;
        }
    }

    /**
     *
     */
    private static class ClearAllKeys<K> extends CacheTaskAdapter<K, Void, Void> {
        /** Keys. */
        private final Set<? extends K> keys;

        /** Local. */
        private final boolean loc;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         */
        public ClearAllKeys(String cacheName, boolean async, boolean loc, Set<? extends K> keys) {
            super(cacheName, async, null);
            this.keys = keys;
            this.loc = loc;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            if (loc)
                cache().localClearAll(keys);
            else
                cache().clearAll(keys);

            return null;
        }
    }

    /**
     *
     */
    private static class InvokeTask<K, V, R> extends CacheTaskAdapter<K, V, R> {
        /** Key. */
        private final K key;

        /** Processor. */
        private final EntryProcessor<K, V, R> processor;

        /** Args. */
        private final Object[] args;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param key Key.
         * @param processor Processor.
         * @param args Args.
         */
        public InvokeTask(String cacheName, boolean async, K key, EntryProcessor<K, V, R> processor,
            Object[] args) {
            super(cacheName, async, null);
            this.args = args;
            this.key = key;
            this.processor = processor;
        }

        /** {@inheritDoc} */
        @Override public R call() throws Exception {
            return cache().invoke(key, processor, args);
        }
    }

    /**
     *
     */
    private static class InvokeAllTask<K, V, T> extends CacheTaskAdapter<K, V, Map<K, EntryProcessorResult<T>>> {
        /** Keys. */
        private final Set<? extends K> keys;

        /** Processor. */
        private final EntryProcessor<K, V, T> processor;

        /** Args. */
        private final Object[] args;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param keys Keys.
         * @param processor Processor.
         * @param args Args.
         */
        public InvokeAllTask(String cacheName, boolean async, Set<? extends K> keys,
            EntryProcessor<K, V, T> processor, Object[] args) {
            super(cacheName, async, null);
            this.args = args;
            this.keys = keys;
            this.processor = processor;
        }

        /** {@inheritDoc} */
        @Override public Map<K, EntryProcessorResult<T>> call() throws Exception {
            return cache().invokeAll(keys, processor, args);
        }
    }

    /**
     *
     */
    private static class CloseTask extends CacheTaskAdapter<Void, Void, Void> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public CloseTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().close();

            return null;
        }
    }

    /**
     *
     */
    private static class DestroyTask extends CacheTaskAdapter<Void, Void, Void> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public DestroyTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            cache().destroy();

            return null;
        }
    }

    /**
     *
     */
    private static class IsClosedTask extends CacheTaskAdapter<Void, Void, Boolean> {
        /**
         * @param cacheName Cache name.
         * @param async Async.
         */
        public IsClosedTask(String cacheName, boolean async) {
            super(cacheName, async, null);
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            return cache().isClosed();
        }
    }

    /**
     *
     */
    private static class UnwrapTask<R> extends CacheTaskAdapter<Void, Void, R> {
        /** Clazz. */
        private final Class<R> clazz;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param clazz Clazz.
         */
        public UnwrapTask(String cacheName, boolean async, Class<R> clazz) {
            super(cacheName, async, null);
            this.clazz = clazz;
        }

        /** {@inheritDoc} */
        @Override public R call() throws Exception {
            return cache().unwrap(clazz);
        }
    }

    /**
     *
     */
    private static abstract class CacheTaskAdapter<K, V, R> implements IgniteCallable<R> {
        /** Ignite. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** Cache name. */
        protected final String cacheName;

        /** Async. */
        protected final boolean async;

        /** Expiry policy. */
        protected final ExpiryPolicy expiryPlc;

        /**
         * @param cacheName Cache name.
         * @param async Async.
         * @param expiryPlc Optional expiry policy.
         */
        public CacheTaskAdapter(String cacheName, boolean async, ExpiryPolicy expiryPlc) {
            this.async = async;
            this.cacheName = cacheName;
            this.expiryPlc = expiryPlc;
        }

        /**
         * @return Cache instance.
         */
        protected IgniteCache<K, V> cache() {
            IgniteCache<K, V> cache = ignite.cache(cacheName);

            cache = expiryPlc != null ? cache.withExpiryPolicy(expiryPlc) : cache;

            return async ? cache.withAsync() : cache;
        }
    }
}

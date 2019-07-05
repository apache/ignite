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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
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

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Ignite proxy. */
    private final transient IgniteProcessProxy igniteProxy;

    /**
     * @param name Name.
     * @param proxy Ignite Process Proxy.
     */
    public IgniteCacheProcessProxy(String name, IgniteProcessProxy proxy) {
        this(name, null, proxy);
    }

    /**
     * @param name Name.
     * @param plc Expiry policy.
     * @param proxy Ignite Process Proxy.
     */
    private IgniteCacheProcessProxy(String name, ExpiryPolicy plc, IgniteProcessProxy proxy) {
        cacheName = name;
        expiryPlc = plc;
        igniteProxy = proxy;
        compute = proxy.remoteCompute();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return compute.call(new GetConfigurationTask<>(cacheName, clazz));
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        return new IgniteCacheProcessProxy<>(cacheName, plc, igniteProxy);
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
    @Override public IgniteCache<K, V> withPartitionRecover() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws CacheException {
        compute.call(new LocalLoadCacheTask<>(cacheName, p, args));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        return compute.call(new GetAndPutIfAbsentTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutIfAbsentAsync(K key, V val) throws CacheException {
        return compute.callAsync(new GetAndPutIfAbsentTask<>(cacheName, key, val));
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
        return compute.call(new IsLocalLockedTask<>(cacheName, key, byCurrThread));
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        return compute.call(new LocalEntriesTask<>(cacheName, peekModes));
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        compute.call(new LocalEvictTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public V localPeek(K key, CachePeekMode... peekModes) {
        return compute.call(new LocalPeekTask<K, V>(cacheName, key, peekModes));
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        return compute.call(new SizeTask(cacheName, peekModes, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException {
        return compute.callAsync(new SizeTask(cacheName, peekModes, false));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        return compute.call(new SizeLongTask(cacheName, peekModes, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException {
        return compute.callAsync(new SizeLongTask(cacheName, peekModes, false));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode... peekModes) throws CacheException {
        return compute.call(new PartitionSizeLongTask(cacheName, peekModes, partition, false));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(int partition, CachePeekMode... peekModes) throws CacheException {
        return compute.callAsync(new PartitionSizeLongTask(cacheName, peekModes, partition, false));
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        return compute.call(new SizeTask(cacheName, peekModes, true));
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        return compute.call(new SizeLongTask(cacheName, peekModes, true));
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode... peekModes) {
        return compute.call(new PartitionSizeLongTask(cacheName, peekModes, partition, true));
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        return compute.call(new GetTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync(K key) {
        return compute.callAsync(new GetTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) {
        return compute.call(new GetEntryTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<CacheEntry<K, V>> getEntryAsync(K key) {
        return compute.callAsync(new GetEntryTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        return compute.call(new GetAllTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) {
        return compute.callAsync(new GetAllTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) {
        return compute.call(new GetEntriesTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(Set<? extends K> keys) {
        return compute.callAsync(new GetEntriesTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        return compute.call(new GetAllOutTxTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        return compute.callAsync(new GetAllOutTxTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return compute.call(new ContainsKeyTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeyAsync(K key) {
        return compute.callAsync(new ContainsKeyTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public void loadAll(Set<? extends K> keys, boolean replaceExistVals, CompletionListener completionLsnr) {
        throw new UnsupportedOperationException("Operation can't be supported automatically.");
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        return compute.call(new ContainsKeysTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) {
        return compute.callAsync(new ContainsKeysTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        compute.call(new PutTask<>(cacheName, expiryPlc, key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAsync(K key, V val) {
        return compute.callAsync(new PutTask<>(cacheName, expiryPlc, key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return compute.call(new GetAndPutTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutAsync(K key, V val) {
        return compute.callAsync(new GetAndPutTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        compute.call(new PutAllTask<>(cacheName, map));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) {
        return compute.callAsync(new PutAllTask<>(cacheName, map));
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return compute.call(new PutIfAbsentTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putIfAbsentAsync(K key, V val) {
        return compute.callAsync(new PutIfAbsentTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return compute.call(new RemoveTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key) {
        return compute.callAsync(new RemoveTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        return compute.call(new RemoveIfExistsTask<>(cacheName, key, oldVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V oldVal) {
        return compute.callAsync(new RemoveIfExistsTask<>(cacheName, key, oldVal));
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return compute.call(new GetAndRemoveTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndRemoveAsync(K key) {
        return compute.callAsync(new GetAndRemoveTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return compute.call(new ReplaceIfExistsTask<>(cacheName, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return compute.callAsync(new ReplaceIfExistsTask<>(cacheName, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return compute.call(new ReplaceTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V val) {
        return compute.callAsync(new ReplaceTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return compute.call(new GetAndReplaceTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndReplaceAsync(K key, V val) {
        return compute.callAsync(new GetAndReplaceTask<>(cacheName, key, val));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        compute.call(new RemoveAllKeysTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync(Set<? extends K> keys) {
        return compute.callAsync(new RemoveAllKeysTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        compute.call(new RemoveAllTask<K, V>(cacheName));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync() {
        return compute.callAsync(new RemoveAllTask<K, V>(cacheName));
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        compute.call(new ClearTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync() {
        return compute.callAsync(new ClearTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        compute.call(new ClearKeyTask<>(cacheName, false, key));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync(K key) {
        return compute.callAsync(new ClearKeyTask<>(cacheName, false, key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        compute.call(new ClearAllKeys<>(cacheName, false, keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAllAsync(Set<? extends K> keys) {
        return compute.callAsync(new ClearAllKeys<>(cacheName, false, keys));
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        compute.call(new ClearKeyTask<>(cacheName, true, key));
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        compute.call(new ClearAllKeys<>(cacheName, true, keys));
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> processor, Object... args) {
        return compute.call(new InvokeTask<>(cacheName, key, processor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(
        K key, EntryProcessor<K, V, T> processor, Object... args) {
        return compute.callAsync(new InvokeTask<>(cacheName, key, processor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> processor, Object... args) {
        return compute.call(new InvokeTask<>(cacheName, key, processor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, CacheEntryProcessor<K, V, T> processor,
        Object... args) {
        return compute.callAsync(new InvokeTask<>(cacheName, key, processor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> processor,
        Object... args) {
        return compute.call(new InvokeAllTask<>(cacheName, keys, processor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> processor, Object... args) {
        return compute.callAsync(new InvokeAllTask<>(cacheName, keys, processor, args));
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return compute.call(new GetNameTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() {
        compute.call(new CloseTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        compute.call(new DestroyTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return compute.call(new IsClosedTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (Ignite.class.equals(clazz))
            return (T)igniteProxy;

        try {
            return compute.call(new UnwrapTask<>(cacheName, clazz));
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Looks like class " + clazz +
                " is unmarshallable. Exception type:" + e.getClass(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        return compute.call(new IteratorTask<K, V>(cacheName)).iterator();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> rebalance() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> indexReadyFuture() {
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
    @Override public CacheMetrics localMetrics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(boolean enabled) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void clearStatistics() {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int partId) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> preloadPartitionAsync(int partId) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int partition) {
        throw new UnsupportedOperationException("Method should be supported.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAllowAtomicOpsInTx() {
        return this;
    }

    /**
     *
     */
    private static class GetConfigurationTask<K, V, C extends Configuration<K, V>> extends CacheTaskAdapter<K, V, C> {
        /** Clazz. */
        private final Class<C> clazz;

        /**
         * @param cacheName Cache name.
         * @param clazz Clazz.
         */
        public GetConfigurationTask(String cacheName, Class<C> clazz) {
            super(cacheName, null);
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
         * @param p P.
         * @param args Args.
         */
        public LocalLoadCacheTask(String cacheName, IgniteBiPredicate<K, V> p, Object[] args) {
            super(cacheName, null);
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
         * @param key Key.
         * @param val Value.
         */
        public GetAndPutIfAbsentTask(String cacheName, K key, V val) {
            super(cacheName, null);
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
         * @param key Key.
         * @param byCurrThread By current thread.
         */
        public IsLocalLockedTask(String cacheName, K key, boolean byCurrThread) {
            super(cacheName, null);
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
         * @param peekModes Peek modes.
         */
        public LocalEntriesTask(String cacheName, CachePeekMode[] peekModes) {
            super(cacheName, null);
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
         * @param keys Keys.
         */
        public LocalEvictTask(String cacheName, Collection<? extends K> keys) {
            super(cacheName, null);
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
         * @param key Key.
         * @param peekModes Peek modes.
         */
        public LocalPeekTask(String cacheName, K key, CachePeekMode[] peekModes) {
            super(cacheName, null);
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
         * @param peekModes Peek modes.
         * @param loc Local.
         */
        public SizeTask(String cacheName, CachePeekMode[] peekModes, boolean loc) {
            super(cacheName, null);
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
         * @param peekModes Peek modes.
         * @param loc Local.
         */
        public SizeLongTask(String cacheName, CachePeekMode[] peekModes, boolean loc) {
            super(cacheName, null);
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
         * @param peekModes Peek modes.
         * @param partition partition.
         * @param loc Local.
         */
        public PartitionSizeLongTask(String cacheName, CachePeekMode[] peekModes, int partition, boolean loc) {
            super(cacheName, null);
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
         * @param key Key.
         */
        public GetTask(String cacheName, K key) {
            super(cacheName, null);
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
         * @param key Key.
         */
        public GetEntryTask(String cacheName, K key) {
            super(cacheName, null);

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
         */
        public RemoveAllTask(String cacheName) {
            super(cacheName, null);
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            IgniteCache<K, V> cache = cache();

            cache.removeAll();

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
         * @param expiryPlc Expiry policy.
         * @param key Key.
         * @param val Value.
         */
        public PutTask(String cacheName, ExpiryPolicy expiryPlc, K key, V val) {
            super(cacheName, expiryPlc);
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
         * @param key Key.
         */
        public ContainsKeyTask(String cacheName, K key) {
            super(cacheName, null);
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
         */
        public ClearTask(String cacheName) {
            super(cacheName, null);
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
         */
        public IteratorTask(String cacheName) {
            super(cacheName, null);
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
         * @param key Key.
         * @param val Value.
         */
        public ReplaceTask(String cacheName, K key, V val) {
            super(cacheName, null);
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
         */
        public GetNameTask(String cacheName) {
            super(cacheName, null);
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
         * @param key Key.
         */
        public RemoveTask(String cacheName, K key) {
            super(cacheName, null);
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
         * @param map Map.
         */
        public PutAllTask(String cacheName, Map<? extends K, ? extends V> map) {
            super(cacheName, null);
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
         * @param keys Keys.
         */
        public RemoveAllKeysTask(String cacheName, Set<? extends K> keys) {
            super(cacheName, null);
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
         * @param keys Keys.
         */
        public GetAllTask(String cacheName, Set<? extends K> keys) {
            super(cacheName, null);
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
    private static class GetEntriesTask<K, V> extends CacheTaskAdapter<K, V, Collection<CacheEntry<K, V>>> {
        /** Keys. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param keys Keys.
         */
        public GetEntriesTask(String cacheName, Set<? extends K> keys) {
            super(cacheName, null);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Collection<CacheEntry<K, V>> call() throws Exception {
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
         * @param keys Keys.
         */
        public GetAllOutTxTask(String cacheName, Set<? extends K> keys) {
            super(cacheName, null);
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
         * @param keys Keys.
         */
        public ContainsKeysTask(String cacheName, Set<? extends K> keys) {
            super(cacheName, null);
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
         * @param key Key.
         * @param val Value.
         */
        public GetAndPutTask(String cacheName, K key, V val) {
            super(cacheName, null);
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
         * @param key Key.
         * @param val Value.
         */
        public PutIfAbsentTask(String cacheName, K key, V val) {
            super(cacheName, null);
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
         * @param key Key.
         * @param oldVal Old value.
         */
        public RemoveIfExistsTask(String cacheName, K key, V oldVal) {
            super(cacheName, null);
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
         * @param key Key.
         */
        public GetAndRemoveTask(String cacheName, K key) {
            super(cacheName, null);
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
         * @param key Key.
         * @param oldVal Old value.
         * @param newVal New value.
         */
        public ReplaceIfExistsTask(String cacheName, K key, V oldVal, V newVal) {
            super(cacheName, null);
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
         * @param key Key.
         * @param val Value.
         */
        public GetAndReplaceTask(String cacheName, K key, V val) {
            super(cacheName, null);
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
         * @param loc Local flag.
         * @param key Key.
         */
        public ClearKeyTask(String cacheName, boolean loc, K key) {
            super(cacheName, null);
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
         * @param loc Local flag.
         * @param keys Keys.
         */
        public ClearAllKeys(String cacheName, boolean loc, Set<? extends K> keys) {
            super(cacheName, null);
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
         * @param key Key.
         * @param processor Processor.
         * @param args Args.
         */
        public InvokeTask(String cacheName, K key, EntryProcessor<K, V, R> processor,
            Object[] args) {
            super(cacheName, null);
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
         * @param keys Keys.
         * @param processor Processor.
         * @param args Args.
         */
        public InvokeAllTask(String cacheName, Set<? extends K> keys,
            EntryProcessor<K, V, T> processor, Object[] args) {
            super(cacheName, null);
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
         */
        public CloseTask(String cacheName) {
            super(cacheName, null);
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
         */
        public DestroyTask(String cacheName) {
            super(cacheName, null);
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
         */
        public IsClosedTask(String cacheName) {
            super(cacheName, null);
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
         * @param clazz Clazz.
         */
        public UnwrapTask(String cacheName, Class<R> clazz) {
            super(cacheName, null);
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
    private abstract static class CacheTaskAdapter<K, V, R> implements IgniteCallable<R> {
        /** Ignite. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** Cache name. */
        protected final String cacheName;

        /** Expiry policy. */
        protected final ExpiryPolicy expiryPlc;

        /**
         * @param cacheName Cache name.
         * @param expiryPlc Optional expiry policy.
         */
        public CacheTaskAdapter(String cacheName, ExpiryPolicy expiryPlc) {
            this.cacheName = cacheName;
            this.expiryPlc = expiryPlc;
        }

        /**
         * @return Cache instance.
         */
        protected IgniteCache<K, V> cache() {
            IgniteCache<K, V> cache = ignite.cache(cacheName);

            cache = expiryPlc != null ? cache.withExpiryPolicy(expiryPlc) : cache;

            return cache;
        }
    }
}

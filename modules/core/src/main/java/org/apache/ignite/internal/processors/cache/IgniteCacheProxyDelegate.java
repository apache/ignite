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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheManager;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.jetbrains.annotations.Nullable;

/**
 * Cache proxy delegate.
 */
@SuppressWarnings("unchecked")
public class IgniteCacheProxyDelegate<K, V> extends IgniteCacheProxy<K, V>{
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Grid cache processor */
    private GridCacheProcessor gridCacheProcessor;

    /** Context. */
    private IgniteCacheProxy<K, V> cache;

    public IgniteCacheProxyDelegate(String name, GridCacheProcessor processor) {
        cacheName = name;
        gridCacheProcessor = processor;
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync() {
        checkCache();

        return cache.withAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        checkCache();

        return cache.isAsync();
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        checkCache();

        return cache.future();
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future(boolean reset) {
        checkCache();

        return cache.future(reset);
    }

    /** {@inheritDoc} */
    @Override public <R> R saveOrGet(IgniteInternalFuture<R> fut) throws IgniteCheckedException {
        checkCache();

        return cache.saveOrGet(fut);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheOperationContext operationContext() {
        checkCache();

        return cache.operationContext();
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheProxy<K, V> cacheNoGate() {
        checkCache();

        return cache.cacheNoGate();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<K, V> context() {
        checkCache();

        return cache.context();
    }

    /** {@inheritDoc} */
    @Override public GridCacheGateway<K, V> gate() {
        checkCache();

        return cache.gate();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        checkCache();

        return cache.metrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        checkCache();

        return cache.metrics(grp);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        checkCache();

        return cache.localMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        checkCache();

        return cache.mxBean();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        checkCache();

        return cache.localMxBean();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        checkCache();

        return cache.getConfiguration(clazz);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        checkCache();

        return cache.withExpiryPolicy(plc);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        checkCache();

        return cache.withSkipStore();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
        checkCache();

        return cache.withKeepBinary();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withNoRetries() {
        checkCache();

        return cache.withNoRetries();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withPartitionRecover() {
        checkCache();

        return cache.withPartitionRecover();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        checkCache();

        cache.loadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        checkCache();

        return cache.loadCacheAsync(p, args);
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        checkCache();

        cache.localLoadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        checkCache();

        return cache.localLoadCacheAsync(p, args);
    }

    @Nullable /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        checkCache();

        return cache.getAndPutIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutIfAbsentAsync(K key, V val) throws CacheException {
        checkCache();

        return cache.getAndPutIfAbsentAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        checkCache();

        return cache.lock(key);
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Collection<? extends K> keys) {
        checkCache();

        return cache.lockAll(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        checkCache();

        return cache.isLocalLocked(key, byCurrThread);
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        checkCache();

        return cache.query(qry);
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        checkCache();

        return cache.query(qry, transformer);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.localEntries(peekModes);
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        checkCache();

        return cache.queryMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        checkCache();

        cache.resetQueryMetrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        checkCache();

        return cache.queryDetailMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        checkCache();

        cache.resetQueryDetailMetrics();
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        checkCache();

        cache.localEvict(keys);
    }

    @Nullable /** {@inheritDoc} */
    @Override public V localPeek(K key, CachePeekMode... peekModes) {
        checkCache();

        return cache.localPeek(key, peekModes);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.size(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.sizeAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.sizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.sizeLongAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int part, CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.sizeLong(part, peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(int part, CachePeekMode... peekModes) throws CacheException {
        checkCache();

        return cache.sizeLongAsync(part, peekModes);
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        checkCache();

        return cache.localSize(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        checkCache();

        return cache.localSizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int part, CachePeekMode... peekModes) {
        checkCache();

        return cache.localSizeLong(part, peekModes);
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        checkCache();

        return cache.get(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync(K key) {
        checkCache();

        return cache.getAsync(key);
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) {
        checkCache();

        return cache.getEntry(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<CacheEntry<K, V>> getEntryAsync(K key) {
        checkCache();

        return cache.getEntryAsync(key);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        checkCache();

        return cache.getAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) {
        checkCache();

        return cache.getAllAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) {
        checkCache();

        return cache.getEntries(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(Set<? extends K> keys) {
        checkCache();

        return cache.getEntriesAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        checkCache();

        return cache.getAllOutTx(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        checkCache();

        return cache.getAllOutTxAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Collection<? extends K> keys) {
        checkCache();

        return cache.getAll(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        checkCache();

        return cache.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeyAsync(K key) {
        checkCache();

        return cache.containsKeyAsync(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        checkCache();

        return cache.containsKeys(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) {
        checkCache();

        return cache.containsKeysAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public void loadAll(Set<? extends K> keys, boolean replaceExisting,
        @Nullable CompletionListener completionLsnr) {
        checkCache();

        cache.loadAll(keys, replaceExisting, completionLsnr);
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        checkCache();

        cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAsync(K key, V val) {
        checkCache();

        return cache.putAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        checkCache();

        return cache.getAndPut(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutAsync(K key, V val) {
        checkCache();

        return cache.getAndPutAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        checkCache();

        cache.putAll(map);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) {
        checkCache();

        return cache.putAllAsync(map);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        checkCache();

        return cache.putIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putIfAbsentAsync(K key, V val) {
        checkCache();

        return cache.putIfAbsentAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        checkCache();

        return cache.remove(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key) {
        checkCache();

        return cache.removeAsync(key);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        checkCache();

        return cache.remove(key, oldVal);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V oldVal) {
        checkCache();

        return cache.removeAsync(key, oldVal);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        checkCache();

        return cache.getAndRemove(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndRemoveAsync(K key) {
        checkCache();

        return cache.getAndRemoveAsync(key);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        checkCache();

        return cache.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        checkCache();

        return cache.replaceAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        checkCache();

        return cache.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V val) {
        checkCache();

        return cache.replaceAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        checkCache();

        return cache.getAndReplace(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndReplaceAsync(K key, V val) {
        checkCache();

        return cache.getAndReplaceAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        checkCache();

        cache.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync(Set<? extends K> keys) {
        checkCache();

        return cache.removeAllAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        checkCache();

        cache.removeAll();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync() {
        checkCache();

        return cache.removeAllAsync();
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        checkCache();

        cache.clear(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync(K key) {
        checkCache();

        return cache.clearAsync(key);
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        checkCache();

        cache.clearAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAllAsync(Set<? extends K> keys) {
        checkCache();

        return cache.clearAllAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        checkCache();

        cache.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync() {
        checkCache();

        return cache.clearAsync();
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        checkCache();

        cache.localClear(key);
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        checkCache();

        cache.localClearAll(keys);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args) throws EntryProcessorException {
        checkCache();

        return cache.invoke(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, EntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invokeAsync(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor,
        Object... args) throws EntryProcessorException {
        checkCache();

        return cache.invoke(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override
    public <T> IgniteFuture<T> invokeAsync(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invokeAsync(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(@Nullable AffinityTopologyVersion topVer, K key,
        EntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invoke(topVer, key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invokeAll(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invokeAllAsync(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invokeAll(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        checkCache();

        return cache.invokeAllAsync(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) {
        checkCache();

        return cache.invokeAll(map, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) {
        checkCache();

        return cache.invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        checkCache();

        return cache.getName();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        checkCache();

        return cache.getCacheManager();
    }

    /** {@inheritDoc} */
    @Override public void setCacheManager(CacheManager cacheMgr) {
        checkCache();

        cache.setCacheManager(cacheMgr);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        checkCache();

        cache.destroy();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        checkCache();

        cache.close();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        checkCache();

        return cache.isClosed();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache delegate() {
        checkCache();

        return cache.delegate();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        checkCache();

        return cache.unwrap(clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        checkCache();

        cache.registerCacheEntryListener(lsnrCfg);
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        checkCache();

        cache.deregisterCacheEntryListener(lsnrCfg);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        checkCache();

        return cache.iterator();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> createAsyncInstance() {
        checkCache();

        return cache.createAsyncInstance();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> keepBinary() {
        checkCache();

        return cache.keepBinary();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withDataCenterId(byte dataCenterId) {
        checkCache();

        return cache.withDataCenterId(dataCenterId);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> skipStore() {
        checkCache();

        return cache.skipStore();
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> createFuture(IgniteInternalFuture<R> fut) {
        checkCache();

        return cache.createFuture(fut);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> internalProxy() {
        checkCache();

        return cache.internalProxy();
    }

    /** {@inheritDoc} */
    @Override public boolean proxyClosed() {
        checkCache();

        return cache.proxyClosed();
    }

    /** {@inheritDoc} */
    @Override public void closeProxy() {
        checkCache();

        cache.closeProxy();
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        checkCache();

        return cache.lostPartitions();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        checkCache();

        cache.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        checkCache();

        cache.readExternal(in);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebalance() {
        checkCache();

        return cache.rebalance();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> indexReadyFuture() {
        checkCache();

        return cache.indexReadyFuture();
    }

    /** {@inheritDoc} */
    @Override public V getTopologySafe(K key) {
        checkCache();

        return cache.getTopologySafe(key);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        checkCache();

        return cache.toString();
    }

    /** {@inheritDoc} */
    @Override public void forEach(Consumer<? super Entry<K, V>> action) {
        checkCache();

        cache.forEach(action);
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Entry<K, V>> spliterator() {
        checkCache();

        return cache.spliterator();
    }

    private void checkCache() {
        if (cache == null)
            cache = gridCacheProcessor.jcache(cacheName);
    }
}

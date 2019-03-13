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

package org.apache.ignite.internal.processors.cache.distributed.dht.consistency;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.AsyncSupportAdapter;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ConsistencyGatewayProtectedCacheProxy<K, V> extends AsyncSupportAdapter<IgniteCache<K, V>>
    implements IgniteCacheProxy<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String MESSAGE = "Method is not allowed at consistency mode.";

    /** Cache proxy delegate. */
    private GatewayProtectedCacheProxy<K, V> delegate;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public ConsistencyGatewayProtectedCacheProxy() {
    }

    /**
     * @param delegate Cache proxy delegate.
     */
    public ConsistencyGatewayProtectedCacheProxy(
        GatewayProtectedCacheProxy<K, V> delegate
    ) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<K, V> context() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheProxy<K, V> cacheNoGate() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> keepBinary() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withDataCenterId(byte dataCenterId) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> skipStore() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAllowAtomicOpsInTx() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> internalProxy() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean isProxyClosed() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void closeProxy() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> destroyAsync() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> closeAsync() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> queryMultipleStatements(SqlFieldsQuery qry) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withNoRetries() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withPartitionRecover() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withConsistencyCheck() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws CacheException, TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutIfAbsentAsync(K key, V val) throws CacheException, TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Collection<? extends K> keys) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public V localPeek(K key, CachePeekMode... peekModes) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(int partition, CachePeekMode... peekModes) throws CacheException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode... peekModes) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws TransactionException {
       return delegate.get(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync(K key) {
        return delegate.getAsync(key);
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) throws TransactionException {
        return delegate.getEntry(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<CacheEntry<K, V>> getEntryAsync(K key) throws TransactionException {
        return delegate.getEntryAsync(key);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) throws TransactionException {
        return delegate.getAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) throws TransactionException {
        return delegate.getAllAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) throws TransactionException {
        return delegate.getEntries(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(
        Set<? extends K> keys) throws TransactionException {
        return delegate.getEntriesAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        return delegate.getAllOutTx(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        return delegate.getAllOutTxAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeyAsync(K key) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) throws TransactionException {

    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAsync(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutAsync(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) throws TransactionException {

    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putIfAbsentAsync(K key, V val) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V oldVal) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndRemoveAsync(K key) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndReplaceAsync(K key, V val) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync(Set<? extends K> keys) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync(K key) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAllAsync(Set<? extends K> keys) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor,
        Object... arguments) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, EntryProcessor<K, V, T> entryProcessor,
        Object... arguments) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor,
        Object... arguments) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, CacheEntryProcessor<K, V, T> entryProcessor,
        Object... arguments) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        delegate.close();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return delegate.isClosed();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        delegate.destroy();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> rebalance() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> indexReadyFuture() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(boolean enabled) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void clearStatistics() {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int partition) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> preloadPartitionAsync(int partition) {
        throw new UnsupportedOperationException(MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int partition) {
        throw new UnsupportedOperationException(MESSAGE);
    }
}

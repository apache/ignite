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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache proxy wrapper with gateway lock provided operations and possibility to change cache operation context.
 */
public class GatewayProtectedCacheProxy<K, V> extends AsyncSupportAdapter<IgniteCache<K, V>>
    implements IgniteCacheProxy<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache proxy delegate. */
    private IgniteCacheProxy<K, V> delegate;

    /** If {@code false} does not acquire read lock on gateway enter. */
    @GridToStringExclude private boolean lock;

    /** Cache operation context. */
    private CacheOperationContext opCtx;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GatewayProtectedCacheProxy() {
    }

    /**
     *
     * @param delegate Cache proxy delegate.
     * @param opCtx Cache operation context.
     * @param lock True if cache proxy should be protected with gateway lock, false in other case.
     */
    public GatewayProtectedCacheProxy(
        @NotNull IgniteCacheProxy<K, V> delegate,
        @NotNull CacheOperationContext opCtx,
        boolean lock
    ) {
        this.delegate = delegate;
        this.opCtx = opCtx;
        this.lock = lock;
    }

    /**
     * Sets CacheManager to delegate.
     *
     * @param cacheMgr Cache Manager.
     */
    public void setCacheManager(org.apache.ignite.cache.CacheManager cacheMgr) {
        if (delegate instanceof IgniteCacheProxyImpl)
            ((IgniteCacheProxyImpl) delegate).setCacheManager(cacheMgr);
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<K, V> context() {
        return delegate.context();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return delegate.getConfiguration(clazz);
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.getName();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        return delegate.getCacheManager();
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> internalProxy() {
        return delegate.internalProxy();
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> cacheNoGate() {
        return new GatewayProtectedCacheProxy<>(delegate, opCtx, false);
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return new GatewayProtectedCacheProxy<>(delegate, opCtx.withExpiryPolicy(plc), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> withSkipStore() {
        return skipStore();
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> skipStore() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            boolean skip = opCtx.skipStore();

            if (skip)
                return this;

            return new GatewayProtectedCacheProxy<>(delegate, opCtx.setSkipStore(true), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> withNoRetries() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            boolean noRetries = opCtx.noRetries();

            if (noRetries)
                return this;

            return new GatewayProtectedCacheProxy<>(delegate, opCtx.setNoRetries(true), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> withPartitionRecover() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            boolean recovery = opCtx.recovery();

            if (recovery)
                return this;

            return new GatewayProtectedCacheProxy<>(delegate, opCtx.setRecovery(true), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> GatewayProtectedCacheProxy<K1, V1> withKeepBinary() {
        return keepBinary();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> GatewayProtectedCacheProxy<K1, V1> keepBinary() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return new GatewayProtectedCacheProxy<>((IgniteCacheProxy<K1, V1>) delegate, opCtx.keepBinary(), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GatewayProtectedCacheProxy<K, V> withDataCenterId(byte dataCenterId) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            Byte prevDataCenterId = opCtx.dataCenterId();

            if (prevDataCenterId != null && dataCenterId == prevDataCenterId)
                return this;

            return new GatewayProtectedCacheProxy<>(delegate, opCtx.setDataCenterId(dataCenterId), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.loadCache(p, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.loadCacheAsync(p, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.localLoadCache(p, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localLoadCacheAsync(p, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPutIfAbsent(K key, V val) throws CacheException, TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndPutIfAbsent(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutIfAbsentAsync(K key, V val) throws CacheException, TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndPutIfAbsentAsync(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) {
        return delegate.lock(key);
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(Collection<? extends K> keys) {
        return delegate.lockAll(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.isLocalLocked(key, byCurrThread);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.query(qry);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.query(qry);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.query(qry, transformer);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localEntries(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.queryMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.resetQueryMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.queryDetailMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.resetQueryDetailMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.localEvict(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V localPeek(K key, CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localPeek(key, peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.size(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.sizeAsync(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.sizeLong(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.sizeLongAsync(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.sizeLong(partition, peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(int partition, CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.sizeLongAsync(partition, peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localSize(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localSizeLong(peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localSizeLong(partition, peekModes);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAll(map, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAllAsync(map, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.get(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync(K key) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAsync(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getEntry(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<CacheEntry<K, V>> getEntryAsync(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getEntryAsync(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAll(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAllAsync(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getEntries(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getEntriesAsync(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAllOutTx(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAllOutTxAsync(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.containsKey(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(Set<? extends K> keys, boolean replaceExisting, CompletionListener completionListener) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.loadAll(keys, replaceExisting, completionListener);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeyAsync(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.containsKeyAsync(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.containsKeys(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.containsKeysAsync(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.put(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAsync(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.putAsync(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndPut(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutAsync(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndPutAsync(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.putAll(map);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.putAllAsync(map);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.putIfAbsent(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putIfAbsentAsync(K key, V val) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.putIfAbsentAsync(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.remove(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.removeAsync(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.remove(key, oldVal);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V oldVal) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.removeAsync(key, oldVal);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndRemove(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndRemoveAsync(K key) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndRemoveAsync(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.replace(key, oldVal, newVal);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.replaceAsync(key, oldVal, newVal);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.replace(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.replaceAsync(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndReplace(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndReplaceAsync(K key, V val) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.getAndReplaceAsync(key, val);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.removeAll(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync(Set<? extends K> keys) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.removeAllAsync(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.removeAll();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.removeAllAsync();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.clear();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.clearAsync();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.clear(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync(K key) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.clearAsync(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.clearAll(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAllAsync(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.clearAllAsync(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.localClear(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.localClearAll(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invoke(key, entryProcessor, arguments);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAsync(key, entryProcessor, arguments);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... arguments) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invoke(key, entryProcessor, arguments);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... arguments) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAsync(key, entryProcessor, arguments);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAll(keys, entryProcessor, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAllAsync(keys, entryProcessor, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, CacheEntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAll(keys, entryProcessor, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys, CacheEntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.invokeAllAsync(keys, entryProcessor, args);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        return delegate.unwrap(clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.registerCacheEntryListener(cacheEntryListenerConfiguration);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.iterator();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        GridCacheGateway<K, V> gate = gate();

        if (!onEnterIfNoStop(gate))
            return;

        IgniteFuture<?> destroyFuture;

        try {
            destroyFuture = delegate.destroyAsync();
        }
        finally {
            onLeave(gate);
        }

        if (destroyFuture != null)
            destroyFuture.get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> destroyAsync() {
        return delegate.destroyAsync();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        GridCacheGateway<K, V> gate = gate();

        if (!onEnterIfNoStop(gate))
            return;

        IgniteFuture<?> closeFuture;

        try {
            closeFuture = closeAsync();
        }
        finally {
            onLeave(gate);
        }

        if (closeFuture != null)
            closeFuture.get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> closeAsync() {
        return delegate.closeAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return delegate.isClosed();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebalance() {
        return delegate.rebalance();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> indexReadyFuture() {
        return delegate.indexReadyFuture();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.metrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.metrics(grp);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.mxBean();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localMxBean();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        GridCacheGateway<K, V> gate = gate();

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.lostPartitions();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /**
     * Safely get CacheGateway.
     *
     * @return Cache Gateway.
     */
    @Nullable private GridCacheGateway<K, V> gate() {
        GridCacheContext<K, V> cacheContext = delegate.context();
        return cacheContext != null ? cacheContext.gate() : null;
    }

    /**
     * Checks that proxy is in valid state (not closed, restarted or destroyed).
     * Throws IllegalStateException or CacheRestartingException if proxy is in invalid state.
     *
     * @param gate Cache gateway.
     */
    private void checkProxyIsValid(@Nullable GridCacheGateway<K, V> gate) {
        if (isProxyClosed())
            throw new IllegalStateException("Cache has been closed: " + context().name());

        if (delegate instanceof IgniteCacheProxyImpl)
            ((IgniteCacheProxyImpl) delegate).checkRestart();
        
        if (gate == null)
            throw new IllegalStateException("Gateway is unavailable. Probably cache has been destroyed, but proxy is not closed.");
    }
    
    /**
     * @param gate Cache gateway.
     * @param opCtx Cache operation context to guard.
     * @return Previous projection set on this thread.
     */
    private CacheOperationContext onEnter(@Nullable GridCacheGateway<K, V> gate, CacheOperationContext opCtx) {
        checkProxyIsValid(gate);
        
        return lock ? gate.enter(opCtx) : gate.enterNoLock(opCtx);
    }

    /**
     * @param gate Cache gateway.
     * @return {@code True} if enter successful.
     */
    private boolean onEnterIfNoStop(@Nullable GridCacheGateway<K, V> gate) {
        try {
            checkProxyIsValid(gate);
        }
        catch (Exception e) {
            return false;
        }

        return lock ? gate.enterIfNotStopped() : gate.enterIfNotStoppedNoLock();
    }

    /**
     * @param gate Cache gateway.
     * @param opCtx Operation context to guard.
     */
    private void onLeave(GridCacheGateway<K, V> gate, CacheOperationContext opCtx) {
        if (lock)
            gate.leave(opCtx);
        else
            gate.leaveNoLock(opCtx);
    }

    /**
     * @param gate Cache gateway.
     */
    private void onLeave(GridCacheGateway<K, V> gate) {
        if (lock)
            gate.leave();
        else
            gate.leaveNoLock();
    }

    /** {@inheritDoc} */
    @Override public boolean isProxyClosed() {
        return delegate.isProxyClosed();
    }

    /** {@inheritDoc} */
    @Override public void closeProxy() {
        delegate.closeProxy();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync() {
        return delegate.withAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return delegate.isAsync();
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        return delegate.future();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(delegate);

        out.writeBoolean(lock);

        out.writeObject(opCtx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        delegate = (IgniteCacheProxy<K, V>) in.readObject();

        lock = in.readBoolean();

        opCtx = (CacheOperationContext) in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object another) {
        GatewayProtectedCacheProxy anotherProxy = (GatewayProtectedCacheProxy) another;

        return delegate.equals(anotherProxy.delegate);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return delegate.hashCode();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.hibernate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Hibernate cache proxy used to substitute hibernate keys with ignite keys.
 */
public class HibernateCacheProxy implements IgniteInternalCache<Object, Object> {
    /** Delegate is lazily loaded which allows for creation of caches after the SPI is bootstrapped */
    private final Supplier<IgniteInternalCache<Object, Object>> delegate;

    /** Transformer. */
    private final HibernateKeyTransformer keyTransformer;

    /** */
    private String cacheName;

    /**
     * @param cacheName Cache name. Should match delegate.get().name(). Needed for lazy loading.
     * @param delegate Delegate.
     * @param keyTransformer Key keyTransformer.
     */
    HibernateCacheProxy(
        String cacheName,
        Supplier<IgniteInternalCache<Object, Object>> delegate,
        HibernateKeyTransformer keyTransformer
    ) {
        assert cacheName != null;
        assert delegate != null;
        assert keyTransformer != null;

        this.cacheName = cacheName;
        this.delegate = delegate;
        this.keyTransformer = keyTransformer;
    }

    /**
     * @return HibernateKeyTransformer
     */
    public HibernateKeyTransformer keyTransformer() {
        return keyTransformer;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return delegate.get().skipStore();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache setSkipStore(boolean skipStore) {
        return delegate.get().setSkipStore(skipStore);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.get().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return delegate.get().containsKey(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(Object key) {
        return delegate.get().containsKeyAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Collection keys) {
        return delegate.get().containsKey(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeysAsync(Collection keys) {
        return delegate.get().containsKeysAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object localPeek(
        Object key,
        CachePeekMode[] peekModes
    ) throws IgniteCheckedException {
        return delegate.get().localPeek(keyTransformer.transform(key), peekModes);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<Object, Object>> localEntries(
        CachePeekMode[] peekModes
    ) throws IgniteCheckedException {
        return delegate.get().localEntries(peekModes);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object get(Object key) throws IgniteCheckedException {
        return delegate.get().get(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntry getEntry(Object key) throws IgniteCheckedException {
        return delegate.get().getEntry(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAsync(Object key) {
        return delegate.get().getAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<CacheEntry<Object, Object>> getEntryAsync(Object key) {
        return delegate.get().getEntryAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public Map getAll(@Nullable Collection keys) throws IgniteCheckedException {
        return delegate.get().getAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<Object, Object>> getEntries(
        @Nullable Collection keys) throws IgniteCheckedException {
        return delegate.get().getEntries(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<Object, Object>> getAllAsync(@Nullable Collection keys) {
        return delegate.get().getAllAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Collection<CacheEntry<Object,Object>>> getEntriesAsync(
        @Nullable Collection keys
    ) {
        return delegate.get().getEntriesAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndPut(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().getAndPut(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndPutAsync(Object key, Object val) {
        return delegate.get().getAndPutAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean put(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().put(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putAsync(Object key, Object val) {
        return delegate.get().putAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndPutIfAbsent(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().getAndPutIfAbsent(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndPutIfAbsentAsync(Object key, Object val) {
        return delegate.get().getAndPutIfAbsentAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().putIfAbsent(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(Object key, Object val) {
        return delegate.get().putIfAbsentAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndReplace(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().getAndReplace(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndReplaceAsync(Object key, Object val) {
        return delegate.get().getAndReplaceAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().replace(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(Object key, Object val) {
        return delegate.get().replaceAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Object key, Object oldVal, Object newVal) throws IgniteCheckedException {
        return delegate.get().replace(keyTransformer.transform(key), oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(Object key, Object oldVal, Object newVal) {
        return delegate.get().replaceAsync(keyTransformer.transform(key), oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable Map m) throws IgniteCheckedException {
        delegate.get().putAll(transform(m));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(@Nullable Map m) {
        return delegate.get().putAllAsync(transform(m));
    }

    /** {@inheritDoc} */
    @Override public Set keySet() {
        return delegate.get().keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<Object, Object>> entrySet() {
        return delegate.get().entrySet();
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) {
        return delegate.get().txStart(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal txStartEx(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) {
        return delegate.get().txStartEx(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize
    ) {
        return delegate.get().txStart(concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNearTxLocal tx() {
        return delegate.get().tx();
    }

    /** {@inheritDoc} */
    @Override public boolean evict(Object key) {
        return delegate.get().evict(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection keys) {
        delegate.get().evictAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public void clearLocally(boolean srv, boolean near, boolean readers) {
        delegate.get().clearLocally(srv, near, readers);
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(Object key) {
        return delegate.get().clearLocally(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set keys, boolean srv, boolean near, boolean readers) {
        delegate.get().clearLocallyAll((Set<?>)transform(keys), srv, near, readers);
    }

    /** {@inheritDoc} */
    @Override public void clear(Object key) throws IgniteCheckedException {
        delegate.get().clear(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set keys) throws IgniteCheckedException {
        delegate.get().clearAll((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        delegate.get().clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        return delegate.get().clearAsync();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(Object key) {
        return delegate.get().clearAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAllAsync(Set keys) {
        return delegate.get().clearAllAsync((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndRemove(Object key) throws IgniteCheckedException {
        return delegate.get().getAndRemove(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndRemoveAsync(Object key) {
        return delegate.get().getAndRemoveAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object key) throws IgniteCheckedException {
        return delegate.get().remove(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(Object key) {
        return delegate.get().removeAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object key, Object val) throws IgniteCheckedException {
        return delegate.get().remove(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(Object key, Object val) {
        return delegate.get().removeAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection keys) throws IgniteCheckedException {
        delegate.get().removeAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection keys) {
        return delegate.get().removeAllAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws IgniteCheckedException {
        delegate.get().removeAll();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return delegate.get().removeAllAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Object key, long timeout) throws IgniteCheckedException {
        return delegate.get().lock(keyTransformer.transform(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(Object key, long timeout) {
        return delegate.get().lockAsync(keyTransformer.transform(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection keys, long timeout) throws IgniteCheckedException {
        return delegate.get().lockAll(transform(keys), timeout);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection keys, long timeout) {
        return delegate.get().lockAllAsync(transform(keys), timeout);
    }

    /** {@inheritDoc} */
    @Override public void unlock(Object key) throws IgniteCheckedException {
        delegate.get().unlock(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection keys) throws IgniteCheckedException {
        delegate.get().unlockAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(Object key) {
        return delegate.get().isLocked(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(Object key) {
        return delegate.get().isLockedByThread(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return delegate.get().size();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
        return delegate.get().sizeLong();
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.get().localSize(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.get().localSizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.get().localSizeLong(partition, peekModes);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.get().size(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.get().sizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.get().sizeLong(partition, peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes) {
        return delegate.get().sizeAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(CachePeekMode[] peekModes) {
        return delegate.get().sizeLongAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(int partition, CachePeekMode[] peekModes) {
        return delegate.get().sizeLongAsync(partition, peekModes);
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return delegate.get().nearSize();
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return delegate.get().primarySize();
    }

    /** {@inheritDoc} */
    @Override public long primarySizeLong() {
        return delegate.get().primarySizeLong();
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration configuration() {
        return delegate.get().configuration();
    }

    /** {@inheritDoc} */
    @Override public Affinity affinity() {
        return delegate.get().affinity();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics() {
        return delegate.get().clusterMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics(ClusterGroup grp) {
        return delegate.get().clusterMetrics(grp);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        return delegate.get().localMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean clusterMxBean() {
        return delegate.get().clusterMxBean();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        return delegate.get().localMxBean();
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        return delegate.get().offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        return delegate.get().offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebalance() {
        return delegate.get().rebalance();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache forSubjectId(UUID subjId) {
        return delegate.get().forSubjectId(subjId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getForcePrimary(Object key) throws IgniteCheckedException {
        return delegate.get().getForcePrimary(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getForcePrimaryAsync(Object key) {
        return delegate.get().getForcePrimaryAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public Map getAllOutTx(Set keys) throws IgniteCheckedException {
        return delegate.get().getAllOutTx((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<Object, Object>> getAllOutTxAsync(Set keys) {
        return delegate.get().getAllOutTxAsync((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public ExpiryPolicy expiry() {
        return delegate.get().expiry();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache withExpiryPolicy(ExpiryPolicy plc) {
        return delegate.get().withExpiryPolicy(plc);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache withNoRetries() {
        return delegate.get().withNoRetries();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteInternalCache<K1, V1> withAllowAtomicOpsInTx() {
        return delegate.get().withAllowAtomicOpsInTx();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext context() {
        return delegate.get().context();
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(
        @Nullable IgniteBiPredicate p,
        @Nullable Object... args
    ) throws IgniteCheckedException {
        delegate.get().localLoadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(
        @Nullable IgniteBiPredicate p,
        @Nullable Object... args
    ) {
        return delegate.get().localLoadCacheAsync(p, args);
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        return delegate.get().lostPartitions();
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int part) throws IgniteCheckedException {
        delegate.get().preloadPartition(part);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> preloadPartitionAsync(int part) throws IgniteCheckedException {
        return delegate.get().preloadPartitionAsync(part);
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int part) throws IgniteCheckedException {
        return delegate.get().localPreloadPartition(part);
    }

    /** {@inheritDoc} */
    @Nullable @Override public EntryProcessorResult invoke(
        @Nullable AffinityTopologyVersion topVer,
        Object key,
        EntryProcessor entryProcessor,
        Object... args
    ) throws IgniteCheckedException {
        return delegate.get().invoke(topVer, key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map> invokeAllAsync(Map map, Object... args) {
        return delegate.get().invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public Map invokeAll(Map map, Object... args) throws IgniteCheckedException {
        return delegate.get().invokeAll(map, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map> invokeAllAsync(Set keys, EntryProcessor entryProcessor, Object... args) {
        return delegate.get().invokeAllAsync((Set<?>)transform(keys), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public Map invokeAll(Set keys, EntryProcessor entryProcessor, Object... args) throws IgniteCheckedException {
        return delegate.get().invokeAll((Set<?>)transform(keys), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<EntryProcessorResult> invokeAsync(
        Object key,
        EntryProcessor entryProcessor,
        Object... args
    ) {
        return delegate.get().invokeAsync(keyTransformer.transform(key), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Nullable @Override public EntryProcessorResult invoke(
        Object key,
        EntryProcessor entryProcessor,
        Object... args
    ) throws IgniteCheckedException {
        return delegate.get().invoke(keyTransformer.transform(key), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<Object,Object>> scanIterator(
        boolean keepBinary,
        @Nullable IgniteBiPredicate p
    ) throws IgniteCheckedException {
        return delegate.get().scanIterator(keepBinary, p);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map drMap) throws IgniteCheckedException {
        return delegate.get().removeAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map drMap) throws IgniteCheckedException {
        delegate.get().removeAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map drMap) throws IgniteCheckedException {
        return delegate.get().putAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map drMap) throws IgniteCheckedException {
        delegate.get().putAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache keepBinary() {
        return delegate.get().keepBinary();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache cache() {
        return delegate.get().cache();
    }

    /** {@inheritDoc} */
    @Override public Iterator iterator() {
        return delegate.get().iterator();
    }

    /**
     * @param keys Keys.
     */
    private Collection<Object> transform(Collection<Object> keys) {
        Collection<Object> res = new LinkedList<>();

        for (Object o : keys)
            res.add(keyTransformer.transform(o));

        return res;
    }

    /**
     * @param map Map.
     */
    private Map<Object, Object> transform(Map<Object, Object> map) {
        Map<Object, Object> res = new HashMap<>();

        Set<Map.Entry<Object, Object>> ents = map.entrySet();

        for (Map.Entry<Object, Object> e : ents)
            res.put(keyTransformer.transform(e.getKey()), e.getValue());

        return res;
    }
}

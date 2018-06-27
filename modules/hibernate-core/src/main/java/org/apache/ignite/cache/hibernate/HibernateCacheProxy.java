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
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
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
    /** Delegate. */
    private final IgniteInternalCache<Object, Object> delegate;

    /** Transformer. */
    private final HibernateKeyTransformer keyTransformer;

    /**
     * @param delegate Delegate.
     * @param keyTransformer Key keyTransformer.
     */
    HibernateCacheProxy(
        IgniteInternalCache<Object, Object> delegate,
        HibernateKeyTransformer keyTransformer
    ) {
        assert delegate != null;
        assert keyTransformer != null;

        this.delegate = delegate;
        this.keyTransformer = keyTransformer;
    }

    /**
     * @return HibernateKeyTransformer
     */
    public HibernateKeyTransformer keyTransformer(){
        return keyTransformer;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return delegate.skipStore();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache setSkipStore(boolean skipStore) {
        return delegate.setSkipStore(skipStore);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return delegate.containsKey(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(Object key) {
        return delegate.containsKeyAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Collection keys) {
        return delegate.containsKey(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeysAsync(Collection keys) {
        return delegate.containsKeysAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object localPeek(
        Object key,
        CachePeekMode[] peekModes,
        @Nullable IgniteCacheExpiryPolicy plc
    ) throws IgniteCheckedException {
        return delegate.localPeek(keyTransformer.transform(key), peekModes, plc);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<Object, Object>> localEntries(
        CachePeekMode[] peekModes
    ) throws IgniteCheckedException {
        return delegate.localEntries(peekModes);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object get(Object key) throws IgniteCheckedException {
        return delegate.get(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntry getEntry(Object key) throws IgniteCheckedException {
        return delegate.getEntry(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAsync(Object key) {
        return delegate.getAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<CacheEntry<Object, Object>> getEntryAsync(Object key) {
        return delegate.getEntryAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public Map getAll(@Nullable Collection keys) throws IgniteCheckedException {
        return delegate.getAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<Object, Object>> getEntries(
        @Nullable Collection keys) throws IgniteCheckedException {
        return delegate.getEntries(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<Object, Object>> getAllAsync(@Nullable Collection keys) {
        return delegate.getAllAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Collection<CacheEntry<Object,Object>>> getEntriesAsync(
        @Nullable Collection keys
    ) {
        return delegate.getEntriesAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndPut(Object key, Object val) throws IgniteCheckedException {
        return delegate.getAndPut(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndPutAsync(Object key, Object val) {
        return delegate.getAndPutAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean put(Object key, Object val) throws IgniteCheckedException {
        return delegate.put(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putAsync(Object key, Object val) {
        return delegate.putAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndPutIfAbsent(Object key, Object val) throws IgniteCheckedException {
        return delegate.getAndPutIfAbsent(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndPutIfAbsentAsync(Object key, Object val) {
        return delegate.getAndPutIfAbsentAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(Object key, Object val) throws IgniteCheckedException {
        return delegate.putIfAbsent(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(Object key, Object val) {
        return delegate.putIfAbsentAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndReplace(Object key, Object val) throws IgniteCheckedException {
        return delegate.getAndReplace(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndReplaceAsync(Object key, Object val) {
        return delegate.getAndReplaceAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Object key, Object val) throws IgniteCheckedException {
        return delegate.replace(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(Object key, Object val) {
        return delegate.replaceAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Object key, Object oldVal, Object newVal) throws IgniteCheckedException {
        return delegate.replace(keyTransformer.transform(key), oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(Object key, Object oldVal, Object newVal) {
        return delegate.replaceAsync(keyTransformer.transform(key), oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable Map m) throws IgniteCheckedException {
        delegate.putAll(transform(m));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(@Nullable Map m) {
        return delegate.putAllAsync(transform(m));
    }

    /** {@inheritDoc} */
    @Override public Set keySet() {
        return delegate.keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<Object, Object>> entrySet() {
        return delegate.entrySet();
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) {
        return delegate.txStart(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal txStartEx(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) {
        return delegate.txStartEx(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize
    ) {
        return delegate.txStart(concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNearTxLocal tx() {
        return delegate.tx();
    }

    /** {@inheritDoc} */
    @Override public boolean evict(Object key) {
        return delegate.evict(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection keys) {
        delegate.evictAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public void clearLocally(boolean srv, boolean near, boolean readers) {
        delegate.clearLocally(srv, near, readers);
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(Object key) {
        return delegate.clearLocally(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set keys, boolean srv, boolean near, boolean readers) {
        delegate.clearLocallyAll((Set<?>)transform(keys), srv, near, readers);
    }

    /** {@inheritDoc} */
    @Override public void clear(Object key) throws IgniteCheckedException {
        delegate.clear(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set keys) throws IgniteCheckedException {
        delegate.clearAll((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        return delegate.clearAsync();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(Object key) {
        return delegate.clearAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAllAsync(Set keys) {
        return delegate.clearAllAsync((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getAndRemove(Object key) throws IgniteCheckedException {
        return delegate.getAndRemove(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getAndRemoveAsync(Object key) {
        return delegate.getAndRemoveAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object key) throws IgniteCheckedException {
        return delegate.remove(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(Object key) {
        return delegate.removeAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object key, Object val) throws IgniteCheckedException {
        return delegate.remove(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(Object key, Object val) {
        return delegate.removeAsync(keyTransformer.transform(key), val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection keys) throws IgniteCheckedException {
        delegate.removeAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection keys) {
        return delegate.removeAllAsync(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws IgniteCheckedException {
        delegate.removeAll();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return delegate.removeAllAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Object key, long timeout) throws IgniteCheckedException {
        return delegate.lock(keyTransformer.transform(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(Object key, long timeout) {
        return delegate.lockAsync(keyTransformer.transform(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection keys, long timeout) throws IgniteCheckedException {
        return delegate.lockAll(transform(keys), timeout);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection keys, long timeout) {
        return delegate.lockAllAsync(transform(keys), timeout);
    }

    /** {@inheritDoc} */
    @Override public void unlock(Object key) throws IgniteCheckedException {
        delegate.unlock(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection keys) throws IgniteCheckedException {
        delegate.unlockAll(transform(keys));
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(Object key) {
        return delegate.isLocked(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(Object key) {
        return delegate.isLockedByThread(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return delegate.size();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
        return delegate.sizeLong();
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.localSize(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.localSizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.localSizeLong(partition, peekModes);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.size(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.sizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return delegate.sizeLong(partition, peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes) {
        return delegate.sizeAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(CachePeekMode[] peekModes) {
        return delegate.sizeLongAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(int partition, CachePeekMode[] peekModes) {
        return delegate.sizeLongAsync(partition, peekModes);
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return delegate.nearSize();
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return delegate.primarySize();
    }

    /** {@inheritDoc} */
    @Override public long primarySizeLong() {
        return delegate.primarySizeLong();
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration configuration() {
        return delegate.configuration();
    }

    /** {@inheritDoc} */
    @Override public Affinity affinity() {
        return delegate.affinity();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics() {
        return delegate.clusterMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics(ClusterGroup grp) {
        return delegate.clusterMetrics(grp);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        return delegate.localMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean clusterMxBean() {
        return delegate.clusterMxBean();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        return delegate.localMxBean();
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        return delegate.offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        return delegate.offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebalance() {
        return delegate.rebalance();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache forSubjectId(UUID subjId) {
        return delegate.forSubjectId(subjId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object getForcePrimary(Object key) throws IgniteCheckedException {
        return delegate.getForcePrimary(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture getForcePrimaryAsync(Object key) {
        return delegate.getForcePrimaryAsync(keyTransformer.transform(key));
    }

    /** {@inheritDoc} */
    @Override public Map getAllOutTx(Set keys) throws IgniteCheckedException {
        return delegate.getAllOutTx((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<Object, Object>> getAllOutTxAsync(Set keys) {
        return delegate.getAllOutTxAsync((Set<?>)transform(keys));
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsDataCache() {
        return delegate.isIgfsDataCache();
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceUsed() {
        return delegate.igfsDataSpaceUsed();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoDataCache() {
        return delegate.isMongoDataCache();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoMetaCache() {
        return delegate.isMongoMetaCache();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ExpiryPolicy expiry() {
        return delegate.expiry();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache withExpiryPolicy(ExpiryPolicy plc) {
        return delegate.withExpiryPolicy(plc);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache withNoRetries() {
        return delegate.withNoRetries();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteInternalCache<K1, V1> withAllowAtomicOpsInTx() {
        return delegate.withAllowAtomicOpsInTx();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext context() {
        return delegate.context();
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(
        @Nullable IgniteBiPredicate p,
        @Nullable Object... args
    ) throws IgniteCheckedException {
        delegate.localLoadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(
        @Nullable IgniteBiPredicate p,
        @Nullable Object... args
    ) {
        return delegate.localLoadCacheAsync(p, args);
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        return delegate.lostPartitions();
    }

    /** {@inheritDoc} */
    @Nullable @Override public EntryProcessorResult invoke(
        @Nullable AffinityTopologyVersion topVer,
        Object key,
        EntryProcessor entryProcessor,
        Object... args
    ) throws IgniteCheckedException {
        return delegate.invoke(topVer, key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map> invokeAllAsync(Map map, Object... args) {
        return delegate.invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public Map invokeAll(Map map, Object... args) throws IgniteCheckedException {
        return delegate.invokeAll(map, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map> invokeAllAsync(Set keys, EntryProcessor entryProcessor, Object... args) {
        return delegate.invokeAllAsync((Set<?>)transform(keys), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public Map invokeAll(Set keys, EntryProcessor entryProcessor, Object... args) throws IgniteCheckedException {
        return delegate.invokeAll((Set<?>)transform(keys), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<EntryProcessorResult> invokeAsync(
        Object key,
        EntryProcessor entryProcessor,
        Object... args
    ) {
        return delegate.invokeAsync(keyTransformer.transform(key), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Nullable @Override public EntryProcessorResult invoke(
        Object key,
        EntryProcessor entryProcessor,
        Object... args
    ) throws IgniteCheckedException {
        return delegate.invoke(keyTransformer.transform(key), entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<Object,Object>> scanIterator(
        boolean keepBinary,
        @Nullable IgniteBiPredicate p
    ) throws IgniteCheckedException {
        return delegate.scanIterator(keepBinary, p);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map drMap) throws IgniteCheckedException {
        return delegate.removeAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map drMap) throws IgniteCheckedException {
        delegate.removeAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map drMap) throws IgniteCheckedException {
        return delegate.putAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map drMap) throws IgniteCheckedException {
        delegate.putAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache keepBinary() {
        return delegate.keepBinary();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache cache() {
        return delegate.cache();
    }

    /** {@inheritDoc} */
    @Override public Iterator iterator() {
        return delegate.iterator();
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

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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.affinity.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;

/**
 * Cache proxy.
 */
public class GridCacheProxyImpl<K, V> implements GridCacheProxy<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private GridCacheContext<K, V> ctx;

    /** Gateway. */
    private GridCacheGateway<K, V> gate;

    /** Cache. */
    @GridToStringInclude
    private GridCacheAdapter<K, V> cache;

    /** Delegate object. */
    @GridToStringExclude
    private GridCacheProjectionEx<K, V> delegate;

    /** Projection. */
    @GridToStringExclude
    private GridCacheProjectionImpl<K, V> prj;

    /** Cache queries. */
    private CacheQueries<K, V> qry;

    /** Affinity. */
    private Affinity<K> aff;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheProxyImpl() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate object.
     * @param prj Optional projection which will be passed to gateway.
     */
    public GridCacheProxyImpl(GridCacheContext<K, V> ctx, GridCacheProjectionEx<K, V> delegate,
        @Nullable GridCacheProjectionImpl<K, V> prj) {
        assert ctx != null;
        assert delegate != null;

        this.ctx = ctx;
        this.delegate = delegate;
        this.prj = prj;

        gate = ctx.gate();
        cache = ctx.cache();

        qry = new GridCacheQueriesProxy<>(ctx, prj, (GridCacheQueriesEx<K, V>)delegate.queries());
        aff = new GridCacheAffinityProxy<>(ctx, ctx.cache().affinity());
    }

    /**
     * @return Cache context.
     */
    @Override public GridCacheContext context() {
        return ctx;
    }

    /**
     * @return Proxy delegate.
     */
    public GridCacheProjectionEx<K, V> delegate() {
        return delegate;
    }

    /**
     * @return Gateway projection.
     */
    public GridCacheProjectionImpl<K, V> gateProjection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.name();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> GridCache<K1, V1> cache() {
        return cache.cache();
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.skipStore();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQueries<K, V> queries() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public Affinity<K> affinity() {
        return aff;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration configuration() {
        return cache.configuration();
    }

    /** {@inheritDoc} */
    @Override public void txSynchronize(@Nullable TransactionSynchronization syncs) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            cache.txSynchronize(syncs);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void txUnsynchronize(@Nullable TransactionSynchronization syncs) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            cache.txUnsynchronize(syncs);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<TransactionSynchronization> txSynchronizations() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.txSynchronizations();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.metrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.mxBean();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long overflowSize() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.overflowSize();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(IgniteBiPredicate<K, V> p, @Nullable Object[] args) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            cache.localLoadCache(p, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(IgniteBiPredicate<K, V> p, @Nullable Object[] args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.localLoadCacheAsync(p, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Cache.Entry<K, V> randomEntry() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.randomEntry();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean affinityNode() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.affinityNode();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> forSubjectId(UUID subjId) {
        return delegate.forSubjectId(subjId);
    }

    /** {@inheritDoc} */
    @Override public CacheProjection<K, V> setSkipStore(boolean skipStore) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.setSkipStore(skipStore);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> CacheProjection<K1, V1> keepPortable() {
        return delegate.keepPortable();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.containsKey(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.containsKeys(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.containsKeyAsync(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeysAsync(Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.containsKeysAsync(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(K key) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.get(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAsync(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getForcePrimary(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getForcePrimaryAsync(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getForcePrimaryAsync(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(List<K> keys) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAllOutTx(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsDataCache() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isIgfsDataCache();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceUsed() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.igfsDataSpaceUsed();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceMax() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.igfsDataSpaceMax();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoDataCache() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isMongoDataCache();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoMetaCache() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isMongoMetaCache();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAllAsync(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPut(K key, V val)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndPut(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndPutAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean put(K key, V val)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.put(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> drMap) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.putAllConflict(drMap);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.putAllConflictAsync(drMap);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.invoke(key, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.invokeAsync(key, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.invokeAll(keys, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.invokeAllAsync(keys, entryProcessor, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.invokeAll(map, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.invokeAllAsync(map, args);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.putAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndPutIfAbsent(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutIfAbsentAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndPutIfAbsentAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.putIfAbsent(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.putIfAbsentAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndReplace(K key, V val) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndReplace(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndReplaceAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndReplaceAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.replace(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.replaceAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.replace(key, oldVal, newVal);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.replaceAsync(key, oldVal, newVal);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.putAll(m);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(@Nullable Map<? extends K, ? extends V> m) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.putAllAsync(m);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.keySet();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<K> primaryKeySet() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.primaryKeySet();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.values();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.entrySet();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(int part) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.entrySet(part);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySetx(CacheEntryPredicate... filter) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.entrySetx(filter);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalTx txStartEx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.txStartEx(concurrency, isolation);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.txStart(concurrency, isolation);
        }
        finally {
            gate.leave(prev);
        }

    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout, int txSize) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.txStart(concurrency, isolation, timeout, txSize);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Transaction tx() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.tx();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key,
        CachePeekMode[] peekModes,
        @Nullable IgniteCacheExpiryPolicy plc)
        throws IgniteCheckedException
    {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.localPeek(key, peekModes, plc);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.localEntries(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.evict(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.evictAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearLocally() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.clearLocally();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.clear();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.clearAsync();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.clearAsync(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.clearAsync(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.clearLocally(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.clearLocallyAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.clear(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.clearAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndRemove(K key)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndRemove(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.getAndRemoveAsync(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.remove(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.removeAllConflict(drMap);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException
    {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removeAllConflictAsync(drMap);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removeAsync(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn> replacexAsync(K key, V oldVal, V newVal) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.replacexAsync(key, oldVal, newVal);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.replacex(key, oldVal, newVal);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn removex(K key, V val) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removex(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn> removexAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removexAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.remove(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removeAsync(key, val);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.removeAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removeAllAsync(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll()
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.removeAll();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.removeAllAsync();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout)
        throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.lock(key, timeout);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.lockAsync(key, timeout);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.lockAll(keys, timeout);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys, long timeout) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.lockAllAsync(keys, timeout);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.unlock(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.unlockAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isLocked(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.isLockedByThread(key);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.size();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.size(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.sizeAsync(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.localSize(peekModes);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.nearSize();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.primarySize();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.promoteAll(keys);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> swapIterator() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.swapIterator();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> offHeapIterator() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.offHeapIterator();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.offHeapEntriesCount();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.offHeapAllocatedSize();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long swapSize() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.swapSize();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long swapKeys() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.swapKeys();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.iterator();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> forceRepartition() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return cache.forceRepartition();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
        out.writeObject(delegate);
        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();
        delegate = (GridCacheProjectionEx<K, V>)in.readObject();
        prj = (GridCacheProjectionImpl<K, V>)in.readObject();

        gate = ctx.gate();
        cache = ctx.cache();

        gate = ctx.gate();
        cache = ctx.cache();

        qry = new GridCacheQueriesProxy<>(ctx, prj, (GridCacheQueriesEx<K, V>)delegate.queries());
        aff = new GridCacheAffinityProxy<>(ctx, ctx.cache().affinity());
    }

    /** {@inheritDoc} */
    @Nullable @Override public ExpiryPolicy expiry() {
        return delegate.expiry();
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            GridCacheProjectionEx<K, V> prj0 = prj != null ? prj.withExpiryPolicy(plc) : delegate.withExpiryPolicy(plc);

            return new GridCacheProxyImpl<>(ctx, prj0, (GridCacheProjectionImpl<K, V>)prj0);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheProxyImpl.class, this);
    }
}

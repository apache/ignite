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
import org.apache.ignite.internal.processors.cache.affinity.GridCacheAffinityProxy;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.CacheOperationContext.DFLT_ALLOW_ATOMIC_OPS_IN_TX;

/**
 * Cache proxy.
 */
public class GridCacheProxyImpl<K, V> implements IgniteInternalCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private GridCacheContext<K, V> ctx;

    /** Gateway. */
    private GridCacheGateway<K, V> gate;

    /** Delegate object. */
    @GridToStringExclude
    private IgniteInternalCache<K, V> delegate;

    /** Projection. */
    @GridToStringExclude
    private CacheOperationContext opCtx;

    /** Affinity. */
    private Affinity<K> aff;

    /** If {@code false} does not acquire read lock on gateway enter. */
    @GridToStringExclude private boolean lock;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheProxyImpl() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate object.
     * @param opCtx Optional operation context which will be passed to gateway.
     */
    public GridCacheProxyImpl(
        GridCacheContext<K, V> ctx,
        IgniteInternalCache<K, V> delegate,
        @Nullable CacheOperationContext opCtx
    ) {
        this(ctx, delegate, opCtx, true);
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate object.
     * @param opCtx Optional operation context which will be passed to gateway.
     * @param lock Take read-lock on gateway if is set to {@code true}.
     */
    public GridCacheProxyImpl(
        GridCacheContext<K, V> ctx,
        IgniteInternalCache<K, V> delegate,
        @Nullable CacheOperationContext opCtx,
        boolean lock
    ) {
        assert ctx != null;
        assert delegate != null;

        this.ctx = ctx;
        this.delegate = delegate;
        this.opCtx = opCtx;
        this.lock = lock;

        gate = ctx.gate();

        aff = new GridCacheAffinityProxy<>(ctx, ctx.cache().affinity());
    }

    /**
     * Enter gateway.
     *
     * @param opCtx Current operation context.
     * @return Previous operation context.
     */
    @Nullable private CacheOperationContext onEnter(CacheOperationContext opCtx) {
        return lock ? gate.enter(opCtx) : gate.enterNoLock(opCtx);
    }

    /**
     * Leave gateway.
     *
     * @param prevOpCtx Previous operation context.
     */
    private void onLeave(CacheOperationContext prevOpCtx) {
        if (lock)
            gate.leave(prevOpCtx);
        else
            gate.leaveNoLock(prevOpCtx);
    }

    /**
     * Projection with no gateway lock.
     */
    public GridCacheProxyImpl<K, V> proxyNoGate() {
        return new GridCacheProxyImpl<>(ctx, delegate, opCtx, false);
    }

    /**
     * @return Cache context.
     */
    @Override public GridCacheContext<K, V> context() {
        return ctx;
    }

    /**
     * @return Proxy delegate.
     */
    public IgniteInternalCache<K, V> delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteInternalCache<K1, V1> cache() {
        return delegate.cache();
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return opCtx != null && opCtx.skipStore();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Affinity<K> affinity() {
        return aff;
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clusterMetrics();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics(ClusterGroup grp) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clusterMetrics(grp);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration configuration() {
        return delegate.configuration();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localMetrics();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean clusterMxBean() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clusterMxBean();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localMxBean();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(IgniteBiPredicate<K, V> p,
        @Nullable Object[] args) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.localLoadCache(p, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(IgniteBiPredicate<K, V> p, @Nullable Object[] args) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localLoadCacheAsync(p, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int part) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.preloadPartition(part);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> preloadPartitionAsync(int part) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.preloadPartitionAsync(part);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int part) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localPreloadPartition(part);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> forSubjectId(UUID subjId) {
        return new GridCacheProxyImpl<>(ctx, delegate,
            opCtx != null ? opCtx.forSubjectId(subjId) :
                new CacheOperationContext(false, subjId, false, null, false, null, false, DFLT_ALLOW_ATOMIC_OPS_IN_TX));
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> setSkipStore(boolean skipStore) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            if (opCtx != null && opCtx.skipStore() == skipStore)
                return this;

            return new GridCacheProxyImpl<>(ctx, delegate,
                opCtx != null ? opCtx.setSkipStore(skipStore) :
                    new CacheOperationContext(true, null, false, null, false, null, false, DFLT_ALLOW_ATOMIC_OPS_IN_TX));
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K1, V1> GridCacheProxyImpl<K1, V1> keepBinary() {
        if (opCtx != null && opCtx.isKeepBinary())
            return (GridCacheProxyImpl<K1, V1>)this;

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)ctx,
            (GridCacheAdapter<K1, V1>)delegate,
            opCtx != null ? opCtx.keepBinary() :
                new CacheOperationContext(false, null, true, null, false, null, false, DFLT_ALLOW_ATOMIC_OPS_IN_TX));
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.containsKey(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Collection<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.containsKeys(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.containsKeyAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeysAsync(Collection<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.containsKeysAsync(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(K key) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.get(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntry<K, V> getEntry(K key) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getEntry(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<CacheEntry<K, V>> getEntryAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getEntryAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getForcePrimary(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getForcePrimaryAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getForcePrimaryAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAllOutTx(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAllOutTxAsync(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsDataCache() {
        CacheOperationContext prev = gate.enter(opCtx);

        try {
            return delegate.isIgfsDataCache();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceUsed() {
        CacheOperationContext prev = gate.enter(opCtx);

        try {
            return delegate.igfsDataSpaceUsed();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAll(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(
        @Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getEntries(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAllAsync(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(
        @Nullable Collection<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getEntriesAsync(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPut(K key, V val)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndPut(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndPutAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean put(K key, V val)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.put(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> drMap) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.putAllConflict(drMap);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.putAllConflictAsync(drMap);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invoke(key, entryProcessor, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invokeAsync(key, entryProcessor, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invokeAll(keys, entryProcessor, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invokeAllAsync(keys, entryProcessor, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invokeAll(map, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invokeAllAsync(map, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.putAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndPutIfAbsent(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutIfAbsentAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndPutIfAbsentAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.putIfAbsent(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.putIfAbsentAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndReplace(K key, V val) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndReplace(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndReplaceAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndReplaceAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.replace(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.replaceAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.replace(key, oldVal, newVal);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.replaceAsync(key, oldVal, newVal);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.putAll(m);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(@Nullable Map<? extends K, ? extends V> m) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.putAllAsync(m);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.keySet();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.entrySet();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> scanIterator(boolean keepBinary,
        @Nullable IgniteBiPredicate<Object, Object> p) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.scanIterator(keepBinary, p);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal txStartEx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.txStartEx(concurrency, isolation);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.txStart(concurrency, isolation);
        }
        finally {
            onLeave(prev);
        }

    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout, int txSize) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.txStart(concurrency, isolation, timeout, txSize);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNearTxLocal tx() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.tx();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key,
        CachePeekMode[] peekModes)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localPeek(key, peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localEntries(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.evict(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.evictAll(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearLocally(boolean srv, boolean near, boolean readers) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.clearLocally(srv, near, readers);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.clear();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clearAsync();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clearAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAllAsync(Set<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clearAllAsync(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.clearLocally(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set<? extends K> keys, boolean srv, boolean near, boolean readers) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.clearLocallyAll(keys, srv, near, readers);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.clear(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.clearAll(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndRemove(K key)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndRemove(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.getAndRemoveAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.remove(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.removeAllConflict(drMap);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.removeAllConflictAsync(drMap);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.removeAsync(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.remove(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.removeAsync(key, val);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.removeAll(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection<? extends K> keys) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.removeAllAsync(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> EntryProcessorResult<T> invoke(
        AffinityTopologyVersion topVer,
        K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.invoke(topVer, key, entryProcessor, args);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll()
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.removeAll();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.removeAllAsync();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout)
        throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.lock(key, timeout);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.lockAsync(key, timeout);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys,
        long timeout) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.lockAll(keys, timeout);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys, long timeout) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.lockAllAsync(keys, timeout);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.unlock(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            delegate.unlockAll(keys);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.isLocked(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.isLockedByThread(key);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.size();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.sizeLong();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.size(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.sizeLong(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.sizeLong(partition, peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.sizeAsync(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(CachePeekMode[] peekModes) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.sizeLongAsync(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(int partition, CachePeekMode[] peekModes) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.sizeLongAsync(partition, peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localSize(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localSizeLong(peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.localSizeLong(partition, peekModes);
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.nearSize();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.primarySize();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long primarySizeLong() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.primarySizeLong();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.offHeapEntriesCount();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.offHeapAllocatedSize();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.iterator();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebalance() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.rebalance();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public ExpiryPolicy expiry() {
        return opCtx != null ? opCtx.expiry() : null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return new GridCacheProxyImpl<>(ctx, delegate,
                opCtx != null ? opCtx.withExpiryPolicy(plc) :
                    new CacheOperationContext(false, null, false, plc, false, null, false, DFLT_ALLOW_ATOMIC_OPS_IN_TX));
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache<K, V> withNoRetries() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return new GridCacheProxyImpl<>(ctx, delegate,
                new CacheOperationContext(false, null, false, null, true, null, false, DFLT_ALLOW_ATOMIC_OPS_IN_TX));
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return delegate.lostPartitions();
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalCache<K, V> withAllowAtomicOpsInTx() {
        CacheOperationContext prev = onEnter(opCtx);

        try {
            return new GridCacheProxyImpl<>(ctx, delegate, opCtx.setAllowAtomicOpsInTx());
        }
        finally {
            onLeave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
        out.writeObject(delegate);
        out.writeObject(opCtx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();
        delegate = (IgniteInternalCache<K, V>)in.readObject();
        opCtx = (CacheOperationContext)in.readObject();

        gate = ctx.gate();

        aff = new GridCacheAffinityProxy<>(ctx, ctx.cache().affinity());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheProxyImpl.class, this);
    }
}

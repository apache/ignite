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
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 * Cache projection.
 */
public class GridCacheProjectionImpl<K, V> implements GridCacheProjectionEx<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Base cache. */
    private GridCacheAdapter<K, V> cache;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Queries impl. */
    private CacheQueries<K, V> qry;

    /** Skip store. */
    @GridToStringInclude
    private boolean skipStore;

    /** Client ID which operates over this projection, if any, */
    private UUID subjId;

    /** */
    private boolean keepPortable;

    /** */
    private ExpiryPolicy expiryPlc;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheProjectionImpl() {
        // No-op.
    }

    /**
     * @param parent Parent projection.
     * @param cctx Cache context.
     * @param skipStore Skip store flag.
     * @param subjId Subject ID.
     * @param keepPortable Keep portable flag.
     * @param expiryPlc Expiry policy.
     */
    public GridCacheProjectionImpl(
        CacheProjection<K, V> parent,
        GridCacheContext<K, V> cctx,
        boolean skipStore,
        @Nullable UUID subjId,
        boolean keepPortable,
        @Nullable ExpiryPolicy expiryPlc) {
        assert parent != null;
        assert cctx != null;

        this.cctx = cctx;

        this.skipStore = skipStore;

        this.subjId = subjId;

        cache = cctx.cache();

        qry = new GridCacheQueriesImpl<>(cctx, this);

        this.keepPortable = keepPortable;

        this.expiryPlc = expiryPlc;
    }

    /**
     * Gets cache context.
     *
     * @return Cache context.
     */
    @Override public GridCacheContext<K, V> context() {
        return cctx;
    }

    /**
     * @return Keep portable flag.
     */
    public boolean isKeepPortable() {
        return keepPortable;
    }

    /**
     * @return {@code True} if portables should be deserialized.
     */
    public boolean deserializePortables() {
        return !keepPortable;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public <K1, V1> GridCache<K1, V1> cache() {
        return (GridCache<K1, V1>)cctx.cache();
    }

    /** {@inheritDoc} */
    @Override public CacheQueries<K, V> queries() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> forSubjectId(UUID subjId) {
        A.notNull(subjId, "subjId");

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            skipStore,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /**
     * Gets client ID for which this projection was created.
     *
     * @return Client ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @Override public CacheProjection<K, V> setSkipStore(boolean skipStore) {
        if (this.skipStore == skipStore)
            return new GridCacheProxyImpl<>(cctx, this, this);

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this,
            cctx,
            skipStore,
            subjId,
            keepPortable,
            expiryPlc);

        return new GridCacheProxyImpl<>(cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> CacheProjection<K1, V1> keepPortable() {
        GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>(
            (CacheProjection<K1, V1>)this,
            (GridCacheContext<K1, V1>)cctx,
            skipStore,
            subjId,
            true,
            expiryPlc);

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)cctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return keySet().size();
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.localSize(peekModes);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.size(peekModes);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes) {
        return cache.sizeAsync(peekModes);
    }

    /** {@inheritDoc} */
    @Override public int globalSize() throws IgniteCheckedException {
        return cache.globalSize();
    }

    /** {@inheritDoc} */
    @Override public int globalPrimarySize() throws IgniteCheckedException {
        return cache.globalPrimarySize();
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return cctx.config().getCacheMode() == PARTITIONED && isNearEnabled(cctx) ?
             cctx.near().nearKeySet(null).size() : 0;
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return primaryKeySet().size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cache.isEmpty() || size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(K key) {
        return cache.containsKeyAsync(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Collection<? extends K> keys) {
        return cache.containsKeys(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeysAsync(Collection<? extends K> keys) {
        return cache.containsKeysAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V val) {
        return cache.containsValue(val);
    }

    /** {@inheritDoc} */
    @Override public V reload(K key) throws IgniteCheckedException {
        return cache.reload(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> reloadAsync(K key) {
        return cache.reloadAsync(key);
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws IgniteCheckedException {
        return cache.get(key, deserializePortables());
    }

    /** {@inheritDoc} */
    @Override public V get(K key, @Nullable GridCacheEntryEx entry, boolean deserializePortable,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return cache.get(key, entry, deserializePortable, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(K key) {
        return cache.getAsync(key, deserializePortables());
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws IgniteCheckedException {
        return cache.getForcePrimary(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getForcePrimaryAsync(K key) {
        return cache.getForcePrimaryAsync(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(List<K> keys) throws IgniteCheckedException {
        return cache.getAllOutTx(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(List<K> keys) {
        return cache.getAllOutTxAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsDataCache() {
        return cache.isIgfsDataCache();
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceUsed() {
        return cache.igfsDataSpaceUsed();
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceMax() {
        return cache.igfsDataSpaceMax();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoDataCache() {
        return cache.isMongoDataCache();
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoMetaCache() {
        return cache.isMongoMetaCache();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        return cache.getAll(keys, deserializePortables());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys) {
        return cache.getAllAsync(keys, deserializePortables());
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable CacheEntryPredicate[] filter)
        throws IgniteCheckedException {
        return putAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return cache.put(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putAsync(K key, V val,
        @Nullable CacheEntryPredicate[] filter) {
        return putAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate[] filter) {
        A.notNull(key, "key", val, "val");

        return cache.putAsync(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return cache.putx(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(K key, V val,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return putxAsync(key, val, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> drMap) throws IgniteCheckedException {
        cache.putAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException {
        return cache.putAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws IgniteCheckedException {
        return cache.invoke(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return cache.invokeAll(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return cache.invokeAsync(key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return cache.invokeAllAsync(keys, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException {
        return cache.invokeAll(map, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        return cache.invokeAllAsync(map, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxAsync(K key, V val,
        @Nullable CacheEntryPredicate[] filter) {
        return putxAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx entry,
        long ttl, @Nullable CacheEntryPredicate[] filter) {
        A.notNull(key, "key", val, "val");

        return cache.putxAsync(key, val, entry, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) throws IgniteCheckedException {
        return putIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> putIfAbsentAsync(K key, V val) {
        return putAsync(key, val, cctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(K key, V val) throws IgniteCheckedException {
        return putxIfAbsentAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putxIfAbsentAsync(K key, V val) {
        return putxAsync(key, val, cctx.noValArray());
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) throws IgniteCheckedException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> replaceAsync(K key, V val) {
        return putAsync(key, val, cctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(K key, V val) throws IgniteCheckedException {
        return replacexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replacexAsync(K key, V val) {
        return putxAsync(key, val, cctx.hasValArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return replaceAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        CacheEntryPredicate fltr = cctx.equalsValue(oldVal);

        return cache.putxAsync(key, newVal, fltr);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        putAllAsync(m, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(Map<? extends K, ? extends V> m,
        @Nullable CacheEntryPredicate[] filter) {
        return cache.putAllAsync(m, filter);
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        return cache.keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet(@Nullable CacheEntryPredicate... filter) {
        return cache.keySet(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<K> primaryKeySet() {
        return cache.primaryKeySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values() {
        return cache.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<V> primaryValues() {
        return cache.primaryValues();
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet() {
        return cache.entrySet();
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySetx(CacheEntryPredicate... filter) {
        return cache.entrySetx(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> primaryEntrySetx(CacheEntryPredicate... filter) {
        return cache.primaryEntrySetx(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(int part) {
        // TODO pass entry filter.
        return cache.entrySet(part);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> primaryEntrySet() {
        return cache.primaryEntrySet();
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.name();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup gridProjection() {
        return cache.gridProjection();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key,
        CachePeekMode[] peekModes,
        @Nullable IgniteCacheExpiryPolicy plc)
        throws IgniteCheckedException
    {
        return cache.localPeek(key, peekModes, plc);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return cache.localEntries(peekModes);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Cache.Entry<K, V> entry(K key) {
        return cache.entry(key);
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        return cache.evict(key);
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection<? extends K> keys) {
        cache.evictAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void evictAll() {
        cache.evictAll(keySet());
    }

    /** {@inheritDoc} */
    @Override public void clearLocally() {
        cache.clearLocally();
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set<? extends K> keys) {
        cache.clearLocallyAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) throws IgniteCheckedException {
        cache.clear(key);
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) throws IgniteCheckedException {
        cache.clearAll(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(K key) {
        return cache.clearAsync(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(Set<? extends K> keys) {
        return cache.clearAsync(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        return cache.clearAsync();
    }

    /** {@inheritDoc} */
    @Override public void clear(long timeout) throws IgniteCheckedException {
        cache.clear(timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        return cache.clearLocally0(key);
    }

    /** {@inheritDoc} */
    @Override public V remove(K key,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return removeAsync(key, filter).get();
    }

    /** {@inheritDoc} */
    @Override public V remove(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return removeAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> removeAsync(K key, CacheEntryPredicate[] filter) {
        return removeAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) {
        return cache.removeAsync(key, entry, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return removexAsync(key, filter).get();
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> drMap) throws IgniteCheckedException {
        cache.removeAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        return cache.removeAllConflictAsync(drMap);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return removexAsync(key, entry, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removexAsync(K key,
        @Nullable CacheEntryPredicate[] filter) {
        return removexAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) {
        return cache.removexAsync(key, entry, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn> replacexAsync(K key, V oldVal, V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        return cache.replacexAsync(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn replacex(K key, V oldVal, V newVal) throws IgniteCheckedException {
        return replacexAsync(key, oldVal, newVal).get();
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn removex(K key, V val) throws IgniteCheckedException {
        return removexAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn> removexAsync(K key, V val) {
        return cache.removexAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) throws IgniteCheckedException {
        return removeAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key, V val) {
        return cache.removeAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable Collection<? extends K> keys,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        cache.removeAll(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection<? extends K> keys,
        @Nullable CacheEntryPredicate[] filter) {
        return cache.removeAllAsync(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public void removeAll()
        throws IgniteCheckedException {
        removeAllAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return cache.removeAllAsync();
    }

    /** {@inheritDoc} */
    @Override public void localRemoveAll() throws IgniteCheckedException {
        cache.localRemoveAll();
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException {
        return cache.lock(key, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout,
        @Nullable CacheEntryPredicate[] filter) {
        return cache.lockAsync(key, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return cache.lockAll(keys, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable CacheEntryPredicate[] filter) {
        return cache.lockAllAsync(keys, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key, CacheEntryPredicate[] filter) throws IgniteCheckedException {
        cache.unlock(key, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(@Nullable Collection<? extends K> keys,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        cache.unlockAll(keys, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        return cache.isLocked(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        return cache.isLockedByThread(key);
    }

    /** {@inheritDoc} */
    @Override public V promote(K key) throws IgniteCheckedException {
        return cache.promote(key, deserializePortables());
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        cache.promoteAll(keys);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart() throws IllegalStateException {
        return cache.txStart();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalTx txStartEx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        return cache.txStartEx(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        return cache.txStart(concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout, int txSize) {
        return cache.txStart(concurrency, isolation, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public Transaction tx() {
        return cache.tx();
    }

    /** {@inheritDoc} */
    @Override public ConcurrentMap<K, V> toMap() {
        return new GridCacheMapAdapter<>(this);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        return cache.entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws IgniteCheckedException {
        cache.localLoadCache(p, args);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        return cache.localLoadCacheAsync(p, args);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        return new GridCacheProjectionImpl<>(
            this,
            cctx,
            skipStore,
            subjId,
            true,
            plc);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);

        out.writeBoolean(skipStore);

        out.writeBoolean(keepPortable);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cctx = (GridCacheContext<K, V>)in.readObject();

        skipStore = in.readBoolean();

        cache = cctx.cache();

        qry = new GridCacheQueriesImpl<>(cctx, this);

        keepPortable = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheProjectionImpl.class, this);
    }
}

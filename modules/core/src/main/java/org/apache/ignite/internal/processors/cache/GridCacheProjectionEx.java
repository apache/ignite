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
import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.util.*;

/**
 * Internal projection interface.
 */
public interface GridCacheProjectionEx<K, V> extends CacheProjection<K, V> {
    /**
     * Creates projection for specified subject ID.
     *
     * @param subjId Client ID.
     * @return Internal projection.
     */
    GridCacheProjectionEx<K, V> forSubjectId(UUID subjId);

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Cached entry. If not provided, equivalent to {CacheProjection#put}.
     * @param ttl Optional time-to-live. If negative, leaves ttl value unchanged.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V put(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException;

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Optional cached entry.
     * @param ttl Optional time-to-live value. If negative, leaves ttl value unchanged.
     * @param filter Optional filter.
     * @return Put operation future.
     */
    public IgniteInternalFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate... filter);

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Cached entry. If not provided, equivalent to {CacheProjection#put}.
     * @param ttl Optional time-to-live. If negative, leaves ttl value unchanged.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    public boolean putx(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException;

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Cached entry. If not provided, equivalent to {CacheProjection#put}.
     * @param ttl Optional time-to-live. If negative, leave ttl value unchanged.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public IgniteInternalFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx entry, long ttl,
        @Nullable CacheEntryPredicate... filter);

    /**
     * Store DR data.
     *
     * @param drMap DR map.
     * @throws IgniteCheckedException If put operation failed.
     */
    public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> drMap) throws IgniteCheckedException;

    /**
     * Store DR data asynchronously.
     *
     * @param drMap DR map.
     * @return Future.
     * @throws IgniteCheckedException If put operation failed.
     */
    public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException;

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Cached entry. If not provided, equivalent to {CacheProjection#put}.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V remove(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException;

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Optional cached entry.
     * @param filter Optional filter.
     * @return Put operation future.
     */
    public IgniteInternalFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter);

    /**
     * Removes DR data.
     *
     * @param drMap DR map.
     * @throws IgniteCheckedException If remove failed.
     */
    public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> drMap) throws IgniteCheckedException;

    /**
     * Removes DR data asynchronously.
     *
     * @param drMap DR map.
     * @return Future.
     * @throws IgniteCheckedException If remove failed.
     */
    public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> drMap) throws IgniteCheckedException;

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Cached entry. If not provided, equivalent to {CacheProjection#put}.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    public boolean removex(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException;

    /**
     * Internal method that is called from {@link CacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Cached entry. If not provided, equivalent to {CacheProjection#put}.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public IgniteInternalFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx entry,
        @Nullable CacheEntryPredicate... filter);

    /**
     * Asynchronously stores given key-value pair in cache only if only if the previous value is equal to the
     * {@code 'oldVal'} passed in.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via {@link CacheStore#write(javax.cache.Cache.Entry)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param oldVal Old value to match.
     * @param newVal Value to be associated with the given key.
     * @return Future for the replace operation. The future will return object containing actual old value and success
     *      flag.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<GridCacheReturn> replacexAsync(K key, V oldVal, V newVal);

    /**
     * Stores given key-value pair in cache only if only if the previous value is equal to the
     * {@code 'oldVal'} passed in.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via {@link CacheStore#write(javax.cache.Cache.Entry)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param oldVal Old value to match.
     * @param newVal Value to be associated with the given key.
     * @return Object containing actual old value and success flag.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If replace operation failed.
     */
    public GridCacheReturn replacex(K key, V oldVal, V newVal) throws IgniteCheckedException;

    /**
     * Removes given key mapping from cache if one exists and value is equal to the passed in value.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via {@link CacheStore#delete(Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @param val Value to match against currently cached value.
     * @return Object containing actual old value and success flag.
     * @throws NullPointerException if the key or value is {@code null}.
     * @throws IgniteCheckedException If remove failed.
     */
    public GridCacheReturn removex(K key, V val) throws IgniteCheckedException;

    /**
     * Asynchronously removes given key mapping from cache if one exists and value is equal to the passed in value.
     * <p>
     * This method will return {@code true} if remove did occur, which means that all optionally
     * provided filters have passed and there was something to remove, {@code false} otherwise.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via {@link CacheStore#delete(Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @param val Value to match against currently cached value.
     * @return Future for the remove operation. The future will return object containing actual old value and success
     *      flag.
     * @throws NullPointerException if the key or value is {@code null}.
     */
    public IgniteInternalFuture<GridCacheReturn> removexAsync(K key, V val);

    /**
     * @param key Key to retrieve the value for.
     * @param entry Cached entry when called from entry wrapper.
     * @param filter Filter to check prior to getting the value. Note that filter check
     *      together with getting the value is an atomic operation.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V get(K key, @Nullable GridCacheEntryEx entry, boolean deserializePortable,
        @Nullable CacheEntryPredicate... filter) throws IgniteCheckedException;

    /**
     * Gets value from cache. Will go to primary node even if this is a backup.
     *
     * @param key Key to get value for.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V getForcePrimary(K key) throws IgniteCheckedException;

    /**
     * Asynchronously gets value from cache. Will go to primary node even if this is a backup.
     *
     * @param key Key to get value for.
     * @return Future with result.
     */
    public IgniteInternalFuture<V> getForcePrimaryAsync(K key);

    /**
     * Gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys Keys to get values for.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public Map<K, V> getAllOutTx(List<K> keys) throws IgniteCheckedException;

    /**
     * Asynchronously gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys Keys to get values for.
     * @return Future with result.
     */
    public IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(List<K> keys);

    /**
     * Checks whether this cache is IGFS data cache.
     *
     * @return {@code True} in case this cache is IGFS data cache.
     */
    public boolean isIgfsDataCache();

    /**
     * Get current amount of used IGFS space in bytes.
     *
     * @return Amount of used IGFS space in bytes.
     */
    public long igfsDataSpaceUsed();

    /**
     * Get maximum space available for IGFS.
     *
     * @return Amount of space available for IGFS in bytes.
     */
    public long igfsDataSpaceMax();

    /**
     * Checks whether this cache is Mongo data cache.
     *
     * @return {@code True} if this cache is mongo data cache.
     */
    public boolean isMongoDataCache();

    /**
     * Checks whether this cache is Mongo meta cache.
     *
     * @return {@code True} if this cache is mongo meta cache.
     */
    public boolean isMongoMetaCache();

    /**
     * Gets entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Entry set.
     */
    public Set<Cache.Entry<K, V>> entrySetx(CacheEntryPredicate... filter);

    /**
     * Gets set of primary entries containing internal entries.
     *
     * @param filter Optional filter.
     * @return Primary entry set.
     */
    public Set<Cache.Entry<K, V>> primaryEntrySetx(CacheEntryPredicate... filter);

    /**
     * @return {@link ExpiryPolicy} associated with this projection.
     */
    public @Nullable ExpiryPolicy expiry();

    /**
     * @param plc {@link ExpiryPolicy} to associate with this projection.
     * @return New projection based on this one, but with the specified expiry policy.
     */
    public GridCacheProjectionEx<K, V> withExpiryPolicy(ExpiryPolicy plc);

    /**
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Invoke result.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> EntryProcessorResult<T> invoke(K key,
         EntryProcessor<K, V, T> entryProcessor,
         Object... args) throws IgniteCheckedException;

    /**
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Future.
     */
    public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args);

    /**
     * @param keys Keys.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Invoke results.
     * @throws IgniteCheckedException If failed.
     */
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException;

    /**
     * @param keys Keys.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Future.
     */
    public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args);

    /**
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Arguments.
     * @return Invoke results.
     * @throws IgniteCheckedException If failed.
     */
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException;

    /**
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Arguments.
     * @return Future.
     */
    public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args);

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context();

    /**
     * Delegates to {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure,Object...)} method
     * to load state from the underlying persistent storage. The loaded values
     * will then be given to the optionally passed in predicate, and, if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all loaded values will be stored in cache.
     * <p>
     * Note that this method does not receive keys as a parameter, so it is up to
     * {@link CacheStore} implementation to provide all the data to be loaded.
     * <p>
     * This method is not transactional and may end up loading a stale value into
     * cache if another thread has updated the value immediately after it has been
     * loaded. It is mostly useful when pre-loading the cache from underlying
     * data store before start, or for read-only caches.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure, Object...)} method.
     * @throws IgniteCheckedException If loading failed.
     */
    public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws IgniteCheckedException;

    /**
     * Asynchronously delegates to {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure, Object...)} method
     * to reload state from the underlying persistent storage. The reloaded values
     * will then be given to the optionally passed in predicate, and if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all reloaded values will be stored in cache.
     * <p>
     * Note that this method does not receive keys as a parameter, so it is up to
     * {@link CacheStore} implementation to provide all the data to be loaded.
     * <p>
     * This method is not transactional and may end up loading a stale value into
     * cache if another thread has updated the value immediately after it has been
     * loaded. It is mostly useful when pre-loading the cache from underlying
     * data store before start, or for read-only caches.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure,Object...)} method.
     * @return Future to be completed whenever loading completes.
     */
    public IgniteInternalFuture<?> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args);
}

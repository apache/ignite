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

package org.apache.ignite.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;

/**
 * This interface provides a rich API for working with individual cache entries. It
 * includes the following main functionality:
 * <ul>
 * <li>
 *  Various {@code 'get(..)'} methods to synchronously or asynchronously get values from cache.
 *  All {@code 'get(..)'} methods are transactional and will participate in an ongoing transaction
 *  if there is one.
 * </li>
 * <li>
 *  Various {@code 'set(..)'}, {@code 'setIfAbsent(..)'}, and {@code 'replace(..)'} methods to
 *  synchronously or asynchronously put single or multiple entries into cache.
 *  All these methods are transactional and will participate in an ongoing transaction
 *  if there is one.
 * </li>
 * <li>
 *  Various {@code 'remove(..)'} methods to synchronously or asynchronously remove single or multiple keys
 *  from cache. All {@code 'remove(..)'} methods are transactional and will participate in an ongoing transaction
 *  if there is one.
 * </li>
 * <li>
 *  Various {@code 'invalidate(..)'} methods to set cached values to {@code null}.
 * <li>
 * <li>
 *  Various {@code 'isLocked(..)'} methods to check on distributed locks on a single or multiple keys
 *  in cache. All locking methods are not transactional and will not enlist keys into ongoing transaction,
 *  if any.
 * </li>
 * <li>
 *  Various {@code 'peek(..)'} methods to peek at values in global or transactional memory, swap
 *  storage, or persistent storage.
 * </li>
 * <li>
 *  Various {@code 'reload(..)'} methods to reload latest values from persistent storage.
 * </li>
 * <li>
 *  Method {@link #evict()} to evict elements from cache, and optionally store
 *  them in underlying swap storage for later access. All {@code 'evict(..)'} methods are not
 *  transactional and will not enlist evicted keys into ongoing transaction, if any.
 * </li>
 * <li>
 *  Methods for {@link #timeToLive(long)} to change or lookup entry's time to live.
 * </ul>
 * <h1 class="header">Extended Put And Remove Methods</h1>
 * All methods that end with {@code 'x'} provide the same functionality as their sibling
 * methods that don't end with {@code 'x'}, however instead of returning a previous value they
 * return a {@code boolean} flag indicating whether operation succeeded or not. Returning
 * a previous value may involve a network trip or a persistent store lookup and should be
 * avoided whenever not needed.
 * <h1 class="header">Predicate Filters</h1>
 * All filters passed into methods on this API are checked <b>atomically</b>. In other words the value
 * of cache entry is guaranteed not to change throughout the cache operation.
 * <h1 class="header">Transactions</h1>
 * Cache API supports distributed transactions. All {@code 'get(..)'}, {@code 'put(..)'}, {@code 'replace(..)'},
 * and {@code 'remove(..)'} operations are transactional and will participate in an ongoing transaction.
 * Other methods like {@code 'peek(..)'} may be transaction-aware, i.e. check in-transaction entries first, but
 * will not affect the current state of transaction. See {@link IgniteTx} documentation for more information
 * about transactions.
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface CacheEntry<K, V> extends Map.Entry<K, V>, javax.cache.Cache.Entry<K, V> {
    /**
     * Cache projection to which this entry belongs. Note that entry and its
     * parent projections have same flags and filters.
     *
     * @return Cache projection for the cache to which this entry belongs.
     */
    public CacheProjection<K, V> projection();

    /**
     * This method has the same semantic as {@link CacheProjection#peek(Object)} method.
     *
     * @return See {@link CacheProjection#peek(Object)}.
     */
    @Nullable public V peek();

    /**
     * This method has the same semantic as
     * {@link CacheProjection#peek(Object, Collection)} method.
     *
     * @param modes See {@link CacheProjection#peek(Object, Collection)}.
     * @return See {@link CacheProjection#peek(Object, Collection)}.
     * @throws IgniteCheckedException See {@link CacheProjection#peek(Object, Collection)}.
     */
    @Nullable public V peek(@Nullable Collection<GridCachePeekMode> modes) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#reload(Object)} method.
     *
     * @return See {@link CacheProjection#reload(Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#reload(Object)}.
     */
    @Nullable public V reload() throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#reloadAsync(Object)} method.
     *
     * @return See {@link CacheProjection#reloadAsync(Object)}.
     */
    public IgniteInternalFuture<V> reloadAsync();

    /**
     * This method has the same semantic as
     * {@link CacheProjection#isLocked(Object)} method.
     *
     * @return See {@link CacheProjection#isLocked(Object)}.
     */
    public boolean isLocked();

    /**
     * This method has the same semantic as
     * {@link CacheProjection#isLockedByThread(Object)} method.
     *
     * @return See {@link CacheProjection#isLockedByThread(Object)}.
     */
    public boolean isLockedByThread();

    /**
     * Gets current version of this cache entry.
     *
     * @return Version of this cache entry.
     */
    public Object version();

    /**
     * Gets expiration time for this entry.
     *
     * @return Absolute time when this value expires.
     */
    public long expirationTime();

    /**
     * Gets time to live, i.e. maximum life time, of this entry in milliseconds.
     *
     * @return Time to live value for this entry.
     */
    public long timeToLive();

    /**
     * Sets time to live, i.e. maximum life time, of this entry in milliseconds.
     * Note that this method is transactional - if entry is enlisted into a transaction,
     * then time-to-live will not be set until transaction commit.
     * <p>
     * When called outside the transaction, this method will have no effect until the
     * next update operation.
     *
     * @param ttl Time to live value for this entry.
     */
    public void timeToLive(long ttl);

    /**
     * Gets the flag indicating current node's primary ownership for this entry.
     * <p>
     * Note, that this value is dynamic and may change with grid topology changes.
     *
     * @return {@code True} if current grid node is the primary owner for this entry.
     */
    public boolean primary();

    /**
     * Gets the flag indicating if current node is backup for this entry.
     * <p>
     * Note, that this value is dynamic and may change with grid topology changes.
     *
     * @return {@code True} if current grid node is the backup for this entry.
     */
    public boolean backup();

    /**
     * Gets affinity partition id for this entry.
     *
     * @return Partition id.
     */
    public int partition();

    /**
     * This method has the same semantic as {@link #get()} method, however it
     * wraps {@link IgniteCheckedException} into {@link IgniteException} if failed in order to
     * comply with {@link Entry} interface.
     *
     * @return See {@link #get()}
     */
    @Nullable @Override public V getValue();

    /**
     * This method has the same semantic as
     * {@link CacheProjection#get(Object)} method.
     *
     * @return See {@link CacheProjection#get(Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#get(Object)}.
     */
    @Nullable public V get() throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#getAsync(Object)} method.
     *
     * @return See {@link CacheProjection#getAsync(Object)}.
     */
    public IgniteInternalFuture<V> getAsync();

    /**
     * This method has the same semantic as {@link #set(Object, org.apache.ignite.lang.IgnitePredicate[])} method, however it
     * wraps {@link IgniteCheckedException} into {@link IgniteException} if failed in order to
     * comply with {@link Entry} interface.
     *
     * @return See {@link #set(Object, org.apache.ignite.lang.IgnitePredicate[])}
     */
    @Nullable @Override public V setValue(V val);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param val See {@link CacheProjection#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}
     * @param filter See {@link CacheProjection#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @throws IgniteCheckedException See {@link CacheProjection#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    @Nullable public V set(V val, @Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param val See {@link CacheProjection#putAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}
     * @param filter See {@link CacheProjection#putAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#putAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    public IgniteInternalFuture<V> setAsync(V val, @Nullable IgnitePredicate<CacheEntry<K, V>>... filter);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putIfAbsent(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#putIfAbsent(Object, Object)}
     * @return See {@link CacheProjection#putIfAbsent(Object, Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#putIfAbsent(Object, Object)}.
     */
    @Nullable public V setIfAbsent(V val) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putIfAbsentAsync(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#putIfAbsentAsync(Object, Object)}
     * @return See {@link CacheProjection#putIfAbsentAsync(Object, Object)}.
     */
    public IgniteInternalFuture<V> setIfAbsentAsync(V val);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param val See {@link CacheProjection#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}
     * @param filter See {@link CacheProjection#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @throws IgniteCheckedException See {@link CacheProjection#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    public boolean setx(V val, @Nullable IgnitePredicate<CacheEntry<K, V>>... filter)
        throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putxAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param val See {@link CacheProjection#putxAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}
     * @param filter See {@link CacheProjection#putxAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#putxAsync(Object, Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    public IgniteInternalFuture<Boolean> setxAsync(V val,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putxIfAbsent(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#putxIfAbsent(Object, Object)}
     * @return See {@link CacheProjection#putxIfAbsent(Object, Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#putxIfAbsent(Object, Object)}.
     */
    public boolean setxIfAbsent(@Nullable V val) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#putxIfAbsentAsync(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#putxIfAbsentAsync(Object, Object)}
     * @return See {@link CacheProjection#putxIfAbsentAsync(Object, Object)}.
     */
    public IgniteInternalFuture<Boolean> setxIfAbsentAsync(V val);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#replace(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#replace(Object, Object)}
     * @return See {@link CacheProjection#replace(Object, Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#replace(Object, Object)}.
     */
    @Nullable public V replace(V val) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#replaceAsync(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#replaceAsync(Object, Object)}
     * @return See {@link CacheProjection#replaceAsync(Object, Object)}.
     */
    public IgniteInternalFuture<V> replaceAsync(V val);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#replacex(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#replacex(Object, Object)}
     * @return See {@link CacheProjection#replacex(Object, Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#replacex(Object, Object)}.
     */
    public boolean replacex(V val) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#replacexAsync(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#replacexAsync(Object, Object)}
     * @return See {@link CacheProjection#replacexAsync(Object, Object)}.
     */
    public IgniteInternalFuture<Boolean> replacexAsync(V val);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#replace(Object, Object, Object)} method.
     *
     * @param oldVal See {@link CacheProjection#replace(Object, Object, Object)}
     * @param newVal See {@link CacheProjection#replace(Object, Object, Object)}
     * @return See {@link CacheProjection#replace(Object, Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#replace(Object, Object)}.
     */
    public boolean replace(V oldVal, V newVal) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#replaceAsync(Object, Object, Object)} method.
     *
     * @param oldVal See {@link CacheProjection#replaceAsync(Object, Object, Object)}
     * @param newVal See {@link CacheProjection#replaceAsync(Object, Object, Object)}
     * @return See {@link CacheProjection#replaceAsync(Object, Object)}.
     */
    public IgniteInternalFuture<Boolean> replaceAsync(V oldVal, V newVal);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#remove(Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param filter See {@link CacheProjection#remove(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#remove(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @throws IgniteCheckedException See {@link CacheProjection#remove(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    @Nullable public V remove(@Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#removeAsync(Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param filter See {@link CacheProjection#removeAsync(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#removeAsync(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    public IgniteInternalFuture<V> removeAsync(@Nullable IgnitePredicate<CacheEntry<K, V>>... filter);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#removex(Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param filter See {@link CacheProjection#removex(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#removex(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @throws IgniteCheckedException See {@link CacheProjection#removex(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    public boolean removex(@Nullable IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#removexAsync(Object, org.apache.ignite.lang.IgnitePredicate[])} method.
     *
     * @param filter See {@link CacheProjection#removexAsync(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     * @return See {@link CacheProjection#removexAsync(Object, org.apache.ignite.lang.IgnitePredicate[])}.
     */
    public IgniteInternalFuture<Boolean> removexAsync(@Nullable IgnitePredicate<CacheEntry<K, V>>... filter);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#remove(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#remove(Object, Object)}.
     * @return See {@link CacheProjection#remove(Object, Object)}.
     * @throws IgniteCheckedException See {@link CacheProjection#remove(Object, Object)}.
     */
    public boolean remove(V val) throws IgniteCheckedException;

    /**
     * This method has the same semantic as
     * {@link CacheProjection#removeAsync(Object, Object)} method.
     *
     * @param val See {@link CacheProjection#removeAsync(Object, Object)}.
     * @return See {@link CacheProjection#removeAsync(Object, Object)}.
     */
    public IgniteInternalFuture<Boolean> removeAsync(V val);

    /**
     * This method has the same semantic as
     * {@link CacheProjection#evict(Object)} method.
     *
     * @return See {@link CacheProjection#evict(Object)}.
     */
    public boolean evict();

    /**
     * This method has the same semantic as
     * {@link CacheProjection#clear(Object)} method.
     *
     * @return See {@link CacheProjection#clear(Object)}.
     */
    public boolean clear();

    /**
     * Optimizes the size of this entry. If entry is expired at the time
     * of the call then entry is removed locally.
     *
     * @throws IgniteCheckedException If failed to compact.
     * @return {@code true} if entry was cleared from cache (if value was {@code null}).
     */
    public boolean compact() throws IgniteCheckedException;

    /**
     * Synchronously acquires lock on a cached object associated with this entry
     * only if the passed in filter (if any) passes. This method together with
     * filter check will be executed as one atomic operation.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions.
     * If you do need explicit locking within transaction, then you should use
     * {@link IgniteTxConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.apache.ignite.internal.processors.cache.CacheFlag#LOCAL}, {@link org.apache.ignite.internal.processors.cache.CacheFlag#READ}.
     *
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration).
     * @param filter Optional filter to validate prior to acquiring the lock.
     * @return {@code True} if all filters passed and lock was acquired,
     *      {@code false} otherwise.
     * @throws IgniteCheckedException If lock acquisition resulted in error.
     * @throws org.apache.ignite.internal.processors.cache.CacheFlagException If flags validation failed.
     */
    public boolean lock(long timeout, @Nullable IgnitePredicate<CacheEntry<K, V>>... filter)
        throws IgniteCheckedException;

    /**
     * Asynchronously acquires lock on a cached object associated with this entry
     * only if the passed in filter (if any) passes. This method together with
     * filter check will be executed as one atomic operation.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link IgniteTxConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.apache.ignite.internal.processors.cache.CacheFlag#LOCAL}, {@link org.apache.ignite.internal.processors.cache.CacheFlag#READ}.
     *
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration).
     * @param filter Optional filter to validate prior to acquiring the lock.
     * @return Future for the lock operation. The future will return {@code true}
     *      whenever all filters pass and locks are acquired before timeout is expired,
     *      {@code false} otherwise.
     * @throws org.apache.ignite.internal.processors.cache.CacheFlagException If flags validation failed.
     */
    public IgniteInternalFuture<Boolean> lockAsync(long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>... filter);

    /**
     * Unlocks this entry only if current thread owns the lock. If optional filter
     * will not pass, then unlock will not happen. If this entry was never locked by
     * current thread, then this method will do nothing.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link IgniteTxConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.apache.ignite.internal.processors.cache.CacheFlag#LOCAL}, {@link org.apache.ignite.internal.processors.cache.CacheFlag#READ}.
     *
     * @param filter Optional filter that needs to pass prior to unlock taking effect.
     * @throws IgniteCheckedException If unlock execution resulted in error.
     * @throws org.apache.ignite.internal.processors.cache.CacheFlagException If flags validation failed.
     */
    public void unlock(IgnitePredicate<CacheEntry<K, V>>... filter) throws IgniteCheckedException;

    /**
     * Checks whether entry is currently present in cache or not. If entry is not in
     * cache (e.g. has been removed) {@code false} is returned. In this case all
     * operations on this entry will cause creation of a new entry in cache.
     *
     * @return {@code True} if entry is in cache, {@code false} otherwise.
     */
    public boolean isCached();

    /**
     * Gets size of serialized key and value in addition to any overhead added by {@code Ignite} itself.
     *
     * @return size in bytes.
     * @throws IgniteCheckedException If failed to evaluate entry size.
     */
    public int memorySize() throws IgniteCheckedException;

    /**
     * Removes metadata by name.
     *
     * @param name Name of the metadata to remove.
     * @param <V> Type of the value.
     * @return Value of removed metadata or {@code null}.
     */
    public <V> V removeMeta(String name);

    /**
     * Removes metadata only if its current value is equal to {@code val} passed in.
     *
     * @param name Name of metadata attribute.
     * @param val Value to compare.
     * @param <V> Value type.
     * @return {@code True} if value was removed, {@code false} otherwise.
     */
    public <V> boolean removeMeta(String name, V val);

    /**
     * Gets metadata by name.
     *
     * @param name Metadata name.
     * @param <V> Type of the value.
     * @return Metadata value or {@code null}.
     */
    public <V> V meta(String name);

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param c Factory closure to produce value to add if it's not attached already.
     *      Not that unlike {@link #addMeta(String, Object)} method the factory closure will
     *      not be called unless the value is required and therefore value will only be created
     *      when it is actually needed.
     * @param <V> Type of the value.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @Nullable public <V> V putMetaIfAbsent(String name, Callable<V> c);

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @Nullable public <V> V putMetaIfAbsent(String name, V val);

    /**
     * Adds a new metadata.
     *
     * @param name Metadata name.
     * @param val Metadata value.
     * @param <V> Type of the value.
     * @return Metadata previously associated with given name, or
     *      {@code null} if there was none.
     */
    @Nullable public <V> V addMeta(String name, V val);

    /**
     * Replaces given metadata with new {@code newVal} value only if its current value
     * is equal to {@code curVal}. Otherwise, it is no-op.
     *
     * @param name Name of the metadata.
     * @param curVal Current value to check.
     * @param newVal New value.
     * @return {@code true} if replacement occurred, {@code false} otherwise.
     */
    public <V> boolean replaceMeta(String name, V curVal, V newVal);
}

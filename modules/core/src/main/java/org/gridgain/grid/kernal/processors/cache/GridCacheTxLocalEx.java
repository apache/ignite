/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Local transaction API.
 */
public interface GridCacheTxLocalEx<K, V> extends GridCacheTxEx<K, V> {
    /**
     * @return Minimum version involved in transaction.
     */
    public GridCacheVersion minVersion();

    /**
     * @return Future for this transaction.
     */
    public GridFuture<GridCacheTxEx<K, V>> future();

    /**
     * @return Commit error.
     */
    @Nullable public Throwable commitError();

    /**
     * @param e Commit error.
     */
    public void commitError(Throwable e);

    /**
     * @throws GridException If commit failed.
     */
    public void userCommit() throws GridException;

    /**
     * @throws GridException If rollback failed.
     */
    public void userRollback() throws GridException;

    /**
     * @return Group lock entry if this is a group-lock transaction.
     */
    @Nullable public GridCacheTxEntry<K, V> groupLockEntry();

    /**
     * @param keys Keys to get.
     * @param cached Cached entry if this method is called from entry wrapper.
     *      Cached entry is passed if and only if there is only one key in collection of keys.
     * @param deserializePortable Deserialize portable flag.
     * @param filter Entry filter.
     * @return Future for this get.
     */
    public GridFuture<Map<K, V>> getAllAsync(Collection<? extends K> keys, @Nullable GridCacheEntryEx<K, V> cached,
        boolean deserializePortable, GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param key Key.
     * @param val Value.
     * @param cached Cached cache entry, if called from entry wrapper.
     * @param ttl Entry time-to-live value. If negative, leave unchanged.
     * @param filter Filter.
     * @return Old value.
     * @throws GridException If put failed.
     */
    public V put(K key, V val, @Nullable GridCacheEntryEx<K, V> cached, long ttl,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return {@code True} if put succeeded.
     * @throws GridException If put failed.
     */
    public boolean putx(K key, V val, GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * @param key Key.
     * @param valTransform Value transform closure.
     * @return {@code True} if put succeeded.
     * @throws GridException If put failed.
     */
    public boolean transform(K key, GridClosure<V, V> valTransform) throws GridException;

    /**
     * @param key Key.
     * @param transformer Closure.
     * @return Value computed by the given closure.
     * @throws GridException If failed.
     */
    public <R> R transformCompute(K key, GridClosure<V, GridBiTuple<V, R>> transformer) throws GridException;

    /**
     * @param key Key.
     * @param val Value.
     * @param cached Cached cache entry, if called from entry wrapper.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @param filter Filter.
     * @return {@code True} if put succeeded.
     * @throws GridException If put failed.
     */
    public boolean putx(K key, V val, @Nullable GridCacheEntryEx<K, V> cached, long ttl,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * @param map Map to put.
     * @param filter Filter.
     * @return Old value for the first key in the map.
     * @throws GridException If put failed.
     */
    public GridCacheReturn<V> putAll(Map<? extends K, ? extends V> map,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * @param drMap DR map to put.
     * @throws GridException If put failed.
     */
    public void putAllDr(Map<? extends K, GridCacheDrInfo<V>> drMap)
        throws GridException;

    /**
     * @param drMap DR map to put.
     * @return Future all the put operation which will return value as if
     *      {@link #put(Object, Object, GridCacheEntryEx, long, GridPredicate[])} was called.
     */
    public GridFuture<?> putAllDrAsync(Map<? extends K, GridCacheDrInfo<V>> drMap);

    /**
     * @param map Map to put.
     * @throws GridException If put failed.
     */
    public void transformAll(@Nullable Map<? extends K, ? extends GridClosure<V, V>> map) throws GridException;

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return Value future.
     */
    public GridFuture<V> putAsync(K key, V val, GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param key Key.
     * @param val Value.
     * @param cached Optional cached cache entry.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @param filter Filter.
     * @return Value future.
     */
    public GridFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> cached, long ttl,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return Value future.
     */
    public GridFuture<Boolean> putxAsync(K key, V val, GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param key Key.
     * @param val Value.
     * @param cached Optional cached cache entry.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @param filter Filter.
     * @return Value future.
     */
    public GridFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> cached, long ttl,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param key Key.
     * @param val Value.
     * @param cached Optional cached cache entry.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @param filter Filter.
     * @return Value future.
     */
    public GridFuture<GridCacheReturn<V>> putxAsync0(K key, V val, @Nullable GridCacheEntryEx<K, V> cached,
        long ttl, GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param key Key.
     * @param transformer Value transform closure.
     * @param cached Cached entry, if called from entry wrapper.
     * @param ttl Entry time-to-live value. If negative, leave unchanged.
     * @return Transform operation future.
     */
    public GridFuture<Boolean> transformAsync(K key, GridClosure<V, V> transformer,
        @Nullable GridCacheEntryEx<K, V> cached, long ttl);

    /**
     * @param key Key.
     * @param cached Cached entry, if any.
     * @param filter Filter.
     * @return Value.
     * @throws GridException If remove failed.
     */
    public V remove(K key, @Nullable GridCacheEntryEx<K, V> cached,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * @param key Key.
     * @param cached Cached entry, if any.
     * @param filter Filter.
     * @return Value.
     * @throws GridException If remove failed.
     */
    public boolean removex(K key, @Nullable GridCacheEntryEx<K, V> cached,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * @param keys Keys to remove.
     * @param filter Filter.
     * @return Value of first removed key.
     * @throws GridException If remove failed.
     */
    public GridCacheReturn<V> removeAll(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException;

    /**
     * Asynchronous remove.
     *
     * @param key Key to remove.
     * @param cached Cached entry, if any.
     * @param filter Filter.
     * @return Removed value.
     */
    public GridFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx<K, V> cached,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * Asynchronous remove.
     *
     * @param key Key to remove.
     * @param cached Cached entry, if any.
     * @param filter Filter.
     * @return Removed value.
     */
    public GridFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx<K, V> cached,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * Asynchronous remove.
     *
     * @param key Key to remove.
     * @param cached Optional cached entry.
     * @param filter Filter.
     * @return Removed value.
     */
    public GridFuture<GridCacheReturn<V>> removexAsync0(K key, @Nullable GridCacheEntryEx<K, V> cached,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param drMap DR map.
     * @throws GridException If remove failed.
     */
    public void removeAllDr(Map<? extends K, GridCacheVersion> drMap)
        throws GridException;

    /**
     * @param drMap DR map.
     * @return Future for asynchronous remove.
     */
    public GridFuture<?> removeAllDrAsync(Map<? extends K, GridCacheVersion> drMap);

    /**
     * @param map Map to put.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if map has size 1.
     * @param filter Filter.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @return Future all the put operation which will return value as if
     *      {@link #put(Object, Object, GridCacheEntryEx, long, GridPredicate[])} was called.
     */
    public GridFuture<GridCacheReturn<V>> putAllAsync(Map<? extends K, ? extends V> map,
        boolean retval, @Nullable GridCacheEntryEx<K, V> cached, long ttl,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param map Map to put.
     * @return Transform operation future.
     */
    public GridFuture<?> transformAllAsync(@Nullable Map<? extends K, ? extends GridClosure<V, V>> map);

    /**
     * @param keys Keys to remove.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if size of keys collection is 1.
     * @param implicit Allows to externally control how transaction handles implicit flags.
     * @param filter Filter.
     * @return Future for asynchronous remove.
     */
    public GridFuture<GridCacheReturn<V>> removeAllAsync(Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached, boolean implicit, boolean retval,
        GridPredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * Performs keys locking for affinity-based group lock transactions.
     *
     * @param keys Keys to lock.
     * @return Lock future.
     */
    public GridFuture<?> groupLockAsync(Collection<K> keys);

    /**
     * @return {@code True} if keys from the same partition are allowed to be enlisted in group-lock transaction.
     */
    public boolean partitionLock();

    /**
     * Finishes transaction (either commit or rollback).
     *
     * @param commit {@code True} if commit, {@code false} if rollback.
     * @return {@code True} if state has been changed.
     * @throws GridException If finish failed.
     */
    public boolean finish(boolean commit) throws GridException;

    /**
     * @param async if {@code True}, then loading will happen in a separate thread.
     * @param keys Keys.
     * @param c Closure.
     * @param deserializePortable Deserialize portable flag.
     * @return Future with {@code True} value if loading took place.
     */
    public GridFuture<Boolean> loadMissing(boolean async, Collection<? extends K> keys, boolean deserializePortable,
        GridBiInClosure<K, V> c);
}

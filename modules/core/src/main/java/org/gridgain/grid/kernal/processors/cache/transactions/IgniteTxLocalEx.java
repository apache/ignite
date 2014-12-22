/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.transactions;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Local transaction API.
 */
public interface IgniteTxLocalEx<K, V> extends IgniteTxEx<K, V> {
    /**
     * @return Minimum version involved in transaction.
     */
    public GridCacheVersion minVersion();

    /**
     * @return Commit error.
     */
    @Nullable public Throwable commitError();

    /**
     * @param e Commit error.
     */
    public void commitError(Throwable e);

    /**
     * @throws IgniteCheckedException If commit failed.
     */
    public void userCommit() throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If rollback failed.
     */
    public void userRollback() throws IgniteCheckedException;

    /**
     * @return Group lock entry if this is a group-lock transaction.
     */
    @Nullable public IgniteTxEntry<K, V> groupLockEntry();

    /**
     * @param keys Keys to get.
     * @param cached Cached entry if this method is called from entry wrapper.
     *      Cached entry is passed if and only if there is only one key in collection of keys.
     * @param deserializePortable Deserialize portable flag.
     * @param filter Entry filter.
     * @return Future for this get.
     */
    public IgniteFuture<Map<K, V>> getAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        boolean deserializePortable,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param map Map to put.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if map has size 1.
     * @param filter Filter.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @return Future for put operation.
     */
    public IgniteFuture<GridCacheReturn<V>> putAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, ? extends V> map,
        boolean retval,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param map Map to put.
     * @return Transform operation future.
     */
    public IgniteFuture<GridCacheReturn<V>> transformAllAsync(
        GridCacheContext<K, V> cacheCtx,
        @Nullable Map<? extends K, ? extends IgniteClosure<V, V>> map,
        boolean retval,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl);

    /**
     * @param keys Keys to remove.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if size of keys collection is 1.
     * @param filter Filter.
     * @return Future for asynchronous remove.
     */
    public IgniteFuture<GridCacheReturn<V>> removeAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        boolean retval,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter);

    /**
     * @param drMap DR map to put.
     * @return Future for DR put operation.
     */
    public IgniteFuture<?> putAllDrAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, GridCacheDrInfo<V>> drMap);

    /**
     * @param drMap DR map.
     * @return Future for asynchronous remove.
     */
    public IgniteFuture<?> removeAllDrAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, GridCacheVersion> drMap);

    /**
     * Performs keys locking for affinity-based group lock transactions.
     *
     * @param keys Keys to lock.
     * @return Lock future.
     */
    public IgniteFuture<?> groupLockAsync(GridCacheContext<K, V> cacheCtx, Collection<K> keys);

    /**
     * @return {@code True} if keys from the same partition are allowed to be enlisted in group-lock transaction.
     */
    public boolean partitionLock();

    /**
     * Finishes transaction (either commit or rollback).
     *
     * @param commit {@code True} if commit, {@code false} if rollback.
     * @return {@code True} if state has been changed.
     * @throws IgniteCheckedException If finish failed.
     */
    public boolean finish(boolean commit) throws IgniteCheckedException;

    /**
     * @param async if {@code True}, then loading will happen in a separate thread.
     * @param keys Keys.
     * @param c Closure.
     * @param deserializePortable Deserialize portable flag.
     * @return Future with {@code True} value if loading took place.
     */
    public IgniteFuture<Boolean> loadMissing(
        GridCacheContext<K, V> cacheCtx,
        boolean async,
        Collection<? extends K> keys,
        boolean deserializePortable,
        IgniteBiInClosure<K, V> c);
}

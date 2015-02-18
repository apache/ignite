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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.processor.*;
import java.util.*;

/**
 * Local transaction API.
 */
public interface IgniteTxLocalEx<K, V> extends IgniteInternalTx<K, V> {
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
     * @param cacheCtx Cache context.
     * @param keys Keys to get.
     * @param cached Cached entry if this method is called from entry wrapper.
     *      Cached entry is passed if and only if there is only one key in collection of keys.
     * @param deserializePortable Deserialize portable flag.
     * @return Future for this get.
     */
    public IgniteInternalFuture<Map<K, V>> getAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        boolean deserializePortable,
        boolean skipVals);

    /**
     * @param cacheCtx Cache context.
     * @param map Map to put.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if map has size 1.
     * @param filter Filter.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @return Future for put operation.
     */
    public IgniteInternalFuture<GridCacheReturn<V>> putAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, ? extends V> map,
        boolean retval,
        @Nullable GridCacheEntryEx<K, V> cached,
        long ttl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter);

    /**
     * @param cacheCtx Cache context.
     * @param map Entry processors map.
     * @param invokeArgs Optional arguments for entry processor.
     * @return Transform operation future.
     */
    public <T> IgniteInternalFuture<GridCacheReturn<Map<K, EntryProcessorResult<T>>>> invokeAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, ? extends EntryProcessor<K, V, Object>> map,
        Object... invokeArgs);

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if size of keys collection is 1.
     * @param filter Filter.
     * @return Future for asynchronous remove.
     */
    public IgniteInternalFuture<GridCacheReturn<V>> removeAllAsync(
        GridCacheContext<K, V> cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached,
        boolean retval,
        IgnitePredicate<Cache.Entry<K, V>>[] filter);

    /**
     * @param cacheCtx Cache context.
     * @param drMap DR map to put.
     * @return Future for DR put operation.
     */
    public IgniteInternalFuture<?> putAllDrAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, GridCacheDrInfo<V>> drMap);

    /**
     * @param cacheCtx Cache context.
     * @param drMap DR map.
     * @return Future for asynchronous remove.
     */
    public IgniteInternalFuture<?> removeAllDrAsync(
        GridCacheContext<K, V> cacheCtx,
        Map<? extends K, GridCacheVersion> drMap);

    /**
     * Performs keys locking for affinity-based group lock transactions.
     *
     * @param cacheCtx Cache context.
     * @param keys Keys to lock.
     * @return Lock future.
     */
    public IgniteInternalFuture<?> groupLockAsync(GridCacheContext<K, V> cacheCtx, Collection<K> keys);

    /**
     * @return {@code True} if keys from the same partition are allowed to be enlisted in group-lock transaction.
     */
    public boolean partitionLock();

    /**
     * @return Return value for
     */
    public GridCacheReturn<V> implicitSingleResult();

    /**
     * Finishes transaction (either commit or rollback).
     *
     * @param commit {@code True} if commit, {@code false} if rollback.
     * @return {@code True} if state has been changed.
     * @throws IgniteCheckedException If finish failed.
     */
    public boolean finish(boolean commit) throws IgniteCheckedException;

    /**
     * @param cacheCtx  Cache context.
     * @param readThrough Read through flag.
     * @param async if {@code True}, then loading will happen in a separate thread.
     * @param keys Keys.
     * @param c Closure.
     * @param deserializePortable Deserialize portable flag.
     * @param skipVals Skip values flag.
     * @return Future with {@code True} value if loading took place.
     */
    public IgniteInternalFuture<Boolean> loadMissing(
        GridCacheContext<K, V> cacheCtx,
        boolean readThrough,
        boolean async,
        Collection<? extends K> keys,
        boolean deserializePortable,
        boolean skipVals,
        IgniteBiInClosure<K, V> c);
}

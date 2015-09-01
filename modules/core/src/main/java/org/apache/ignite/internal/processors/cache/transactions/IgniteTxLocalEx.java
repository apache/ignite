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

import java.util.Collection;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Local transaction API.
 */
public interface IgniteTxLocalEx extends IgniteInternalTx {
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
     * @param cacheCtx Cache context.
     * @param keys Keys to get.
     * @param cached Cached entry if this method is called from entry wrapper
     *      Cached entry is passed if and only if there is only one key in collection of keys.
     * @param deserializePortable Deserialize portable flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects
     * @param skipStore Skip store flag.
     * @return Future for this get.
     */
    public <K, V> IgniteInternalFuture<Map<K, V>> getAllAsync(
        GridCacheContext cacheCtx,
        Collection<KeyCacheObject> keys,
        @Nullable GridCacheEntryEx cached,
        boolean deserializePortable,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean skipStore);

    /**
     * @param cacheCtx Cache context.
     * @param map Map to put.
     * @param retval Flag indicating whether a value should be returned.
     * @param cached Cached entry, if any. Will be provided only if map has size 1.
     * @param filter Filter.
     * @param ttl Time to live for entry. If negative, leave unchanged.
     * @return Future for put operation.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> putAllAsync(
        GridCacheContext cacheCtx,
        Map<? extends K, ? extends V> map,
        boolean retval,
        @Nullable GridCacheEntryEx cached,
        long ttl,
        CacheEntryPredicate[] filter);

    /**
     * @param cacheCtx Cache context.
     * @param map Entry processors map.
     * @param invokeArgs Optional arguments for entry processor.
     * @return Transform operation future.
     */
    public <K, V, T> IgniteInternalFuture<GridCacheReturn> invokeAsync(
        GridCacheContext cacheCtx,
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
    public <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync(
        GridCacheContext cacheCtx,
        Collection<? extends K> keys,
        @Nullable GridCacheEntryEx cached,
        boolean retval,
        CacheEntryPredicate[] filter);

    /**
     * @param cacheCtx Cache context.
     * @param drMap DR map to put.
     * @return Future for DR put operation.
     */
    public IgniteInternalFuture<?> putAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheDrInfo> drMap);

    /**
     * @param cacheCtx Cache context.
     * @param drMap DR map.
     * @return Future for asynchronous remove.
     */
    public IgniteInternalFuture<?> removeAllDrAsync(
        GridCacheContext cacheCtx,
        Map<KeyCacheObject, GridCacheVersion> drMap);

    /**
     * @return Return value for
     */
    public GridCacheReturn implicitSingleResult();

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
        GridCacheContext cacheCtx,
        boolean readThrough,
        boolean async,
        Collection<KeyCacheObject> keys,
        boolean deserializePortable,
        boolean skipVals,
        IgniteBiInClosure<KeyCacheObject, Object> c);
}
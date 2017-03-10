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
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridInClosure3;
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
     * @param deserializeBinary Deserialize binary flag.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects
     * @param skipStore Skip store flag.
     * @return Future for this get.
     */
    public <K, V> IgniteInternalFuture<Map<K, V>> getAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<KeyCacheObject> keys,
        boolean deserializeBinary,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean skipStore,
        boolean needVer);

    /**
     * @param cacheCtx Cache context.
     * @param map Map to put.
     * @param retval Flag indicating whether a value should be returned.
     * @return Future for put operation.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> putAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Map<? extends K, ? extends V> map,
        boolean retval);

    /**
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param val Value.
     * @param retval Return value flag.
     * @param filter Filter.
     * @return Future for put operation.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> putAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        V val,
        boolean retval,
        CacheEntryPredicate filter);

    /**
     * @param cacheCtx Cache context.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for entry processor.
     * @return Operation future.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> invokeAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        K key,
        EntryProcessor<K, V, Object> entryProcessor,
        Object... invokeArgs);

    /**
     * @param cacheCtx Cache context.
     * @param map Entry processors map.
     * @param invokeArgs Optional arguments for entry processor.
     * @return Operation future.
     */
    public <K, V, T> IgniteInternalFuture<GridCacheReturn> invokeAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Map<? extends K, ? extends EntryProcessor<K, V, Object>> map,
        Object... invokeArgs);

    /**
     * @param cacheCtx Cache context.
     * @param keys Keys to remove.
     * @param retval Flag indicating whether a value should be returned.
     * @param filter Filter.
     * @param singleRmv {@code True} for single key remove operation ({@link Cache#remove(Object)}.
     * @return Future for asynchronous remove.
     */
    public <K, V> IgniteInternalFuture<GridCacheReturn> removeAllAsync(
        GridCacheContext cacheCtx,
        @Nullable AffinityTopologyVersion entryTopVer,
        Collection<? extends K> keys,
        boolean retval,
        CacheEntryPredicate filter,
        boolean singleRmv);

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
     * @param skipVals Skip values flag.
     * @param needVer If {@code true} version is required for loaded values.
     * @param c Closure to be applied for loaded values.
     * @param expiryPlc Expiry policy.
     * @return Future with {@code True} value if loading took place.
     */
    public IgniteInternalFuture<Void> loadMissing(
        GridCacheContext cacheCtx,
        AffinityTopologyVersion topVer,
        boolean readThrough,
        boolean async,
        Collection<KeyCacheObject> keys,
        boolean skipVals,
        boolean needVer,
        boolean keepBinary,
        final ExpiryPolicy expiryPlc,
        GridInClosure3<KeyCacheObject, Object, GridCacheVersion> c);
}

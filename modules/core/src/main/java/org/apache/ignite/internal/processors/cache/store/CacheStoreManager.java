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

package org.apache.ignite.internal.processors.cache.store;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.processors.cache.GridCacheManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Cache store manager interface.
 */
public interface CacheStoreManager<K, V> extends GridCacheManager<K, V> {
    /**
     * Initialize store manager.
     *
     * @param cfgStore   Actual store.
     * @param sesHolders Session holders.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    public void initialize(@Nullable CacheStore<?, ?> cfgStore, Map<CacheStore, ThreadLocal> sesHolders)
        throws IgniteCheckedException;

    /**
     * @return {@code true} If store configured.
     */
    public boolean configured();

    /**
     * @return Wrapped store.
     */
    public CacheStore<Object, Object> store();

    /**
     * @return Unwrapped store provided in configuration.
     */
    public CacheStore<?, ?> configuredStore();

    /**
     * @return {@code true} If local store is configured.
     */
    public boolean isLocal();

    /**
     * @return {@code True} is write-through is enabled.
     */
    public boolean isWriteThrough();

    /**
     * @return {@code True} is write-behind is enabled.
     */
    public boolean isWriteBehind();

    /**
     * @return Whether DHT transaction can write to store from DHT.
     */
    public boolean isWriteToStoreFromDht();

    /**
     * Loads data from persistent store.
     *
     * @param tx Cache transaction.
     * @param key Cache key.
     * @return Loaded value, possibly <tt>null</tt>.
     * @throws IgniteCheckedException If data loading failed.
     */
    @Nullable public Object load(@Nullable IgniteInternalTx tx, KeyCacheObject key) throws IgniteCheckedException;

    /**
     * Loads data from persistent store.
     *
     * @param tx Cache transaction.
     * @param keys Cache keys.
     * @param vis Closure.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If data loading failed.
     */
    public boolean loadAll(@Nullable IgniteInternalTx tx, Collection<? extends KeyCacheObject> keys,
        IgniteBiInClosure<KeyCacheObject, Object> vis) throws IgniteCheckedException;

    /**
     * @param tx Cache transaction.
     * @param keys Cache keys.
     * @param vis Closure to apply for loaded elements.
     * @throws IgniteCheckedException If data loading failed.
     */
    public void localStoreLoadAll(@Nullable IgniteInternalTx tx, Collection<? extends KeyCacheObject> keys,
        final GridInClosure3<KeyCacheObject, Object, GridCacheVersion> vis) throws IgniteCheckedException;

    /**
     * Loads data from persistent store.
     *
     * @param vis Closer to cache loaded elements.
     * @param args User arguments.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If data loading failed.
     */
    public boolean loadCache(final GridInClosure3<KeyCacheObject, Object, GridCacheVersion> vis, Object[] args)
        throws IgniteCheckedException;

    /**
     * Puts key-value pair into storage.
     *
     * @param tx Cache transaction.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @return {@code true} If there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean put(@Nullable IgniteInternalTx tx, Object key, Object val, GridCacheVersion ver)
        throws IgniteCheckedException;

    /**
     * Puts key-value pair into storage.
     *
     * @param tx Cache transaction.
     * @param map Map.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean putAll(@Nullable IgniteInternalTx tx, Map<Object, IgniteBiTuple<Object, GridCacheVersion>> map)
        throws IgniteCheckedException;

    /**
     * @param tx Cache transaction.
     * @param key Key.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean remove(@Nullable IgniteInternalTx tx, Object key) throws IgniteCheckedException;

    /**
     * @param tx Cache transaction.
     * @param keys Key.
     * @return {@code True} if there is a persistent storage.
     * @throws IgniteCheckedException If storage failed.
     */
    public boolean removeAll(@Nullable IgniteInternalTx tx, Collection<Object> keys)
        throws IgniteCheckedException;

    /**
     * @param tx Transaction.
     * @param commit Commit.
     * @throws IgniteCheckedException If failed.
     */
    public void sessionEnd(IgniteInternalTx tx, boolean commit, boolean last) throws IgniteCheckedException;

    /**
     * End session initiated by write-behind store.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void writeBehindSessionInit() throws IgniteCheckedException;

    /**
     * End session initiated by write-behind store.
     *
     * @param threwEx If exception was thrown.
     * @throws IgniteCheckedException If failed.
     */
    public void writeBehindSessionEnd(boolean threwEx) throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void forceFlush() throws IgniteCheckedException;

    /**
     * @return Convert-binary flag.
     */
    public boolean convertBinary();

    /**
     * @return Configured convert binary flag.
     */
    public boolean configuredConvertBinary();
}

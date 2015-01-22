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

package org.apache.ignite.internal.processors.cache.dr;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.dr.*;
import org.jetbrains.annotations.*;

/**
 * Replication manager class which processes all replication events.
 */
public interface GridCacheDrManager<K, V> extends GridCacheManager<K, V> {
    /**
     * @return Data center ID.
     */
    public byte dataCenterId();

    /**
     * Handles DR for atomic cache.
     *
     * @param e Cache entry.
     * @param op Operation.
     * @param writeObj New value.
     * @param valBytes New value byte.
     * @param ttl TTL.
     * @param drTtl DR TTL.
     * @param drExpireTime DR expire time
     * @param drVer DR version.
     * @return DR result.
     * @throws IgniteCheckedException If update failed.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    public GridDrResolveResult<V> resolveAtomic(GridCacheEntryEx<K, V> e,
         GridCacheOperation op,
         @Nullable Object writeObj,
         @Nullable byte[] valBytes,
         @Nullable IgniteCacheExpiryPolicy expiryPlc,
         long drTtl,
         long drExpireTime,
         @Nullable GridCacheVersion drVer) throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * Handles DR for transactional cache.
     *
     * @param e Cache entry.
     * @param txEntry Transaction entry.
     * @param newVer Version.
     * @param op Operation.
     * @param newVal New value.
     * @param newValBytes New value bytes.
     * @param newTtl TTL.
     * @param newDrExpireTime DR expire time
     * @return DR result.
     * @throws IgniteCheckedException If update failed.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    public GridDrResolveResult<V> resolveTx(
        GridCacheEntryEx<K, V> e,
        IgniteTxEntry<K, V> txEntry,
        GridCacheVersion newVer,
        GridCacheOperation op,
        V newVal,
        byte[] newValBytes,
        long newTtl,
        long newDrExpireTime) throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * Performs replication.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param drType Replication type.
     * @throws IgniteCheckedException If failed.
     */
    public void replicate(K key,
        @Nullable byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        GridDrType drType)throws IgniteCheckedException;

    /**
     * Process partitions "before exchange" event.
     *
     * @param topVer Topology version.
     * @param left {@code True} if exchange has been caused by node leave.
     * @throws IgniteCheckedException If failed.
     */
    public void beforeExchange(long topVer, boolean left) throws IgniteCheckedException;

    /**
     * @return {@code True} is DR is enabled.
     */
    public boolean enabled();

    /**
     * @return {@code True} if receives DR data.
     */
    public boolean receiveEnabled();

    /**
     * In case some partition is evicted, we remove entries of this partition from backup queue.
     *
     * @param part Partition.
     */
    public void partitionEvicted(int part);

    /**
     * Callback for received entries from receiver hub.
     *
     * @param entriesCnt Number of received entries.
     */
    public void onReceiveCacheEntriesReceived(int entriesCnt);

    /**
     * Resets metrics for current cache.
     */
    public void resetMetrics();
}

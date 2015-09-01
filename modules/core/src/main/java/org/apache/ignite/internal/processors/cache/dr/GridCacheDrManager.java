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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.jetbrains.annotations.Nullable;

/**
 * Replication manager class which processes all replication events.
 */
public interface GridCacheDrManager extends GridCacheManager {
    /**
     * @return Data center ID.
     */
    public byte dataCenterId();

    /**
     * Performs replication.
     *
     * @param key Key.
     * @param val Value.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param drType Replication type.
     * @throws IgniteCheckedException If failed.
     */
    public void replicate(KeyCacheObject key,
        @Nullable CacheObject val,
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
    public void beforeExchange(AffinityTopologyVersion topVer, boolean left) throws IgniteCheckedException;

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
     * Callback for manual conflict resolution.
     *
     * @param useNew Use new.
     * @param useOld Use old.
     * @param merge Merge.
     */
    public void onReceiveCacheConflictResolved(boolean useNew, boolean useOld, boolean merge);

    /**
     * Resets metrics for current cache.
     */
    public void resetMetrics();
}
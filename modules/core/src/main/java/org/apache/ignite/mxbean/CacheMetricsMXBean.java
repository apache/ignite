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

package org.apache.ignite.mxbean;

import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;

/**
 * This interface defines JMX view on {@link IgniteCache}.
 */
@MXBeanDescription("MBean that provides access to cache descriptor.")
public interface CacheMetricsMXBean extends CacheStatisticsMXBean, CacheMXBean, CacheMetrics {
    /** {@inheritDoc} */
    @MXBeanDescription("Clear statistics.")
    public void clear();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of hits.")
    public long getCacheHits();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of successful hits.")
    public float getCacheHitPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of misses.")
    public long getCacheMisses();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of accesses that failed to find anything.")
    public float getCacheMissPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of gets.")
    public long getCacheGets();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of puts.")
    public long getCachePuts();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of removals.")
    public long getCacheRemovals();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of eviction entries.")
    public long getCacheEvictions();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time to execute get.")
    public float getAverageGetTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time to execute put.")
    public float getAveragePutTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time to execute remove.")
    public float getAverageRemoveTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time to commit transaction.")
    public float getAverageTxCommitTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time to rollback transaction.")
    public float getAverageTxRollbackTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of transaction commits.")
    public long getCacheTxCommits();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of transaction rollback.")
    public long getCacheTxRollbacks();

    /** {@inheritDoc} */
    @MXBeanDescription("Cache name.")
    public String name();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of entries that was swapped to disk.")
    public long getOverflowSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of gets from off-heap memory.")
    public long getOffHeapGets();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of puts to off-heap memory.")
    public long getOffHeapPuts();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of removed entries from off-heap memory.")
    public long getOffHeapRemovals();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of evictions from off-heap memory.")
    public long getOffHeapEvictions();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of hits on off-heap memory.")
    public long getOffHeapHits();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of hits on off-heap memory.")
    public float getOffHeapHitPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of misses on off-heap memory.")
    public long getOffHeapMisses();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of misses on off-heap memory.")
    public float getOffHeapMissPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of entries stored in off-heap memory.")
    public long getOffHeapEntriesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of primary entries stored in off-heap memory.")
    public long getOffHeapPrimaryEntriesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of backup stored in off-heap memory.")
    public long getOffHeapBackupEntriesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Memory size allocated in off-heap.")
    public long getOffHeapAllocatedSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Off-heap memory maximum size.")
    public long getOffHeapMaxSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of gets from swap.")
    public long getSwapGets();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of puts to swap.")
    public long getSwapPuts();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of removed entries from swap.")
    public long getSwapRemovals();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of hits on swap.")
    public long getSwapHits();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of misses on swap.")
    public long getSwapMisses();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of hits on swap.")
    public float getSwapHitPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of misses on swap.")
    public float getSwapMissPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of entries stored in swap.")
    public long getSwapEntriesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Size of swap.")
    public long getSwapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of non-null values in the cache.")
    public int getSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of keys in the cache (possibly with null values).")
    public int getKeySize();

    /** {@inheritDoc} */
    @MXBeanDescription("True if cache is empty.")
    public boolean isEmpty();

    /** {@inheritDoc} */
    @MXBeanDescription("Current size of evict queue.")
    public int getDhtEvictQueueCurrentSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction per-thread map size.")
    public int getTxThreadMapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction per-Xid map size.")
    public int getTxXidMapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction committed queue size.")
    public int getTxCommitQueueSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction prepared queue size.")
    public int getTxPrepareQueueSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction start version counts map size.")
    public int getTxStartVersionCountsSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction committed ID map size.")
    public int getTxCommittedVersionsSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction rolled back ID map size.")
    public int getTxRolledbackVersionsSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT per-thread map size.")
    public int getTxDhtThreadMapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT per-Xid map size.")
    public int getTxDhtXidMapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT committed queue size.")
    public int getTxDhtCommitQueueSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT prepared queue size.")
    public int getTxDhtPrepareQueueSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT start version counts map size.")
    public int getTxDhtStartVersionCountsSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT committed ID map size.")
    public int getTxDhtCommittedVersionsSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Transaction DHT rolled back ID map size.")
    public int getTxDhtRolledbackVersionsSize();

    /** {@inheritDoc} */
    @MXBeanDescription("True if write-behind is enabled for this cache.")
    public boolean isWriteBehindEnabled();

    /** {@inheritDoc} */
    @MXBeanDescription("Size of internal buffer that triggers flush procedure.")
    public int getWriteBehindFlushSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Count of flush threads.")
    public int getWriteBehindFlushThreadCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Flush frequency interval in milliseconds.")
    public long getWriteBehindFlushFrequency();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum size of batch for similar operations.")
    public int getWriteBehindStoreBatchSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindTotalCriticalOverflowCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindCriticalOverflowCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Count of cache cache entries that are currently in retry state.")
    public int getWriteBehindErrorRetryCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Count of cache entries that are waiting to be flushed.")
    public int getWriteBehindBufferSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Key type.")
    public String getKeyType();

    /** {@inheritDoc} */
    @MXBeanDescription("Value type.")
    public String getValueType();

    /** {@inheritDoc} */
    @MXBeanDescription("True if the cache is store by value.")
    public boolean isStoreByValue();

    /** {@inheritDoc} */
    @MXBeanDescription("True if statistics collection is enabled.")
    public boolean isStatisticsEnabled();

    /** {@inheritDoc} */
    @MXBeanDescription("True if management is enabled.")
    public boolean isManagementEnabled();

    /** {@inheritDoc} */
    @MXBeanDescription("True when a cache is in read-through mode.")
    public boolean isReadThrough();

    /** {@inheritDoc} */
    @MXBeanDescription("True when a cache is in write-through mode.")
    public boolean isWriteThrough();
}
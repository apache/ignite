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
import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * This interface defines JMX view on {@link IgniteCache}.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
@MXBeanDescription("MBean that provides access to cache descriptor.")
public interface CacheMetricsMXBean extends CacheStatisticsMXBean, CacheMXBean, CacheMetrics {
    /** {@inheritDoc} */
    @Override @MXBeanDescription("Clear statistics.")
    public void clear();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of hits.")
    public long getCacheHits();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Percentage of successful hits.")
    public float getCacheHitPercentage();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of misses.")
    public long getCacheMisses();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Percentage of accesses that failed to find anything.")
    public float getCacheMissPercentage();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of gets.")
    public long getCacheGets();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of puts.")
    public long getCachePuts();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of removals.")
    public long getCacheRemovals();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of eviction entries.")
    public long getCacheEvictions();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Average time to execute get.")
    public float getAverageGetTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Average time to execute put.")
    public float getAveragePutTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Average time to execute remove.")
    public float getAverageRemoveTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Average time to commit transaction.")
    public float getAverageTxCommitTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Average time to rollback transaction.")
    public float getAverageTxRollbackTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of transaction commits.")
    public long getCacheTxCommits();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of transaction rollback.")
    public long getCacheTxRollbacks();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Cache name.")
    public String name();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of gets from off-heap memory.")
    public long getOffHeapGets();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of puts to off-heap memory.")
    public long getOffHeapPuts();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of removed entries from off-heap memory.")
    public long getOffHeapRemovals();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of evictions from off-heap memory.")
    public long getOffHeapEvictions();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of hits on off-heap memory.")
    public long getOffHeapHits();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Percentage of hits on off-heap memory.")
    public float getOffHeapHitPercentage();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of misses on off-heap memory.")
    public long getOffHeapMisses();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Percentage of misses on off-heap memory.")
    public float getOffHeapMissPercentage();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of entries stored in off-heap memory.")
    public long getOffHeapEntriesCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of entries in heap memory.")
    public long getHeapEntriesCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of primary entries stored in off-heap memory.")
    public long getOffHeapPrimaryEntriesCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of backup stored in off-heap memory.")
    public long getOffHeapBackupEntriesCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Memory size allocated in off-heap.")
    public long getOffHeapAllocatedSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Cache size.")
    public int getSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of non-null values in the cache as a long value.")
    public long getCacheSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of keys in the cache (possibly with null values).")
    public int getKeySize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("True if cache is empty.")
    public boolean isEmpty();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Current size of evict queue.")
    public int getDhtEvictQueueCurrentSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction per-thread map size.")
    public int getTxThreadMapSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction per-Xid map size.")
    public int getTxXidMapSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction committed queue size.")
    public int getTxCommitQueueSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction prepared queue size.")
    public int getTxPrepareQueueSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction start version counts map size.")
    public int getTxStartVersionCountsSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction committed ID map size.")
    public int getTxCommittedVersionsSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction rolled back ID map size.")
    public int getTxRolledbackVersionsSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT per-thread map size.")
    public int getTxDhtThreadMapSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT per-Xid map size.")
    public int getTxDhtXidMapSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT committed queue size.")
    public int getTxDhtCommitQueueSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT prepared queue size.")
    public int getTxDhtPrepareQueueSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT start version counts map size.")
    public int getTxDhtStartVersionCountsSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT committed ID map size.")
    public int getTxDhtCommittedVersionsSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Transaction DHT rolled back ID map size.")
    public int getTxDhtRolledbackVersionsSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("True if write-behind is enabled for this cache.")
    public boolean isWriteBehindEnabled();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Size of internal buffer that triggers flush procedure.")
    public int getWriteBehindFlushSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Count of flush threads.")
    public int getWriteBehindFlushThreadCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Flush frequency interval in milliseconds.")
    public long getWriteBehindFlushFrequency();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Maximum size of batch for similar operations.")
    public int getWriteBehindStoreBatchSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindTotalCriticalOverflowCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindCriticalOverflowCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Count of cache cache entries that are currently in retry state.")
    public int getWriteBehindErrorRetryCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Count of cache entries that are waiting to be flushed.")
    public int getWriteBehindBufferSize();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Key type.")
    public String getKeyType();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Value type.")
    public String getValueType();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("True if the cache is store by value.")
    public boolean isStoreByValue();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Estimated number of keys to be rebalanced on current node.")
    public long getKeysToRebalanceLeft();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Estimated rebalancing speed in bytes.")
    public long getRebalancingBytesRate();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Estimated rebalancing speed in keys.")
    public long getRebalancingKeysRate();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Number of currently rebalancing partitions on current node.")
    public int getRebalancingPartitionsCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Rebalancing start time.")
    public long getRebalancingStartTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Total number of partitions on current node.")
    public int getTotalPartitionsCount();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("Estimated rebalancing finish time.")
    public long getEstimatedRebalancingFinishTime();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("True if statistics collection is enabled.")
    public boolean isStatisticsEnabled();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("True if management is enabled.")
    public boolean isManagementEnabled();

    /** {@inheritDoc} */
    @Override @MXBeanDescription("True when a cache is in read-through mode.")
    public boolean isReadThrough();

    /** {@inheritDoc} */
    @MXBeanDescription("True when a cache is in write-through mode.")
    @Override public boolean isWriteThrough();

    /** {@inheritDoc} */
    @MXBeanDescription("True when a cache topology is valid for read operations.")
    @Override public boolean isValidForReading();

    /** {@inheritDoc} */
    @MXBeanDescription("True when a cache topology is valid for write operations.")
    @Override public boolean isValidForWriting();

    /** {@inheritDoc} */
    @MXBeanDescription("Tx key collisions. Show key and appropriate collisions queue size for the last " +
        "IGNITE_DUMP_TX_COLLISIONS_INTERVAL.")
    @Override public String getTxKeyCollisions();

    /**
     * Enable statistic collection for the cache.
     */
    @MXBeanDescription("Enable statistic collection for the cache.")
    public void enableStatistics();

    /**
     * Disable statistic collection for the cache.
     */
    @MXBeanDescription("Disable statistic collection for the cache.")
    public void disableStatistics();

    /** {@inheritDoc} */
    @MXBeanDescription("True if index rebuilding in progress.")
    @Override public boolean isIndexRebuildInProgress();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of keys processed during index rebuilding. To get remaining number of keys for " +
        "rebuilding, subtract current value from cache size.")
    @Override public long getIndexRebuildKeysProcessed();
}

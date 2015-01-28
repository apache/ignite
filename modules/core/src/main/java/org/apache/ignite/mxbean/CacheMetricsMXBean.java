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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;

import javax.cache.management.*;

/**
 * This interface defines JMX view on {@link IgniteCache}.
 */
@IgniteMXBeanDescription("MBean that provides access to cache descriptor.")
public interface CacheMetricsMXBean extends CacheStatisticsMXBean, CacheMXBean, CacheMetrics {
    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Clear statistics.")
    public void clear();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of hits.")
    public long getCacheHits();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Percentage of successful hits.")
    public float getCacheHitPercentage();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of misses.")
    public long getCacheMisses();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Percentage of accesses that failed to find anything.")
    public float getCacheMissPercentage();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of gets.")
    public long getCacheGets();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of puts.")
    public long getCachePuts();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of removals.")
    public long getCacheRemovals();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of eviction entries.")
    public long getCacheEvictions();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time to execute get.")
    public float getAverageGetTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time to execute put.")
    public float getAveragePutTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time to execute remove.")
    public float getAverageRemoveTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time to commit transaction.")
    public float getAverageTxCommitTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time to rollback transaction.")
    public float getAverageTxRollbackTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of transaction commits.")
    public long getCacheTxCommits();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of transaction rollback.")
    public long getCacheTxRollbacks();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Cache name.")
    public String name();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of entries that was swapped to disk.")
    public long getOverflowSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of entries stored in off-heap memory.")
    public long getOffHeapEntriesCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Memory size allocated in off-heap.")
    public long getOffHeapAllocatedSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of non-null values in the cache.")
    public int getSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of keys in the cache (possibly with null values).")
    public int getKeySize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True if cache is empty.")
    public boolean isEmpty();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Current size of evict queue.")
    public int getDhtEvictQueueCurrentSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction per-thread map size.")
    public int getTxThreadMapSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction per-Xid map size.")
    public int getTxXidMapSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction committed queue size.")
    public int getTxCommitQueueSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction prepared queue size.")
    public int getTxPrepareQueueSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction start version counts map size.")
    public int getTxStartVersionCountsSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction committed ID map size.")
    public int getTxCommittedVersionsSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction rolled back ID map size.")
    public int getTxRolledbackVersionsSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT per-thread map size.")
    public int getTxDhtThreadMapSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT per-Xid map size.")
    public int getTxDhtXidMapSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT committed queue size.")
    public int getTxDhtCommitQueueSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT prepared queue size.")
    public int getTxDhtPrepareQueueSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT start version counts map size.")
    public int getTxDhtStartVersionCountsSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT committed ID map size.")
    public int getTxDhtCommittedVersionsSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Transaction DHT rolled back ID map size.")
    public int getTxDhtRolledbackVersionsSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True if write-behind is enabled for this cache.")
    public boolean isWriteBehindEnabled();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Size of internal buffer that triggers flush procedure.")
    public int getWriteBehindFlushSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Count of flush threads.")
    public int getWriteBehindFlushThreadCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Flush frequency interval in milliseconds.")
    public long getWriteBehindFlushFrequency();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum size of batch for similar operations.")
    public int getWriteBehindStoreBatchSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindTotalCriticalOverflowCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindCriticalOverflowCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Count of cache cache entries that are currently in retry state.")
    public int getWriteBehindErrorRetryCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Count of cache entries that are waiting to be flushed.")
    public int getWriteBehindBufferSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Key type.")
    public String getKeyType();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Value type.")
    public String getValueType();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True if the cache is store by value.")
    public boolean isStoreByValue();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True if statistics collection is enabled.")
    public boolean isStatisticsEnabled();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True if management is enabled.")
    public boolean isManagementEnabled();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True when a cache is in read-through mode.")
    public boolean isReadThrough();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("True when a cache is in write-through mode.")
    public boolean isWriteThrough();
}

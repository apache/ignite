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

package org.apache.ignite.mbean;

import org.apache.ignite.cache.*;
import org.apache.ignite.*;

/**
 * This interface defines JMX view on {@link IgniteCache}.
 */
import org.apache.ignite.*;

import javax.cache.management.*;

/**
 * This interface defines JMX view on {@link IgniteCache}.
 */
@IgniteMBeanDescription("MBean that provides access to cache descriptor.")
public interface CacheMetricsMXBean extends CacheStatisticsMXBean, CacheMetrics {
    /** {@inheritDoc} */
    @IgniteMBeanDescription("Clear statistics.")
    public void clear();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of hits.")
    public long getCacheHits();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Percentage of successful hits.")
    public float getCacheHitPercentage();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of misses.")
    public long getCacheMisses();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Percentage of accesses that failed to find anything.")
    public float getCacheMissPercentage();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of gets.")
    public long getCacheGets();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of puts.")
    public long getCachePuts();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of removals.")
    public long getCacheRemovals();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of eviction entries.")
    public long getCacheEvictions();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time to execute get.")
    public float getAverageGetTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time to execute put.")
    public float getAveragePutTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time to execute remove.")
    public float getAverageRemoveTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time to commit transaction.")
    public float getAverageTxCommitTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time to rollback transaction.")
    public float getAverageTxRollbackTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of transaction commits.")
    public long getCacheTxCommits();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of transaction rollback.")
    public long getCacheTxRollbacks();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Cache name.")
    public String name();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of entries that was swapped to disk.")
    public long getOverflowSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of entries stored in off-heap memory.")
    public long getOffHeapEntriesCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Memory size allocated in off-heap.")
    public long getOffHeapAllocatedSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of non-null values in the cache.")
    public int getSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of keys in the cache (possibly with null values).")
    public int getKeySize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True if cache is empty.")
    public boolean isEmpty();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Current size of evict queue.")
    public int getDhtEvictQueueCurrentSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction per-thread map size.")
    public int getTxThreadMapSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction per-Xid map size.")
    public int getTxXidMapSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction committed queue size.")
    public int getTxCommitQueueSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction prepared queue size.")
    public int getTxPrepareQueueSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction start version counts map size.")
    public int getTxStartVersionCountsSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction committed ID map size.")
    public int getTxCommittedVersionsSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction rolled back ID map size.")
    public int getTxRolledbackVersionsSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT per-thread map size.")
    public int getTxDhtThreadMapSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT per-Xid map size.")
    public int getTxDhtXidMapSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT committed queue size.")
    public int getTxDhtCommitQueueSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT prepared queue size.")
    public int getTxDhtPrepareQueueSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT start version counts map size.")
    public int getTxDhtStartVersionCountsSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT committed ID map size.")
    public int getTxDhtCommittedVersionsSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Transaction DHT rolled back ID map size.")
    public int getTxDhtRolledbackVersionsSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True if write-behind is enabled for this cache.")
    public boolean isWriteBehindEnabled();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Size of internal buffer that triggers flush procedure.")
    public int getWriteBehindFlushSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Count of flush threads.")
    public int getWriteBehindFlushThreadCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Flush frequency interval in milliseconds.")
    public long getWriteBehindFlushFrequency();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum size of batch for similar operations.")
    public int getWriteBehindStoreBatchSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindTotalCriticalOverflowCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindCriticalOverflowCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Count of cache cache entries that are currently in retry state.")
    public int getWriteBehindErrorRetryCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Count of cache entries that are waiting to be flushed.")
    public int getWriteBehindBufferSize();
}

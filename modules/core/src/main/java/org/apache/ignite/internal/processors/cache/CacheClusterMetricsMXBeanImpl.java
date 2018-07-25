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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.mxbean.CacheMetricsMXBean;

/**
 * Management bean that provides access to {@link IgniteCache IgniteCache}.
 */
class CacheClusterMetricsMXBeanImpl implements CacheMetricsMXBean {
    /** Cache. */
    private GridCacheAdapter<?, ?> cache;

    /**
     * Creates MBean;
     *
     * @param cache Cache.
     */
    CacheClusterMetricsMXBeanImpl(GridCacheAdapter<?, ?> cache) {
        assert cache != null;

        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.clusterMetrics().name();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapGets() {
        return cache.clusterMetrics().getOffHeapGets();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPuts() {
        return cache.clusterMetrics().getOffHeapPuts();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapRemovals() {
        return cache.clusterMetrics().getOffHeapRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEvictions() {
        return cache.clusterMetrics().getOffHeapEvictions();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return cache.clusterMetrics().getOffHeapHits();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        return cache.clusterMetrics().getOffHeapHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return cache.clusterMetrics().getOffHeapMisses();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        return cache.clusterMetrics().getOffHeapMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return cache.clusterMetrics().getOffHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return cache.clusterMetrics().getHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return cache.clusterMetrics().getOffHeapPrimaryEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return cache.clusterMetrics().getOffHeapBackupEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return cache.clusterMetrics().getOffHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return cache.clusterMetrics().getSize();
    }

    /** {@inheritDoc} */
    @Override public long getCacheSize() {
        return cache.clusterMetrics().getCacheSize();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return cache.clusterMetrics().getKeySize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cache.clusterMetrics().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return cache.clusterMetrics().getDhtEvictQueueCurrentSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return cache.clusterMetrics().getTxCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return cache.clusterMetrics().getTxThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return cache.clusterMetrics().getTxXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return cache.clusterMetrics().getTxPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return cache.clusterMetrics().getTxStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return cache.clusterMetrics().getTxCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return cache.clusterMetrics().getTxRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return cache.clusterMetrics().getTxDhtThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return cache.clusterMetrics().getTxDhtXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return cache.clusterMetrics().getTxDhtCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return cache.clusterMetrics().getTxDhtPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return cache.clusterMetrics().getTxDhtStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return cache.clusterMetrics().getTxDhtCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return cache.clusterMetrics().getTxDhtRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehindEnabled() {
        return cache.clusterMetrics().isWriteBehindEnabled();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return cache.clusterMetrics().getWriteBehindFlushSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return cache.clusterMetrics().getWriteBehindFlushThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return cache.clusterMetrics().getWriteBehindFlushFrequency();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return cache.clusterMetrics().getWriteBehindStoreBatchSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return cache.clusterMetrics().getWriteBehindTotalCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return cache.clusterMetrics().getWriteBehindCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return cache.clusterMetrics().getWriteBehindErrorRetryCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return cache.clusterMetrics().getWriteBehindBufferSize();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        try {
            cache.context().shared().cache().clearStatistics(Collections.singleton(cache.name()));
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return cache.clusterMetrics().getCacheHits();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        return cache.clusterMetrics().getCacheHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return cache.clusterMetrics().getCacheMisses();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        return cache.clusterMetrics().getCacheMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return cache.clusterMetrics().getCacheGets();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return cache.clusterMetrics().getCachePuts();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return cache.clusterMetrics().getCacheRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return cache.clusterMetrics().getCacheEvictions();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return cache.clusterMetrics().getAverageGetTime();
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return cache.clusterMetrics().getAveragePutTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return cache.clusterMetrics().getAverageRemoveTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return cache.clusterMetrics().getAverageTxCommitTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return cache.clusterMetrics().getAverageTxRollbackTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return cache.clusterMetrics().getCacheTxCommits();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return cache.clusterMetrics().getCacheTxRollbacks();
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return cache.clusterMetrics().getKeyType();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return cache.clusterMetrics().getValueType();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return cache.clusterMetrics().isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return cache.clusterMetrics().isStatisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return cache.clusterMetrics().isManagementEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return cache.clusterMetrics().isReadThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return cache.clusterMetrics().isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public int getTotalPartitionsCount() {
        return cache.clusterMetrics().getTotalPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancedKeys() {
        return cache.clusterMetrics().getRebalancedKeys();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return cache.clusterMetrics().getEstimatedRebalancingKeys();
    }

    /** {@inheritDoc} */
    @Override public int getRebalancingPartitionsCount() {
        return cache.clusterMetrics().getRebalancingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getKeysToRebalanceLeft() {
        return cache.clusterMetrics().getKeysToRebalanceLeft();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return cache.clusterMetrics().getRebalancingKeysRate();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return cache.clusterMetrics().getRebalancingBytesRate();
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return cache.clusterMetrics().estimateRebalancingFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return cache.clusterMetrics().rebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        return cache.clusterMetrics().getEstimatedRebalancingFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return cache.clusterMetrics().getRebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return cache.clusterMetrics().getRebalanceClearingPartitionsLeft();
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForReading() {
        return cache.clusterMetrics().isValidForReading();
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForWriting() {
        return cache.clusterMetrics().isValidForWriting();
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics() {
        try {
            cache.context().shared().cache().enableStatistics(Collections.singleton(cache.name()), true);
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public void disableStatistics() {
        try {
            cache.context().shared().cache().enableStatistics(Collections.singleton(cache.name()), false);
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}

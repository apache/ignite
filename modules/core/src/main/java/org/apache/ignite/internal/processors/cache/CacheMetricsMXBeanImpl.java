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

import org.apache.ignite.*;
import org.apache.ignite.mxbean.*;

/**
 * Management bean that provides access to {@link IgniteCache IgniteCache}.
 */
class CacheMetricsMXBeanImpl implements CacheMetricsMXBean {
    /** Cache. */
    private GridCacheAdapter<?, ?> cache;

    /**
     * Creates MBean;
     *
     * @param cache Cache.
     */
    CacheMetricsMXBeanImpl(GridCacheAdapter<?, ?> cache) {
        assert cache != null;

        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cache.metrics0().name();
    }

    /** {@inheritDoc} */
    @Override public long getOverflowSize() {
        return cache.metrics0().getOverflowSize();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return cache.metrics0().getOffHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return cache.metrics0().getOffHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return cache.metrics0().getSize();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return cache.metrics0().getKeySize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cache.metrics0().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return cache.metrics0().getDhtEvictQueueCurrentSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return cache.metrics0().getTxCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return cache.metrics0().getTxThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return cache.metrics0().getTxXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return cache.metrics0().getTxPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return cache.metrics0().getTxStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return cache.metrics0().getTxCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return cache.metrics0().getTxRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return cache.metrics0().getTxDhtThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return cache.metrics0().getTxDhtXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return cache.metrics0().getTxDhtCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return cache.metrics0().getTxDhtPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return cache.metrics0().getTxDhtStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return cache.metrics0().getTxDhtCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return cache.metrics0().getTxDhtRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehindEnabled() {
        return cache.metrics0().isWriteBehindEnabled();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return cache.metrics0().getWriteBehindFlushSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return cache.metrics0().getWriteBehindFlushThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return cache.metrics0().getWriteBehindFlushFrequency();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return cache.metrics0().getWriteBehindStoreBatchSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return cache.metrics0().getWriteBehindTotalCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return cache.metrics0().getWriteBehindCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return cache.metrics0().getWriteBehindErrorRetryCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return cache.metrics0().getWriteBehindBufferSize();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        cache.metrics0().clear();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return cache.metrics0().getCacheHits();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        return cache.metrics0().getCacheHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return cache.metrics0().getCacheMisses();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        return cache.metrics0().getCacheMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return cache.metrics0().getCacheGets();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return cache.metrics0().getCachePuts();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return cache.metrics0().getCacheRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return cache.metrics0().getCacheEvictions();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return cache.metrics0().getAverageGetTime();
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return cache.metrics0().getAveragePutTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return cache.metrics0().getAverageRemoveTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return cache.metrics0().getAverageTxCommitTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return cache.metrics0().getAverageTxRollbackTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return cache.metrics0().getCacheTxCommits();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return cache.metrics0().getCacheTxRollbacks();
    }
}

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

package org.apache.ignite.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test task writing predefined metrics values to a stream.
 */
@SuppressWarnings("UnusedDeclaration")
public class PlatformCacheWriteMetricsTask extends ComputeTaskAdapter<Long, Object> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Long ptr) {
        return Collections.singletonMap(new Job(ptr, F.first(subgrid)), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class Job extends ComputeJobAdapter {
        /** Grid. */
        @IgniteInstanceResource
        protected transient Ignite ignite;

        /** Stream ptr. */
        private final long ptr;

        private final ClusterNode node;

        /**
         * Constructor.
         *
         * @param ptr Stream ptr.
         */
        private Job(long ptr, ClusterNode node) {
            this.ptr = ptr;
            this.node = node;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            PlatformContext ctx = PlatformUtils.platformContext(ignite);

            try (PlatformMemory mem = ctx.memory().get(ptr)) {
                PlatformOutputStream out = mem.output();
                BinaryRawWriterEx writer = ctx.writer(out);

                PlatformCache.writeCacheMetrics(writer, new TestCacheMetrics());

                out.synchronize();
            }

            return true;
        }
    }

    /**
     * Predefined metrics.
     */
    private static class TestCacheMetrics implements CacheMetrics {
        /** {@inheritDoc} */
        @Override public long getCacheHits() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getCacheHitPercentage() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCacheMisses() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getCacheMissPercentage() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCacheGets() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCachePuts() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCacheRemovals() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCacheEvictions() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getAverageGetTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getAveragePutTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getAverageRemoveTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getAverageTxCommitTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getAverageTxRollbackTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCacheTxCommits() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getCacheTxRollbacks() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long getOverflowSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapGets() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapPuts() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapRemovals() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapEvictions() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapHits() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getOffHeapHitPercentage() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapMisses() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getOffHeapMissPercentage() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapEntriesCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapPrimaryEntriesCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapBackupEntriesCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapAllocatedSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapMaxSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapGets() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapPuts() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapRemovals() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapHits() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapMisses() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapEntriesCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getSwapSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getSwapHitPercentage() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public float getSwapMissPercentage() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getKeySize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int getDhtEvictQueueCurrentSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxThreadMapSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxXidMapSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxCommitQueueSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxPrepareQueueSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxStartVersionCountsSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxCommittedVersionsSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxRolledbackVersionsSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtThreadMapSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtXidMapSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtCommitQueueSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtPrepareQueueSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtStartVersionCountsSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtCommittedVersionsSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtRolledbackVersionsSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean isWriteBehindEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindFlushSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindFlushThreadCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long getWriteBehindFlushFrequency() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindStoreBatchSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindTotalCriticalOverflowCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindCriticalOverflowCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindErrorRetryCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindBufferSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String getKeyType() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String getValueType() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isStoreByValue() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isStatisticsEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isManagementEnabled() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isReadThrough() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isWriteThrough() {
            return false;
        }
    }
}


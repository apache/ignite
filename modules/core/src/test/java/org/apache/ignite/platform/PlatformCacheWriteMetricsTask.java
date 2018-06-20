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
        return Collections.singletonMap(new Job(ptr), F.first(subgrid));
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

        /**
         * Constructor.
         *
         * @param ptr Stream ptr.
         */
        private Job(long ptr) {
            this.ptr = ptr;
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
            return 1;
        }

        /** {@inheritDoc} */
        @Override public float getCacheHitPercentage() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public long getCacheMisses() {
            return 3;
        }

        /** {@inheritDoc} */
        @Override public float getCacheMissPercentage() {
            return 4;
        }

        /** {@inheritDoc} */
        @Override public long getCacheGets() {
            return 5;
        }

        /** {@inheritDoc} */
        @Override public long getCachePuts() {
            return 6;
        }

        /** {@inheritDoc} */
        @Override public long getCacheRemovals() {
            return 7;
        }

        /** {@inheritDoc} */
        @Override public long getCacheEvictions() {
            return 8;
        }

        /** {@inheritDoc} */
        @Override public float getAverageGetTime() {
            return 9;
        }

        /** {@inheritDoc} */
        @Override public float getAveragePutTime() {
            return 10;
        }

        /** {@inheritDoc} */
        @Override public float getAverageRemoveTime() {
            return 11;
        }

        /** {@inheritDoc} */
        @Override public float getAverageTxCommitTime() {
            return 12;
        }

        /** {@inheritDoc} */
        @Override public float getAverageTxRollbackTime() {
            return 13;
        }

        /** {@inheritDoc} */
        @Override public long getCacheTxCommits() {
            return 14;
        }

        /** {@inheritDoc} */
        @Override public long getCacheTxRollbacks() {
            return 15;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "myCache";
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapGets() {
            return 16;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapPuts() {
            return 17;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapRemovals() {
            return 18;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapEvictions() {
            return 19;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapHits() {
            return 20;
        }

        /** {@inheritDoc} */
        @Override public float getOffHeapHitPercentage() {
            return 21;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapMisses() {
            return 22;
        }

        /** {@inheritDoc} */
        @Override public float getOffHeapMissPercentage() {
            return 23;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapEntriesCount() {
            return 24;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapPrimaryEntriesCount() {
            return 25;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapBackupEntriesCount() {
            return 26;
        }

        /** {@inheritDoc} */
        @Override public long getOffHeapAllocatedSize() {
            return 27;
        }

        /** {@inheritDoc} */
        @Override public int getSize() {
            return 29;
        }

        /** {@inheritDoc} */
        @Override public int getKeySize() {
            return 30;
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int getDhtEvictQueueCurrentSize() {
            return 31;
        }

        /** {@inheritDoc} */
        @Override public int getTxThreadMapSize() {
            return 32;
        }

        /** {@inheritDoc} */
        @Override public int getTxXidMapSize() {
            return 33;
        }

        /** {@inheritDoc} */
        @Override public int getTxCommitQueueSize() {
            return 34;
        }

        /** {@inheritDoc} */
        @Override public int getTxPrepareQueueSize() {
            return 35;
        }

        /** {@inheritDoc} */
        @Override public int getTxStartVersionCountsSize() {
            return 36;
        }

        /** {@inheritDoc} */
        @Override public int getTxCommittedVersionsSize() {
            return 37;
        }

        /** {@inheritDoc} */
        @Override public int getTxRolledbackVersionsSize() {
            return 38;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtThreadMapSize() {
            return 39;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtXidMapSize() {
            return 40;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtCommitQueueSize() {
            return 41;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtPrepareQueueSize() {
            return 42;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtStartVersionCountsSize() {
            return 43;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtCommittedVersionsSize() {
            return 44;
        }

        /** {@inheritDoc} */
        @Override public int getTxDhtRolledbackVersionsSize() {
            return 45;
        }

        /** {@inheritDoc} */
        @Override public boolean isWriteBehindEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindFlushSize() {
            return 46;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindFlushThreadCount() {
            return 47;
        }

        /** {@inheritDoc} */
        @Override public long getWriteBehindFlushFrequency() {
            return 48;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindStoreBatchSize() {
            return 49;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindTotalCriticalOverflowCount() {
            return 50;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindCriticalOverflowCount() {
            return 51;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindErrorRetryCount() {
            return 52;
        }

        /** {@inheritDoc} */
        @Override public int getWriteBehindBufferSize() {
            return 53;
        }

        /** {@inheritDoc} */
        @Override public String getKeyType() {
            return "foo";
        }

        /** {@inheritDoc} */
        @Override public String getValueType() {
            return "bar";
        }

        /** {@inheritDoc} */
        @Override public boolean isStoreByValue() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isStatisticsEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isManagementEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isReadThrough() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isWriteThrough() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isValidForReading() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isValidForWriting() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int getTotalPartitionsCount() {
            return 54;
        }

        /** {@inheritDoc} */
        @Override public int getRebalancingPartitionsCount() {
            return 55;
        }

        /** {@inheritDoc} */
        @Override public long getKeysToRebalanceLeft() {
            return 56;
        }

        /** {@inheritDoc} */
        @Override public long getRebalancingKeysRate() {
            return 57;
        }

        /** {@inheritDoc} */
        @Override public long getRebalancingBytesRate() {
            return 58;
        }

        /** {@inheritDoc} */
        @Override public long getHeapEntriesCount() {
            return 59;
        }

        /** {@inheritDoc} */
        @Override public long estimateRebalancingFinishTime() {
            return 60;
        }

        /** {@inheritDoc} */
        @Override public long rebalancingStartTime() {
            return 61;
        }

        /** {@inheritDoc} */
        @Override public long getEstimatedRebalancingFinishTime() {
            return 62;
        }

        /** {@inheritDoc} */
        @Override public long getRebalancingStartTime() {
            return 63;
        }

        /** {@inheritDoc} */
        @Override public long getRebalanceClearingPartitionsLeft() {
            return 64;
        }

        /** {@inheritDoc} */
        @Override public long getCacheSize() {
            return 65;
        }

        /** {@inheritDoc} */
        @Override public long getRebalancedKeys() {
            return 66;
        }

        /** {@inheritDoc} */
        @Override public long getEstimatedRebalancingKeys() {
            return 67;
        }
    }
}

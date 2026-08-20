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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueClientDisconnectTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueMultiNodeConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicLongClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicReferenceClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureWithJobTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresNoClassOnServerTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteSequenceInternalCleanupTest;
import org.apache.ignite.internal.processors.cache.datastructures.OutOfMemoryVolatileRegionTest;
import org.apache.ignite.internal.processors.cache.datastructures.SemaphoreFailoverNoWaitingAcquirerTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicReferenceMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSequenceMultiThreadedTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSequenceTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSetFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedDataStructuresFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueCreateMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedLockSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedQueueNoBackupsTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicReferenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicReferenceMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetWithClientSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedLockSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheAtomicReplicatedNodeRestartSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache data structures.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheReplicatedSequenceMultiNodeSelfTest.class,
    GridCacheReplicatedSetWithClientSelfTest.class,
    IgniteReplicatedCountDownLatchSelfTest.class,
    IgniteReplicatedSemaphoreSelfTest.class,
    IgniteReplicatedLockSelfTest.class,
    IgniteCacheAtomicReplicatedNodeRestartSelfTest.class,
    OutOfMemoryVolatileRegionTest.class,
    GridCachePartitionedSequenceMultiNodeSelfTest.class,
    GridCachePartitionedQueueApiSelfTest.class,
    GridCachePartitionedAtomicQueueApiSelfTest.class,
    GridCachePartitionedQueueMultiNodeSelfTest.class,
    GridCachePartitionedAtomicQueueMultiNodeSelfTest.class,
    GridCacheQueueClientDisconnectTest.class,
    GridCachePartitionedQueueCreateMultiNodeSelfTest.class,
    GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest.class,
    GridCachePartitionedSetSelfTest.class,
    IgnitePartitionedCountDownLatchSelfTest.class,
    IgniteDataStructureWithJobTest.class,
    IgnitePartitionedSemaphoreSelfTest.class,
    SemaphoreFailoverNoWaitingAcquirerTest.class,
    IgnitePartitionedLockSelfTest.class,
    GridCachePartitionedSetFailoverSelfTest.class,
    GridCachePartitionedAtomicSetFailoverSelfTest.class,
    GridCacheQueueCleanupSelfTest.class,
    GridCachePartitionedDataStructuresFailoverSelfTest.class,
    GridCacheQueueMultiNodeConsistencySelfTest.class,
    IgniteReplicatedAtomicLongApiSelfTest.class,
    GridCachePartitionedAtomicSequenceMultiThreadedTest.class,
    GridCachePartitionedAtomicSequenceTxSelfTest.class,
    GridCacheReplicatedAtomicReferenceApiSelfTest.class,
    GridCachePartitionedAtomicReferenceMultiNodeTest.class,
    GridCacheReplicatedAtomicReferenceMultiNodeTest.class,
    IgniteDataStructuresNoClassOnServerTest.class,
    IgnitePartitionedQueueNoBackupsTest.class,
    IgniteSequenceInternalCleanupTest.class,
    IgniteCacheDataStructuresBinarySelfTestSuite.class,
    IgniteAtomicLongClusterReadOnlyTest.class,
    IgniteAtomicReferenceClusterReadOnlyTest.class,
})
public class IgniteCacheDataStructuresSelfTestSuite {
}

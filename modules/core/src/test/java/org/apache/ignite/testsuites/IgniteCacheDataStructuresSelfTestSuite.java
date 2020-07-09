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

import org.apache.ignite.internal.processors.cache.AtomicCacheAffinityConfigurationTest;
import org.apache.ignite.internal.processors.cache.consistency.AtomicReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ReplicatedExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ReplicatedImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.SingleBackupExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.SingleBackupImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueClientDisconnectTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueMultiNodeConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicLongClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicReferenceClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicSequenceClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicStampedClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDiscoveryDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCountDownLatchClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureUniqueNameTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureWithJobTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresCreateDeniedInClusterReadOnlyMode;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteQueueClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteSequenceInternalCleanupTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteSetClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.SemaphoreFailoverNoWaitingAcquirerTest;
import org.apache.ignite.internal.processors.cache.datastructures.SemaphoreFailoverSafeReleasePermitsTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalAtomicQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalAtomicSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalLockSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicReferenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSequenceMultiThreadedTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSequenceTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSetFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicStampedApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedDataStructuresFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueCreateMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueEntryMoveSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueFailoverDataConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueJoinedNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetWithClientSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetWithNodeFilterSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedQueueNoBackupsTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedSetNoBackupsSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicReferenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicStampedApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedDataStructuresFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetWithClientSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetWithNodeFilterSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedLockSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheAtomicReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.datastructures.GridCacheReplicatedQueueRemoveSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache data structures.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCachePartitionedQueueFailoverDataConsistencySelfTest.class,
    GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest.class,

    GridCacheLocalSequenceApiSelfTest.class,
    GridCacheLocalSetSelfTest.class,
    GridCacheLocalAtomicSetSelfTest.class,
    GridCacheLocalQueueApiSelfTest.class,
    GridCacheLocalAtomicQueueApiSelfTest.class,
    IgniteLocalCountDownLatchSelfTest.class,
    IgniteLocalSemaphoreSelfTest.class,
    IgniteLocalLockSelfTest.class,

    GridCacheReplicatedSequenceApiSelfTest.class,
    GridCacheReplicatedSequenceMultiNodeSelfTest.class,
    GridCacheReplicatedQueueApiSelfTest.class,
    GridCacheReplicatedQueueMultiNodeSelfTest.class,
    GridCacheReplicatedQueueRotativeMultiNodeTest.class,
    GridCacheReplicatedSetSelfTest.class,
    GridCacheReplicatedSetWithClientSelfTest.class,
    GridCacheReplicatedSetWithNodeFilterSelfTest.class,
    GridCacheReplicatedDataStructuresFailoverSelfTest.class,
    IgniteReplicatedCountDownLatchSelfTest.class,
    IgniteReplicatedSemaphoreSelfTest.class,
    IgniteReplicatedLockSelfTest.class,
    IgniteCacheAtomicReplicatedNodeRestartSelfTest.class,
    GridCacheReplicatedQueueRemoveSelfTest.class,

    GridCachePartitionedSequenceApiSelfTest.class,
    GridCachePartitionedSequenceMultiNodeSelfTest.class,
    GridCachePartitionedQueueApiSelfTest.class,
    GridCachePartitionedAtomicQueueApiSelfTest.class,
    GridCachePartitionedQueueMultiNodeSelfTest.class,
    GridCachePartitionedAtomicQueueMultiNodeSelfTest.class,
    GridCacheQueueClientDisconnectTest.class,

    GridCachePartitionedQueueCreateMultiNodeSelfTest.class,
    GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest.class,
    GridCachePartitionedSetSelfTest.class,
    GridCachePartitionedSetWithClientSelfTest.class,
    GridCachePartitionedSetWithNodeFilterSelfTest.class,
    IgnitePartitionedSetNoBackupsSelfTest.class,
    GridCachePartitionedAtomicSetSelfTest.class,
    IgnitePartitionedCountDownLatchSelfTest.class,
    IgniteDataStructureWithJobTest.class,
    IgnitePartitionedSemaphoreSelfTest.class,
    SemaphoreFailoverSafeReleasePermitsTest.class,
    SemaphoreFailoverNoWaitingAcquirerTest.class,
    // TODO IGNITE-3141, enabled when fixed.
    // suite.addTest(new JUnit4TestAdapter(IgnitePartitionedLockSelfTest.class,

    GridCachePartitionedSetFailoverSelfTest.class,
    GridCachePartitionedAtomicSetFailoverSelfTest.class,

    GridCachePartitionedQueueRotativeMultiNodeTest.class,
    GridCachePartitionedAtomicQueueRotativeMultiNodeTest.class,
    GridCacheQueueCleanupSelfTest.class,

    GridCachePartitionedQueueEntryMoveSelfTest.class,

    GridCachePartitionedDataStructuresFailoverSelfTest.class,
    GridCacheQueueMultiNodeConsistencySelfTest.class,

    IgniteLocalAtomicLongApiSelfTest.class,
    IgnitePartitionedAtomicLongApiSelfTest.class,
    IgniteReplicatedAtomicLongApiSelfTest.class,

    GridCachePartitionedAtomicSequenceMultiThreadedTest.class,
    GridCachePartitionedAtomicSequenceTxSelfTest.class,

    GridCachePartitionedAtomicStampedApiSelfTest.class,
    GridCacheReplicatedAtomicStampedApiSelfTest.class,

    GridCachePartitionedAtomicReferenceApiSelfTest.class,
    GridCacheReplicatedAtomicReferenceApiSelfTest.class,

    //suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicReferenceMultiNodeTest.class,
    //suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicReferenceMultiNodeTest.class,

    GridCachePartitionedNodeRestartTxSelfTest.class,
    GridCachePartitionedQueueJoinedNodeSelfTest.class,

    IgniteDataStructureUniqueNameTest.class,
    //suite.addTest(new JUnit4TestAdapter(IgniteDataStructuresNoClassOnServerTest.class,

    IgniteClientDataStructuresTest.class,
    IgniteClientDiscoveryDataStructuresTest.class,

    IgnitePartitionedQueueNoBackupsTest.class,

    IgniteSequenceInternalCleanupTest.class,

    AtomicCacheAffinityConfigurationTest.class,

    IgniteCacheDataStructuresBinarySelfTestSuite.class,

    ImplicitTransactionalReadRepairTest.class,
    SingleBackupImplicitTransactionalReadRepairTest.class,
    ReplicatedImplicitTransactionalReadRepairTest.class,

    AtomicReadRepairTest.class,

    ExplicitTransactionalReadRepairTest.class,
    SingleBackupExplicitTransactionalReadRepairTest.class,
    ReplicatedExplicitTransactionalReadRepairTest.class,

    IgniteAtomicLongClusterReadOnlyTest.class,
    IgniteAtomicReferenceClusterReadOnlyTest.class,
    IgniteAtomicSequenceClusterReadOnlyTest.class,
    IgniteAtomicStampedClusterReadOnlyTest.class,
    IgniteCountDownLatchClusterReadOnlyTest.class,
    IgniteDataStructuresCreateDeniedInClusterReadOnlyMode.class,
    IgniteQueueClusterReadOnlyTest.class,
    IgniteSetClusterReadOnlyTest.class
})
public class IgniteCacheDataStructuresSelfTestSuite {
}

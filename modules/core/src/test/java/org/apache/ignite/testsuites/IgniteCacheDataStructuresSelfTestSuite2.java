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
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicSequenceClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicStampedClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCountDownLatchClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureUniqueNameTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresCreateDeniedInClusterReadOnlyMode;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteQueueClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteSetClusterReadOnlyTest;
import org.apache.ignite.internal.processors.cache.datastructures.SemaphoreFailoverSafeReleasePermitsTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicReferenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicStampedApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueEntryMoveSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueFailoverDataConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueJoinedNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetWithClientSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSetWithNodeFilterSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedSetNoBackupsSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicStampedApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedDataStructuresFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetWithNodeFilterSelfTest;
import org.apache.ignite.internal.processors.datastructures.GridCacheReplicatedQueueRemoveSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Split off from {@link IgniteCacheDataStructuresSelfTestSuite} to reduce the single-suite
 * runtime in CI; contains an independent subset of the same test classes.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCachePartitionedQueueFailoverDataConsistencySelfTest.class,
    GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest.class,
    GridCacheReplicatedSequenceApiSelfTest.class,
    GridCacheReplicatedQueueApiSelfTest.class,
    GridCacheReplicatedQueueMultiNodeSelfTest.class,
    GridCacheReplicatedQueueRotativeMultiNodeTest.class,
    GridCacheReplicatedSetSelfTest.class,
    GridCacheReplicatedSetWithNodeFilterSelfTest.class,
    GridCacheReplicatedDataStructuresFailoverSelfTest.class,
    GridCacheReplicatedQueueRemoveSelfTest.class,
    GridCachePartitionedSequenceApiSelfTest.class,
    GridCachePartitionedSetWithClientSelfTest.class,
    GridCachePartitionedSetWithNodeFilterSelfTest.class,
    IgnitePartitionedSetNoBackupsSelfTest.class,
    GridCachePartitionedAtomicSetSelfTest.class,
    SemaphoreFailoverSafeReleasePermitsTest.class,
    GridCachePartitionedQueueRotativeMultiNodeTest.class,
    GridCachePartitionedAtomicQueueRotativeMultiNodeTest.class,
    GridCachePartitionedQueueEntryMoveSelfTest.class,
    IgnitePartitionedAtomicLongApiSelfTest.class,
    GridCachePartitionedAtomicStampedApiSelfTest.class,
    GridCacheReplicatedAtomicStampedApiSelfTest.class,
    GridCachePartitionedAtomicReferenceApiSelfTest.class,
    GridCachePartitionedNodeRestartTxSelfTest.class,
    GridCachePartitionedQueueJoinedNodeSelfTest.class,
    IgniteDataStructureUniqueNameTest.class,
    IgniteClientDataStructuresTest.class,
    AtomicCacheAffinityConfigurationTest.class,
    IgniteAtomicSequenceClusterReadOnlyTest.class,
    IgniteAtomicStampedClusterReadOnlyTest.class,
    IgniteCountDownLatchClusterReadOnlyTest.class,
    IgniteDataStructuresCreateDeniedInClusterReadOnlyMode.class,
    IgniteQueueClusterReadOnlyTest.class,
    IgniteSetClusterReadOnlyTest.class,
})
public class IgniteCacheDataStructuresSelfTestSuite2 {
}

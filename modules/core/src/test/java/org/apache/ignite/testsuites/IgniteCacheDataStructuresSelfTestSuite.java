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

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.AtomicCacheAffinityConfigurationTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueClientDisconnectTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueMultiNodeConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDiscoveryDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureUniqueNameTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureWithJobTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresNoClassOnServerTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteSequenceInternalCleanupTest;
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
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicReferenceMultiNodeTest;
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
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedQueueNoBackupsTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedSetNoBackupsSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicReferenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicReferenceMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedAtomicStampedApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedDataStructuresFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueRotativeMultiNodeTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedLockSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.IgniteReplicatedSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheAtomicReplicatedNodeRestartSelfTest;

/**
 * Test suite for cache data structures.
 */
public class IgniteCacheDataStructuresSelfTestSuite extends TestSuite {
    /**
     * @return Cache test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Data Structures Test Suite");

        for (int i = 0; i< 20; i++)
        suite.addTest(new TestSuite(GridCachePartitionedDataStructuresFailoverSelfTest.class));

        return suite;
    }
}

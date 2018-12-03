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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.AtomicCacheAffinityConfigurationTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueClientDisconnectTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueMultiNodeConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDiscoveryDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureUniqueNameTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureWithJobTest;
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

        // Data structures.
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueFailoverDataConsistencySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheLocalSequenceApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalAtomicSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalQueueApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalAtomicQueueApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteLocalCountDownLatchSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteLocalSemaphoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteLocalLockSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedSequenceApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedSequenceMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedQueueApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedQueueMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedQueueRotativeMultiNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedSetWithClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedSetWithNodeFilterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedDataStructuresFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteReplicatedCountDownLatchSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteReplicatedSemaphoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteReplicatedLockSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicReplicatedNodeRestartSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSequenceApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSequenceMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicQueueApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicQueueMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueueClientDisconnectTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSetWithClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSetWithNodeFilterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePartitionedSetNoBackupsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePartitionedCountDownLatchSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDataStructureWithJobTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePartitionedSemaphoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SemaphoreFailoverSafeReleasePermitsTest.class));
        suite.addTest(new JUnit4TestAdapter(SemaphoreFailoverNoWaitingAcquirerTest.class));
        // TODO IGNITE-3141, enabled when fixed.
        // suite.addTest(new JUnit4TestAdapter(IgnitePartitionedLockSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSetFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicSetFailoverSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueRotativeMultiNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicQueueRotativeMultiNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueueCleanupSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueEntryMoveSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedDataStructuresFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueueMultiNodeConsistencySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteLocalAtomicLongApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePartitionedAtomicLongApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteReplicatedAtomicLongApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicSequenceMultiThreadedTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicSequenceTxSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicStampedApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicStampedApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicReferenceApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicReferenceApiSelfTest.class));

        //suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicReferenceMultiNodeTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicReferenceMultiNodeTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNodeRestartTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedQueueJoinedNodeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDataStructureUniqueNameTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteDataStructuresNoClassOnServerTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteClientDataStructuresTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientDiscoveryDataStructuresTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePartitionedQueueNoBackupsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteSequenceInternalCleanupTest.class));

        suite.addTestSuite(AtomicCacheAffinityConfigurationTest.class);

        return suite;
    }
}

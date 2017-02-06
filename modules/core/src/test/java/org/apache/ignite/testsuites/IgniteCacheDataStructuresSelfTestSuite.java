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
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueMultiNodeConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDiscoveryDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureUniqueNameTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructureWithJobTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalAtomicOffheapSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalAtomicQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalAtomicSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalOffheapQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalAtomicLongApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalCountDownLatchSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalLockSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.IgniteLocalSemaphoreSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicOffheapQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicOffheapQueueCreateMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicOffheapQueueMultiNodeSelfTest;
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
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedOffHeapValuesQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedOffHeapValuesSetSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedOffheapDataStructuresFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedOffheapSetFailoverSelfTest;
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

        // Data structures.
        suite.addTest(new TestSuite(GridCachePartitionedQueueFailoverDataConsistencySelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest.class));

        suite.addTest(new TestSuite(GridCacheLocalSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicOffheapSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalOffheapQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(IgniteLocalCountDownLatchSelfTest.class));
        suite.addTest(new TestSuite(IgniteLocalSemaphoreSelfTest.class));
        suite.addTest(new TestSuite(IgniteLocalLockSelfTest.class));

        suite.addTest(new TestSuite(GridCacheReplicatedSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedSequenceMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedDataStructuresFailoverSelfTest.class));
        suite.addTest(new TestSuite(IgniteReplicatedCountDownLatchSelfTest.class));
        suite.addTest(new TestSuite(IgniteReplicatedSemaphoreSelfTest.class));
        suite.addTest(new TestSuite(IgniteReplicatedLockSelfTest.class));
        suite.addTest(new TestSuite(IgniteCacheAtomicReplicatedNodeRestartSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedSequenceMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffHeapValuesQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicOffheapQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicOffheapQueueMultiNodeSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicOffheapQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedSetSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffHeapValuesSetSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedSetNoBackupsSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSetSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedCountDownLatchSelfTest.class));
        suite.addTest(new TestSuite(IgniteDataStructureWithJobTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedSemaphoreSelfTest.class));
        // TODO https://issues.apache.org/jira/browse/IGNITE-4173, enable when fixed.
        // suite.addTest(new TestSuite(SemaphoreFailoverSafeReleasePermitsTest.class));
        // TODO IGNITE-3141, enabled when fixed.
        // suite.addTest(new TestSuite(IgnitePartitionedLockSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedSetFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffheapSetFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSetFailoverSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCacheQueueCleanupSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedQueueEntryMoveSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedDataStructuresFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffheapDataStructuresFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCacheQueueMultiNodeConsistencySelfTest.class));

        suite.addTest(new TestSuite(IgniteLocalAtomicLongApiSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedAtomicLongApiSelfTest.class));
        suite.addTest(new TestSuite(IgniteReplicatedAtomicLongApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicSequenceMultiThreadedTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSequenceTxSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicStampedApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedAtomicStampedApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicReferenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedAtomicReferenceApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedNodeRestartTxSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueJoinedNodeSelfTest.class));

        suite.addTest(new TestSuite(IgniteDataStructureUniqueNameTest.class));

        suite.addTest(new TestSuite(IgniteClientDataStructuresTest.class));
        suite.addTest(new TestSuite(IgniteClientDiscoveryDataStructuresTest.class));

        suite.addTest(new TestSuite(IgnitePartitionedQueueNoBackupsTest.class));

        return suite;
    }
}

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

import junit.framework.*;
import org.apache.ignite.internal.processors.cache.datastructures.*;
import org.apache.ignite.internal.processors.cache.datastructures.local.*;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.*;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.*;

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
        // TODO: IGNITE-264
        // suite.addTest(new TestSuite(GridCachePartitionedQueueFailoverDataConsistencySelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest.class));

        suite.addTest(new TestSuite(GridCacheLocalSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicOffheapSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalOffheapQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(IgniteLocalCountDownLatchSelfTest.class));

        suite.addTest(new TestSuite(GridCacheReplicatedSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedSequenceMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedSetSelfTest.class));
        // TODO: GG-5306
        // suite.addTest(new TestSuite(GridCacheReplicatedDataStructuresFailoverSelfTest.class));
        suite.addTest(new TestSuite(IgniteReplicatedCountDownLatchSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedSequenceMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffHeapValuesQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicOffheapQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicOffheapQueueMultiNodeSelfTest.class));

        // TODO: IGNITE-80.
        //suite.addTest(new TestSuite(GridCachePartitionedQueueCreateMultiNodeSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedAtomicOffheapQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedSetSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffHeapValuesSetSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedSetNoBackupsSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSetSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedCountDownLatchSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedSetFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOffheapSetFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSetFailoverSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCacheQueueCleanupSelfTest.class));

        // TODO: GG-5620 Uncomment when fix
        //suite.addTest(new TestSuite(GridCachePartitionedQueueEntryMoveSelfTest.class));

        // TODO: GG-2699
        //suite.addTest(new TestSuite(GridCachePartitionedDataStructuresFailoverSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedOffheapDataStructuresFailoverSelfTest.class));
        // TODO: GG-4807 Uncomment when fix
        // suite.addTest(new TestSuite(GridCacheQueueMultiNodeConsistencySelfTest.class));

        suite.addTest(new TestSuite(IgniteLocalAtomicLongApiSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedAtomicLongApiSelfTest.class));
        suite.addTest(new TestSuite(IgniteReplicatedAtomicLongApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicSequenceMultiThreadedTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicStampedApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedAtomicStampedApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicReferenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedAtomicReferenceApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedNodeRestartTxSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueJoinedNodeSelfTest.class));

        suite.addTest(new TestSuite(IgniteDataStructureUniqueNameTest.class));

        return suite;
    }
}

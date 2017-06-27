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
import org.apache.ignite.GridCacheAffinityBackupsSelfTest;
import org.apache.ignite.IgniteCacheAffinitySelfTest;
import org.apache.ignite.cache.affinity.AffinityClientNodeSelfTest;
import org.apache.ignite.cache.affinity.AffinityHistoryCleanupTest;
import org.apache.ignite.cache.affinity.local.LocalAffinityFunctionTest;
import org.apache.ignite.internal.GridCachePartitionExchangeManagerHistSizeTest;
import org.apache.ignite.internal.processors.cache.CacheKeepBinaryTransactionTest;
import org.apache.ignite.internal.processors.cache.CacheNearReaderUpdateTest;
import org.apache.ignite.internal.processors.cache.CacheRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSerializableTransactionsTest;
import org.apache.ignite.internal.processors.cache.ClusterStatePartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.EntryVersionConsistencyReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePutStackOverflowSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheReadThroughEvictionsVariationsSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheStoreCollectionTest;
import org.apache.ignite.internal.processors.cache.PartitionsExchangeOnDiscoveryHistoryOverflowTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLateAffinityAssignmentNodeJoinValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLateAffinityAssignmentTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteActiveOnStartNodeJoinValidationSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheGroupsPartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxIteratorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.IgniteCacheAtomicProtocolTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.CacheManualRebalancingTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheSyncRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgniteCacheWriteBehindNoUpdateSelfTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite5 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 5");

        suite.addTestSuite(CacheSerializableTransactionsTest.class);
        suite.addTestSuite(CacheNearReaderUpdateTest.class);
        suite.addTestSuite(IgniteCacheStoreCollectionTest.class);
        suite.addTestSuite(IgniteCacheWriteBehindNoUpdateSelfTest.class);
        suite.addTestSuite(IgniteCachePutStackOverflowSelfTest.class);
        suite.addTestSuite(CacheKeepBinaryTransactionTest.class);

        suite.addTestSuite(CacheLateAffinityAssignmentTest.class);
        suite.addTestSuite(CacheLateAffinityAssignmentNodeJoinValidationTest.class);
        suite.addTestSuite(IgniteActiveOnStartNodeJoinValidationSelfTest.class);
        suite.addTestSuite(EntryVersionConsistencyReadThroughTest.class);
        suite.addTestSuite(IgniteCacheSyncRebalanceModeSelfTest.class);

        suite.addTest(IgniteCacheReadThroughEvictionsVariationsSuite.suite());
        suite.addTestSuite(IgniteCacheTxIteratorSelfTest.class);

        suite.addTestSuite(ClusterStatePartitionedSelfTest.class);
        suite.addTestSuite(ClusterStateReplicatedSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionLossPolicySelfTest.class);
        suite.addTestSuite(IgniteCacheGroupsPartitionLossPolicySelfTest.class);

        suite.addTestSuite(CacheRebalancingSelfTest.class);
        suite.addTestSuite(CacheManualRebalancingTest.class);

        // Affinity tests.
        suite.addTestSuite(GridCacheAffinityBackupsSelfTest.class);
        suite.addTestSuite(IgniteCacheAffinitySelfTest.class);
        suite.addTestSuite(AffinityClientNodeSelfTest.class);
        suite.addTestSuite(LocalAffinityFunctionTest.class);
        suite.addTestSuite(AffinityHistoryCleanupTest.class);

        suite.addTestSuite(IgniteCacheAtomicProtocolTest.class);

        suite.addTestSuite(PartitionsExchangeOnDiscoveryHistoryOverflowTest.class);

        suite.addTestSuite(GridCachePartitionExchangeManagerHistSizeTest.class);

        return suite;
    }
}

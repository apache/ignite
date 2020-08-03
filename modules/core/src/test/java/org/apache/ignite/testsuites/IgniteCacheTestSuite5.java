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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.GridCacheAffinityBackupsSelfTest;
import org.apache.ignite.IgniteCacheAffinitySelfTest;
import org.apache.ignite.cache.affinity.AffinityClientNodeSelfTest;
import org.apache.ignite.cache.affinity.AffinityDistributionLoggingTest;
import org.apache.ignite.cache.affinity.AffinityHistoryCleanupTest;
import org.apache.ignite.cache.affinity.local.LocalAffinityFunctionTest;
import org.apache.ignite.internal.GridCacheHashMapPutAllWarningsTest;
import org.apache.ignite.internal.GridCachePartitionExchangeManagerHistSizeTest;
import org.apache.ignite.internal.GridCachePartitionExchangeManagerWarningsTest;
import org.apache.ignite.internal.processors.cache.CacheCreateDestroyClusterReadOnlyModeTest;
import org.apache.ignite.internal.processors.cache.CacheKeepBinaryTransactionTest;
import org.apache.ignite.internal.processors.cache.CacheNearReaderUpdateTest;
import org.apache.ignite.internal.processors.cache.CacheRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSerializableTransactionsTest;
import org.apache.ignite.internal.processors.cache.ClientSlowDiscoveryTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.ClientSlowDiscoveryTransactionRemapTest;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTest;
import org.apache.ignite.internal.processors.cache.ClusterStateClientPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateClientReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateNoRebalancePartitionedTest;
import org.apache.ignite.internal.processors.cache.ClusterStateNoRebalanceReplicatedTest;
import org.apache.ignite.internal.processors.cache.ClusterStatePartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateThinClientPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateThinClientReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.ConcurrentCacheStartTest;
import org.apache.ignite.internal.processors.cache.EntryVersionConsistencyReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePutStackOverflowSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheReadThroughEvictionsVariationsSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheStoreCollectionTest;
import org.apache.ignite.internal.processors.cache.PartitionsExchangeOnDiscoveryHistoryOverflowTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLateAffinityAssignmentNodeJoinValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLateAffinityAssignmentTest;
import org.apache.ignite.internal.processors.cache.distributed.GridExchangeFreeCellularSwitchComplexOperationsTest;
import org.apache.ignite.internal.processors.cache.distributed.GridExchangeFreeCellularSwitchIsolationTest;
import org.apache.ignite.internal.processors.cache.distributed.GridExchangeFreeSwitchTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheGroupsPartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxIteratorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.NotMappedPartitionInTxTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.IgniteCacheAtomicProtocolTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.CacheManualRebalancingTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheSyncRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgniteCacheWriteBehindNoUpdateSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite5 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        GridTestUtils.addTestIfNeeded(suite, CacheSerializableTransactionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheNearReaderUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheStoreCollectionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheWriteBehindNoUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePutStackOverflowSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheKeepBinaryTransactionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheLateAffinityAssignmentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLateAffinityAssignmentNodeJoinValidationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridExchangeFreeSwitchTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridExchangeFreeCellularSwitchComplexOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridExchangeFreeCellularSwitchIsolationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, EntryVersionConsistencyReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSyncRebalanceModeSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxIteratorSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ClusterStatePartitionedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateClientPartitionedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateThinClientPartitionedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateNoRebalancePartitionedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateReplicatedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateClientReplicatedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateThinClientReplicatedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateNoRebalanceReplicatedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterReadOnlyModeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheCreateDestroyClusterReadOnlyModeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionLossPolicySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheGroupsPartitionLossPolicySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheRebalancingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheManualRebalancingTest.class, ignoredTests);

        // Affinity tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityBackupsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAffinitySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AffinityClientNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LocalAffinityFunctionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AffinityHistoryCleanupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AffinityDistributionLoggingTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicProtocolTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PartitionsExchangeOnDiscoveryHistoryOverflowTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionExchangeManagerHistSizeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionExchangeManagerWarningsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheHashMapPutAllWarningsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, NotMappedPartitionInTxTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ConcurrentCacheStartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReadThroughEvictionsVariationsSuite.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ClientSlowDiscoveryTopologyChangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClientSlowDiscoveryTransactionRemapTest.class, ignoredTests);

        //GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPreloadSelfTest.class, ignoredTests);

        return suite;
    }
}

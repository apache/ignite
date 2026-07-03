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
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeColocatedBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.MdcAffinityBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionExcludeNeighborsSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionStandardHashSelfTest;
import org.apache.ignite.internal.IgniteReflectionFactorySelfTest;
import org.apache.ignite.internal.processors.cache.CacheComparatorTest;
import org.apache.ignite.internal.processors.cache.CacheDhtLocalPartitionAfterRemoveSelfTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsSingleNodeTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsTest;
import org.apache.ignite.internal.processors.cache.CacheGroupLocalConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOptimisticTransactionsWithFilterTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedProjectionAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteAtomicCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheIncrementTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionMapUpdateSafeLossPolicyTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheAndNodeStop;
import org.apache.ignite.internal.processors.cache.IgniteOnePhaseCommitNearReadersTest;
import org.apache.ignite.internal.processors.cache.NonAffinityCoordinatorDynamicStartStopTest;
import org.apache.ignite.internal.processors.cache.RebalanceIteratorLargeEntriesOOMTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTestAllowOverwrite;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionStateTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionNotLoadedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTransformEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodeChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.CacheGetReadFromBackupFailoverTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedOptimisticTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicEvictionNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEvictionNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadUnloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadWaitForBackupsTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedSupplyEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedTopologyChangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedBackupNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCrossCacheTxNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.IgniteCacheContainsKeyColocatedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.IgniteCacheContainsKeyNearAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearDynamicStartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearJobExecutionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOneNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPrimarySyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicOpSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicStoreMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLoadCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiThreadedPutGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheStoreUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridPartitionedBackupLoadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheContainsKeyNearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheNearTxRollbackTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCacheMultithreadedUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCacheSyncUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NoneRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedJobExecutionTest;
import org.apache.ignite.internal.processors.continuous.IgniteContinuousQueryMetadataUpdateTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite2 {
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

        GridTestUtils.addTestIfNeeded(suite, GridCacheTransformEventSelfTest.class, ignoredTests);

        // Partitioned cache.
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicApiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicOpSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NoneRebalanceModeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearOneNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridNearCacheStoreUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicStoreMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMultiNodeLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMultiThreadedPutGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNodeFailureSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxSingleThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxTimeoutSelfTest.class, ignoredTests);

        // Preload
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadUnloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadWaitForBackupsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheDhtLocalPartitionAfterRemoveSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLoadingConcurrentGridStartSelfTestAllowOverwrite.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridPartitionedBackupLoadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetReadFromBackupFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedLoadCacheSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionNotLoadedEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedEvictionSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTopologyChangeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedSupplyEventsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedOptimisticTransactionSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheOffheapUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearDynamicStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPrimarySyncSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClientNodeChangingTopologyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionMapUpdateSafeLossPolicyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteAtomicCacheEntryProcessorNodeJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheAndNodeStop.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NearCacheSyncUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEnumOperationsSingleNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEnumOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheIncrementTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionedBackupNodeFailureRecoveryTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteContinuousQueryMetadataUpdateTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, NearCacheMultithreadedUpdateTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearTxRollbackTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyNearSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyNearAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyColocatedAtomicSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteOnePhaseCommitNearReadersTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheOptimisticTransactionsWithFilterTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, NonAffinityCoordinatorDynamicStartStopTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtAtomicEvictionNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtEvictionNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCrossCacheTxNearEnabledSelfTest.class, ignoredTests);

        // Configuration validation
        GridTestUtils.addTestIfNeeded(suite, CacheGroupLocalConfigurationSelfTest.class, ignoredTests);

        // Affinity and collocation
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedAffinitySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedProjectionAffinitySelfTest.class, ignoredTests);

        // Other tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearJobExecutionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedJobExecutionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionExcludeNeighborsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionStandardHashSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionBackupFilterSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterNodeAttributeColocatedBackupFilterSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MdcAffinityBackupFilterSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CachePartitionStateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheComparatorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteReflectionFactorySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RebalanceIteratorLargeEntriesOOMTest.class, ignoredTests);

        return suite;
    }
}

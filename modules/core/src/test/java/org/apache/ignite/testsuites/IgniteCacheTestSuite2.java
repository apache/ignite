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
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodeChangingTopologyTest;
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

        // Local cache.
        /*GridTestUtils.addTestIfNeeded(suite, GridCacheLocalBasicApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalBasicStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalBasicStoreMultithreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalAtomicBasicStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalAtomicGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalLoadAllSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalMultithreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalTxSingleThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalTxReadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalTxTimeoutSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalEvictionEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalTxMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalIsolatedNodesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheFastNodeLeftForTransactionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheTransformEventSelfTest.class, ignoredTests);

        // Partitioned cache.
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicApiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicOpSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearMultiGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NoneRebalanceModeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearOneNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearReaderPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedAtomicGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridNearCacheStoreUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicStoreMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheConcurrentReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMultiNodeLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMultiThreadedPutGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNodeFailureSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedExplicitLockNodeFailureSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLockReleaseNodeLeaveTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNestedTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxConcurrentGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxReadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxSingleThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedTxSingleThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxTimeoutSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheFinishPartitionsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledTxMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtEntrySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtMappingSelfTest.class, ignoredTests);

        // Preload
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadOnheapSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadBigDataSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadPutGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadDisabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedPreloadRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPreloadRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadStartStopSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadUnloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedPreloadLifecycleSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadDelayedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadWaitForBackupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RebalanceIsProcessingWhenAssignmentIsEmptyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheDhtLocalPartitionAfterRemoveSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLoadingConcurrentGridStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLoadingConcurrentGridStartSelfTestAllowOverwrite.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTxLoadingConcurrentGridStartSelfTestAllowOverwrite.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridPartitionedBackupLoadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetReadFromBackupFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedLoadCacheSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionNotLoadedEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtEvictionsDisabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearEvictionEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearEvictionEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedEvictionSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTopologyChangeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedUnloadEventsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedSupplyEventsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedOptimisticTransactionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicMessageCountSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPartitionedClearSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheOffheapUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearClientHitTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPrimarySyncSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedPrimarySyncSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionMapUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClientNodePartitionsExchangeTest.class, ignoredTests);*/
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClientNodeChangingTopologyTest.class, ignoredTests);
        /*GridTestUtils.addTestIfNeeded(suite, IgniteCacheServerNodeConcurrentStart.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionMapUpdateSafeLossPolicyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryProcessorNodeJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteAtomicCacheEntryProcessorNodeJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearTxForceKeyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CrossCacheTxRandomOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CrossCacheTxNearEnabledRandomOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheAndNodeStop.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NearCacheSyncUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEnumOperationsSingleNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEnumOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheIncrementTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionedBackupNodeFailureRecoveryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheVariableTopologySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteNoCustomEventsOnNodeStart.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheExchangeMessageDuplicatedStateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteContinuousQueryMetadataUpdateTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, NearCacheMultithreadedUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NearCachePutAllMultinodeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteOnePhaseCommitInvokeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNoSyncForGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearTxRollbackTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyNearSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyColocatedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyNearAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyColocatedAtomicSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteOnePhaseCommitNearReadersTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteNearClientCacheCloseTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClientCacheStartFailoverTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheOptimisticTransactionsWithFilterSingleServerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheOptimisticTransactionsWithFilterTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, NonAffinityCoordinatorDynamicStartStopTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClearDuringRebalanceTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedDebugTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtAtomicEvictionNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtEvictionNearReadersSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadMessageCountTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCrossCacheTxNearEnabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxConsistencyColocatedRestartSelfTest.class, ignoredTests);

        // Configuration validation
        GridTestUtils.addTestIfNeeded(suite, CacheConfigurationLeakTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MemoryPolicyConfigValidationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MemoryPolicyInitializationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGroupLocalConfigurationSelfTest.class, ignoredTests);

        // Affinity and collocation
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedAffinitySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedProjectionAffinitySelfTest.class, ignoredTests);

        // Other tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearJobExecutionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedJobExecutionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionExcludeNeighborsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionStandardHashSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRendezvousAffinityClientSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionBackupFilterSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterNodeAttributeAffinityBackupFilterSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterNodeAttributeColocatedBackupFilterSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CachePartitionStateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheComparatorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePartitionPartialCountersMapSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteReflectionFactorySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NoPresentCacheInterceptorOnClientTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDetectLostPartitionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TransactionValidationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RebalanceIteratorLargeEntriesOOMTest.class, ignoredTests);*/

        return suite;
    }
}

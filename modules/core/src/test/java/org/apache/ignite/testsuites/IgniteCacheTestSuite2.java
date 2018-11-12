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

import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionExcludeNeighborsSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionStandardHashSelfTest;
import org.apache.ignite.internal.IgniteReflectionFactorySelfTest;
import org.apache.ignite.internal.processors.cache.CacheComparatorTest;
import org.apache.ignite.internal.processors.cache.CacheConcurrentReadThroughTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationLeakTest;
import org.apache.ignite.internal.processors.cache.CacheDhtLocalPartitionAfterRemoveSelfTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsSingleNodeTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsTest;
import org.apache.ignite.internal.processors.cache.CacheExchangeMessageDuplicatedStateTest;
import org.apache.ignite.internal.processors.cache.CacheGroupLocalConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOptimisticTransactionsWithFilterSingleServerTest;
import org.apache.ignite.internal.processors.cache.CacheOptimisticTransactionsWithFilterTest;
import org.apache.ignite.internal.processors.cache.CrossCacheTxNearEnabledRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.CrossCacheTxRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFinishPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedProjectionAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVariableTopologySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteAtomicCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheIncrementTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoSyncForGetTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionMapUpdateTest;
import org.apache.ignite.internal.processors.cache.IgniteClientCacheStartFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheAndNodeStop;
import org.apache.ignite.internal.processors.cache.IgniteNearClientCacheCloseTest;
import org.apache.ignite.internal.processors.cache.IgniteOnePhaseCommitInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteOnePhaseCommitNearReadersTest;
import org.apache.ignite.internal.processors.cache.MemoryPolicyConfigValidationTest;
import org.apache.ignite.internal.processors.cache.NonAffinityCoordinatorDynamicStartStopTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTestAllowOverwrite;
import org.apache.ignite.internal.processors.cache.distributed.CacheLockReleaseNodeLeaveTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionStateTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheTxLoadingConcurrentGridStartSelfTestAllowOverwrite;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionNotLoadedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionedNearDisabledTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTransformEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodeChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodePartitionsExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheServerNodeConcurrentStart;
import org.apache.ignite.internal.processors.cache.distributed.dht.CachePartitionPartialCountersMapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedOptimisticTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedPreloadRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedPrimarySyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEntrySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEvictionsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtMappingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadBigDataSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDelayedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadPutGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadStartStopSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadUnloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedTopologyChangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedUnloadEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheClearDuringRebalanceTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedBackupNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearClientHitTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearJobExecutionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMultiGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOneNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPartitionedClearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPreloadRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPrimarySyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReaderPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxForceKeyTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicOpSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicStoreMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedExplicitLockNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLoadCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiThreadedPutGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedPreloadLifecycleSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheRendezvousAffinityClientSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheStoreUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridPartitionedBackupLoadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCacheSyncUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NoneRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedJobExecutionTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalBasicApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalEventSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalIsolatedNodesSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalLoadAllSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalLockSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.MemoryPolicyInitializationTest;
import org.apache.ignite.internal.processors.continuous.IgniteNoCustomEventsOnNodeStart;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite2 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "false");

        TestSuite suite = new TestSuite("IgniteCache Test Suite part 2");

        // Local cache.
        suite.addTestSuite(GridCacheLocalBasicApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalBasicStoreSelfTest.class);
        //suite.addTestSuite(GridCacheLocalBasicStoreMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicBasicStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalGetAndTransformStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicGetAndTransformStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalLoadAllSelfTest.class);
        suite.addTestSuite(GridCacheLocalLockSelfTest.class);
        suite.addTestSuite(GridCacheLocalMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxSingleThreadedSelfTest.class);
        //suite.addTestSuite(GridCacheLocalTxReadTest.class);
        suite.addTestSuite(GridCacheLocalTxTimeoutSelfTest.class);
        suite.addTestSuite(GridCacheLocalEventSelfTest.class);
        suite.addTestSuite(GridCacheLocalEvictionEventSelfTest.class);
        suite.addTestSuite(GridCacheVariableTopologySelfTest.class);
        suite.addTestSuite(GridCacheLocalTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCacheTransformEventSelfTest.class);
        suite.addTestSuite(GridCacheLocalIsolatedNodesSelfTest.class);

        // Partitioned cache.
        suite.addTestSuite(GridCachePartitionedGetSelfTest.class);
        suite.addTest(new TestSuite(GridCachePartitionedBasicApiTest.class));
        suite.addTest(new TestSuite(GridCacheNearMultiGetSelfTest.class));
        suite.addTest(new TestSuite(NoneRebalanceModeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearJobExecutionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedJobExecutionTest.class));
        suite.addTest(new TestSuite(GridCacheNearOneNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearReadersSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearReaderPreloadSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearReadersSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAffinitySelfTest.class));
        //suite.addTest(new TestSuite(RendezvousAffinityFunctionSelfTest.class));
        suite.addTest(new TestSuite(RendezvousAffinityFunctionExcludeNeighborsSelfTest.class));
        suite.addTest(new TestSuite(RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest.class));
        suite.addTest(new TestSuite(RendezvousAffinityFunctionStandardHashSelfTest.class));
        suite.addTest(new TestSuite(GridCacheRendezvousAffinityClientSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedProjectionAffinitySelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedBasicOpSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedBasicStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedGetAndTransformStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicGetAndTransformStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedBasicStoreMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedEventSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedLockSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedNearDisabledLockSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedMultiNodeLockSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedMultiThreadedPutGetSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedNodeFailureSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedExplicitLockNodeFailureSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedTxSingleThreadedSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedEntryLockSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedEvictionSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedNestedTxTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedStorePutSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedTxConcurrentGetTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedTxMultiNodeSelfTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedTxReadTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedTxSingleThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedTxTimeoutSelfTest.class));
        suite.addTest(new TestSuite(GridCacheFinishPartitionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtEntrySelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtMappingSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedTxMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedNearDisabledTxMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadOnheapSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadBigDataSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadPutGetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadDisabledSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(CacheDhtLocalPartitionAfterRemoveSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedPreloadRestartSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearPreloadRestartSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadUnloadSelfTest.class));
        suite.addTest(new TestSuite(RendezvousAffinityFunctionBackupFilterSelfTest.class));
        suite.addTest(new TestSuite(ClusterNodeAttributeAffinityBackupFilterSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedPreloadLifecycleSelfTest.class));
        suite.addTest(new TestSuite(CacheLoadingConcurrentGridStartSelfTest.class));
        suite.addTest(new TestSuite(CacheLoadingConcurrentGridStartSelfTestAllowOverwrite.class));
        suite.addTest(new TestSuite(CacheTxLoadingConcurrentGridStartSelfTestAllowOverwrite.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadDelayedSelfTest.class));
        suite.addTest(new TestSuite(GridPartitionedBackupLoadSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedLoadCacheSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionNotLoadedEventSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtEvictionsDisabledSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearEvictionEventSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearEvictionEventSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedTopologyChangeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedUnloadEventsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedOptimisticTransactionSelfTest.class));
        suite.addTestSuite(GridCacheAtomicMessageCountSelfTest.class);
        suite.addTest(new TestSuite(GridCacheNearPartitionedClearSelfTest.class));

        suite.addTest(new TestSuite(GridCacheOffheapUpdateSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearClientHitTest.class));
        suite.addTest(new TestSuite(GridCacheNearPrimarySyncSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedPrimarySyncSelfTest.class));

        suite.addTest(new TestSuite(IgniteCachePartitionMapUpdateTest.class));
        suite.addTest(new TestSuite(IgniteCacheClientNodePartitionsExchangeTest.class));
        suite.addTest(new TestSuite(IgniteCacheClientNodeChangingTopologyTest.class));
        suite.addTest(new TestSuite(IgniteCacheServerNodeConcurrentStart.class));

        suite.addTest(new TestSuite(IgniteCacheEntryProcessorNodeJoinTest.class));
        suite.addTest(new TestSuite(IgniteAtomicCacheEntryProcessorNodeJoinTest.class));
        suite.addTest(new TestSuite(GridCacheNearTxForceKeyTest.class));
        suite.addTest(new TestSuite(CrossCacheTxRandomOperationsTest.class));
        suite.addTest(new TestSuite(CrossCacheTxNearEnabledRandomOperationsTest.class));
        suite.addTest(new TestSuite(IgniteDynamicCacheAndNodeStop.class));
        suite.addTest(new TestSuite(CacheLockReleaseNodeLeaveTest.class));
        suite.addTest(new TestSuite(NearCacheSyncUpdateTest.class));
        suite.addTest(new TestSuite(CacheConfigurationLeakTest.class));
        suite.addTest(new TestSuite(MemoryPolicyConfigValidationTest.class));
        suite.addTest(new TestSuite(MemoryPolicyInitializationTest.class));
        suite.addTest(new TestSuite(CacheGroupLocalConfigurationSelfTest.class));
        suite.addTest(new TestSuite(CacheEnumOperationsSingleNodeTest.class));
        suite.addTest(new TestSuite(CacheEnumOperationsTest.class));
        suite.addTest(new TestSuite(IgniteCacheIncrementTxTest.class));
        suite.addTest(new TestSuite(IgniteCachePartitionedBackupNodeFailureRecoveryTest.class));

        suite.addTest(new TestSuite(IgniteNoCustomEventsOnNodeStart.class));

        suite.addTest(new TestSuite(CacheExchangeMessageDuplicatedStateTest.class));
        suite.addTest(new TestSuite(CacheConcurrentReadThroughTest.class));

        suite.addTest(new TestSuite(GridNearCacheStoreUpdateTest.class));
        //suite.addTest(new TestSuite(NearCacheMultithreadedUpdateTest.class));
        //suite.addTest(new TestSuite(NearCachePutAllMultinodeTest.class));

        suite.addTest(new TestSuite(IgniteOnePhaseCommitInvokeTest.class));

        suite.addTest(new TestSuite(IgniteCacheNoSyncForGetTest.class));
        //suite.addTest(new TestSuite(IgniteCacheContainsKeyNearSelfTest.class));
        //suite.addTest(new TestSuite(IgniteCacheNearTxRollbackTest.class));

        suite.addTest(new TestSuite(IgniteOnePhaseCommitNearReadersTest.class));
        suite.addTest(new TestSuite(IgniteNearClientCacheCloseTest.class));
        suite.addTest(new TestSuite(IgniteClientCacheStartFailoverTest.class));

        suite.addTest(new TestSuite(CacheOptimisticTransactionsWithFilterSingleServerTest.class));
        suite.addTest(new TestSuite(CacheOptimisticTransactionsWithFilterTest.class));

        suite.addTest(new TestSuite(NonAffinityCoordinatorDynamicStartStopTest.class));

        suite.addTest(new TestSuite(IgniteCacheClearDuringRebalanceTest.class));

        suite.addTest(new TestSuite(CachePartitionStateTest.class));

        suite.addTest(new TestSuite(CacheComparatorTest.class));

        suite.addTest(new TestSuite(CachePartitionPartialCountersMapSelfTest.class));

        suite.addTest(new TestSuite(IgniteReflectionFactorySelfTest.class));

        //suite.addTest(new TestSuite(GridCacheColocatedDebugTest.class));
        //suite.addTest(new TestSuite(GridCacheDhtAtomicEvictionNearReadersSelfTest.class));
        //suite.addTest(new TestSuite(GridCacheDhtEntrySetSelfTest.class));
        //suite.addTest(new TestSuite(GridCacheDhtEvictionNearReadersSelfTest.class));
        //suite.addTest(new TestSuite(GridCacheDhtMultiBackupTest.class));
        //suite.addTest(new TestSuite(GridCacheDhtPreloadMessageCountTest.class));
        //suite.addTest(new TestSuite(GridCachePartitionedNearDisabledMetricsSelfTest.class));
        //suite.addTest(new TestSuite(IgniteCacheContainsKeyColocatedSelfTest.class));
        //suite.addTest(new TestSuite(IgniteCrossCacheTxNearEnabledSelfTest.class));
        //suite.addTest(new TestSuite(IgniteTxConsistencyColocatedRestartSelfTest.class));

        return suite;
    }
}

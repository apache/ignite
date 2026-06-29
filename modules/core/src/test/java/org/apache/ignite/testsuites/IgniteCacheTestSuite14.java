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
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest;
import org.apache.ignite.internal.processors.cache.CacheConcurrentReadThroughTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationLeakTest;
import org.apache.ignite.internal.processors.cache.CacheExchangeMessageDuplicatedStateTest;
import org.apache.ignite.internal.processors.cache.CacheOptimisticTransactionsWithFilterSingleServerTest;
import org.apache.ignite.internal.processors.cache.CrossCacheTxNearEnabledRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.CrossCacheTxRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFastNodeLeftForTransactionTest;
import org.apache.ignite.internal.processors.cache.GridCacheFinishPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVariableTopologySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoSyncForGetTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionMapUpdateTest;
import org.apache.ignite.internal.processors.cache.IgniteClientCacheStartFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteNearClientCacheCloseTest;
import org.apache.ignite.internal.processors.cache.IgniteOnePhaseCommitInvokeTest;
import org.apache.ignite.internal.processors.cache.NoPresentCacheInterceptorOnClientTest;
import org.apache.ignite.internal.processors.cache.TransactionValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDetectLostPartitionsTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLockReleaseNodeLeaveTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheTxLoadingConcurrentGridStartSelfTestAllowOverwrite;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionedNearDisabledTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodePartitionsExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheServerNodeConcurrentStart;
import org.apache.ignite.internal.processors.cache.distributed.dht.CachePartitionPartialCountersMapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedDebugTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedPreloadRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedPrimarySyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEntrySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEvictionsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtMappingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadBigDataSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDelayedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadMessageCountTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadPutGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadStartStopSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedUnloadEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheClearDuringRebalanceTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheContainsKeyColocatedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteTxConsistencyColocatedRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.RebalanceIsProcessingWhenAssignmentIsEmptyTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearClientHitTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMultiGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPartitionedClearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPreloadRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReaderPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxForceKeyTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedExplicitLockNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNestedTxTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedPreloadLifecycleSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxConcurrentGetTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxReadTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheRendezvousAffinityClientSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCachePutAllMultinodeTest;
import org.apache.ignite.internal.processors.continuous.IgniteNoCustomEventsOnNodeStart;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Split off from {@link IgniteCacheTestSuite2} to reduce the single-suite runtime in CI;
 * contains an independent subset of the same test classes.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite14 {
    /**
     * @return Test suite.
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

        GridTestUtils.addTestIfNeeded(suite, GridCacheFastNodeLeftForTransactionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearMultiGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearReaderPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedAtomicGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedBasicStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheConcurrentReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedExplicitLockNodeFailureSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLockReleaseNodeLeaveTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNestedTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxConcurrentGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxReadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedTxSingleThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheFinishPartitionsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTxMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedNearDisabledTxMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtEntrySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtMappingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadOnheapSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadBigDataSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadPutGetSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadDisabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedPreloadRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPreloadRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadStartStopSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedPreloadLifecycleSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadDelayedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RebalanceIsProcessingWhenAssignmentIsEmptyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLoadingConcurrentGridStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTxLoadingConcurrentGridStartSelfTestAllowOverwrite.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtEvictionsDisabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearEvictionEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearEvictionEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedUnloadEventsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicMessageCountSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPartitionedClearSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearClientHitTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedPrimarySyncSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionMapUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClientNodePartitionsExchangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheServerNodeConcurrentStart.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryProcessorNodeJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearTxForceKeyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CrossCacheTxRandomOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CrossCacheTxNearEnabledRandomOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheVariableTopologySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteNoCustomEventsOnNodeStart.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheExchangeMessageDuplicatedStateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NearCachePutAllMultinodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOnePhaseCommitInvokeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNoSyncForGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyColocatedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteNearClientCacheCloseTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClientCacheStartFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheOptimisticTransactionsWithFilterSingleServerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheClearDuringRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedDebugTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadMessageCountTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxConsistencyColocatedRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheConfigurationLeakTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRendezvousAffinityClientSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterNodeAttributeAffinityBackupFilterSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePartitionPartialCountersMapSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NoPresentCacheInterceptorOnClientTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDetectLostPartitionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TransactionValidationTest.class, ignoredTests);

        return suite;
    }
}

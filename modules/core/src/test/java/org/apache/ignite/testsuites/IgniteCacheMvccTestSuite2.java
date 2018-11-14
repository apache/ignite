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

import java.util.HashSet;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionExcludeNeighborsSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionStandardHashSelfTest;
import org.apache.ignite.internal.IgniteReflectionFactorySelfTest;
import org.apache.ignite.internal.processors.cache.CacheComparatorTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationLeakTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsSingleNodeTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsTest;
import org.apache.ignite.internal.processors.cache.CacheExchangeMessageDuplicatedStateTest;
import org.apache.ignite.internal.processors.cache.CacheGroupLocalConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOptimisticTransactionsWithFilterSingleServerTest;
import org.apache.ignite.internal.processors.cache.CacheOptimisticTransactionsWithFilterTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedProjectionAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteAtomicCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoSyncForGetTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionMapUpdateTest;
import org.apache.ignite.internal.processors.cache.IgniteClientCacheStartFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheAndNodeStop;
import org.apache.ignite.internal.processors.cache.IgniteNearClientCacheCloseTest;
import org.apache.ignite.internal.processors.cache.MemoryPolicyConfigValidationTest;
import org.apache.ignite.internal.processors.cache.NonAffinityCoordinatorDynamicStartStopTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTestAllowOverwrite;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionStateTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionedNearDisabledMvccTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionedNearDisabledTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTransformEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodePartitionsExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheServerNodeConcurrentStart;
import org.apache.ignite.internal.processors.cache.distributed.dht.CachePartitionPartialCountersMapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedMvccTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedOptimisticTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicEvictionNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedUnloadEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedBackupNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearClientHitTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearJobExecutionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxForceKeyTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMvccTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMvccTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMvccTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheRendezvousAffinityClientSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCacheSyncUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NoneRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedJobExecutionTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.MemoryPolicyInitializationTest;
import org.apache.ignite.internal.processors.continuous.IgniteNoCustomEventsOnNodeStart;

/**
 * Test suite.
 */
public class IgniteCacheMvccTestSuite2 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>(128);
        // Optimistic tx tests
        ignoredTests.add(GridCacheColocatedOptimisticTransactionSelfTest.class);
        ignoredTests.add(CacheOptimisticTransactionsWithFilterSingleServerTest.class);
        ignoredTests.add(CacheOptimisticTransactionsWithFilterTest.class);

        // On-heap cache test.
        ignoredTests.add(GridCacheDhtPreloadOnheapSelfTest.class);

        // Atomic cache tests.
        ignoredTests.add(GridCacheLocalAtomicBasicStoreSelfTest.class);
        ignoredTests.add(GridCacheLocalAtomicGetAndTransformStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicNearMultiNodeSelfTest.class);
        ignoredTests.add(GridCacheAtomicNearReadersSelfTest.class);
        ignoredTests.add(GridCachePartitionedAtomicGetAndTransformStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicNearEvictionEventSelfTest.class);
        ignoredTests.add(GridCacheAtomicMessageCountSelfTest.class);
        ignoredTests.add(IgniteAtomicCacheEntryProcessorNodeJoinTest.class);
        ignoredTests.add(GridCacheDhtAtomicEvictionNearReadersSelfTest.class);
        ignoredTests.add(GridCacheNearClientHitTest.class);
        ignoredTests.add(GridCacheNearTxForceKeyTest.class);
        ignoredTests.add(CacheLoadingConcurrentGridStartSelfTest.class);
        ignoredTests.add(CacheLoadingConcurrentGridStartSelfTestAllowOverwrite.class);
        ignoredTests.add(IgniteCachePartitionedBackupNodeFailureRecoveryTest.class);

        // Other non-tx tests.
        ignoredTests.add(RendezvousAffinityFunctionSelfTest.class);
        ignoredTests.add(RendezvousAffinityFunctionExcludeNeighborsSelfTest.class);
        ignoredTests.add(RendezvousAffinityFunctionFastPowerOfTwoHashSelfTest.class);
        ignoredTests.add(RendezvousAffinityFunctionStandardHashSelfTest.class);
        ignoredTests.add(GridCachePartitionedAffinitySelfTest.class);
        ignoredTests.add(GridCacheRendezvousAffinityClientSelfTest.class);
        ignoredTests.add(GridCachePartitionedProjectionAffinitySelfTest.class);
        ignoredTests.add(RendezvousAffinityFunctionBackupFilterSelfTest.class);
        ignoredTests.add(ClusterNodeAttributeAffinityBackupFilterSelfTest.class);
        ignoredTests.add(NonAffinityCoordinatorDynamicStartStopTest.class);

        ignoredTests.add(NoneRebalanceModeSelfTest.class);
        ignoredTests.add(IgniteCachePartitionMapUpdateTest.class);
        ignoredTests.add(IgniteCacheClientNodePartitionsExchangeTest.class);
        ignoredTests.add(IgniteCacheServerNodeConcurrentStart.class);

        ignoredTests.add(GridCachePartitionedUnloadEventsSelfTest.class);

        ignoredTests.add(IgniteNoCustomEventsOnNodeStart.class);
        ignoredTests.add(CacheExchangeMessageDuplicatedStateTest.class);
        ignoredTests.add(IgniteDynamicCacheAndNodeStop.class);

        ignoredTests.add(GridCacheReplicatedJobExecutionTest.class);
        ignoredTests.add(GridCacheNearJobExecutionSelfTest.class);

        ignoredTests.add(CacheConfigurationLeakTest.class);
        ignoredTests.add(MemoryPolicyConfigValidationTest.class);
        ignoredTests.add(MemoryPolicyInitializationTest.class);
        ignoredTests.add(CacheGroupLocalConfigurationSelfTest.class);

        ignoredTests.add(CachePartitionStateTest.class);
        ignoredTests.add(CacheComparatorTest.class);
        ignoredTests.add(CachePartitionPartialCountersMapSelfTest.class);
        ignoredTests.add(IgniteReflectionFactorySelfTest.class);

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(GridCacheTransformEventSelfTest.class);
        ignoredTests.add(IgniteClientCacheStartFailoverTest.class);
        ignoredTests.add(IgniteNearClientCacheCloseTest.class);
        ignoredTests.add(IgniteCacheNoSyncForGetTest.class);
        ignoredTests.add(CacheEnumOperationsSingleNodeTest.class);
        ignoredTests.add(CacheEnumOperationsTest.class);
        ignoredTests.add(NearCacheSyncUpdateTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        // TODO IGNITE-10175: refactor these tests (use assume) to support both mvcc and non-mvcc modes after moving to JUnit4/5.
        ignoredTests.add(GridCachePartitionedTxSingleThreadedSelfTest.class); // See GridCachePartitionedMvccTxSingleThreadedSelfTest
        ignoredTests.add(GridCacheColocatedTxSingleThreadedSelfTest.class); // See GridCacheColocatedMvccTxSingleThreadedSelfTest
        ignoredTests.add(GridCachePartitionedTxMultiThreadedSelfTest.class); // See GridCachePartitionedMvccTxMultiThreadedSelfTest
        ignoredTests.add(GridCachePartitionedNearDisabledTxMultiThreadedSelfTest.class); // See GridCachePartitionedNearDisabledMvccTxMultiThreadedSelfTest
        ignoredTests.add(GridCachePartitionedTxTimeoutSelfTest.class); // See GridCachePartitionedMvccTxTimeoutSelfTest

        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 2");

        suite.addTest(IgniteCacheTestSuite2.suite(ignoredTests));

        // Add Mvcc clones.
        suite.addTestSuite(GridCachePartitionedMvccTxSingleThreadedSelfTest.class);
        suite.addTestSuite(GridCacheColocatedMvccTxSingleThreadedSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMvccTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledMvccTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMvccTxTimeoutSelfTest.class);

        return suite;
    }
}

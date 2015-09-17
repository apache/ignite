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

import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.GridCacheAffinityBackupsSelfTest;
import org.apache.ignite.IgniteCacheAffinitySelfTest;
import org.apache.ignite.cache.IgniteWarmupClosureSelfTest;
import org.apache.ignite.cache.affinity.IgniteClientNodeAffinityTest;
import org.apache.ignite.cache.affinity.fair.GridFairAffinityFunctionNodesSelfTest;
import org.apache.ignite.cache.affinity.fair.GridFairAffinityFunctionSelfTest;
import org.apache.ignite.cache.affinity.fair.IgniteFairAffinityDynamicCacheSelfTest;
import org.apache.ignite.cache.store.GridCacheBalancingStoreSelfTest;
import org.apache.ignite.cache.store.GridCacheLoadOnlyStoreAdapterSelfTest;
import org.apache.ignite.cache.store.StoreResourceInjectionSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreMultitreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreMultithreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreSelfTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityCallSelfTest;
import org.apache.ignite.internal.processors.cache.CacheFutureExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityApiSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAsyncOperationsLimitSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheColocatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLifecycleAwareSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLocalTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMissingCommitVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMixedPartitionExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheNearTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheObjectToStringSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapTieredAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapTieredEvictionAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapTieredEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheP2PUndeploySelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedOffHeapLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStopSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStorePutxSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStoreValueBytesSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSwapPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSwapReloadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTxPartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPrimaryWriteOrderInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPrimaryWriteOrderWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerEagerTtlDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheManyAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNearLockValueSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTransactionalStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteClientAffinityAssignmentSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllLargeBatchSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllUpdateNonPreloadedPartitionSelfTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheAtomicExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheContinuousExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheIsolatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheP2PDisableExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCachePartitionedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCachePrivateExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheReplicatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheSharedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheTxExecutionContextTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicNearUpdateTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheTxNearUpdateTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheEntrySetIterationPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSystemTransactionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCrossCacheTxStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheGlobalLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheGetStoreErrorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxExceptionSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImplSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultinodeCreateCacheTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite");

        suite.addTestSuite(IgniteCacheEntryListenerAtomicTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerAtomicReplicatedTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerAtomicLocalTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerTxTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerTxReplicatedTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerTxLocalTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerEagerTtlDisabledTest.class);

        suite.addTestSuite(IgniteClientAffinityAssignmentSelfTest.class);

        suite.addTestSuite(IgniteCacheAtomicInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderWithStoreInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalWithStoreInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxLocalInvokeTest.class);
        suite.addTestSuite(IgniteCrossCacheTxStoreSelfTest.class);

        suite.addTestSuite(IgnitePutAllLargeBatchSelfTest.class);
        suite.addTestSuite(IgnitePutAllUpdateNonPreloadedPartitionSelfTest.class);

        // User's class loader tests.
        suite.addTestSuite(IgniteCacheAtomicExecutionContextTest.class);
        suite.addTestSuite(IgniteCachePartitionedExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheReplicatedExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheTxExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheContinuousExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheIsolatedExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheP2PDisableExecutionContextTest.class);
        suite.addTestSuite(IgniteCachePrivateExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheSharedExecutionContextTest.class);

        // Warmup closure tests.
        suite.addTestSuite(IgniteWarmupClosureSelfTest.class);

        // Affinity tests.
        suite.addTestSuite(GridFairAffinityFunctionNodesSelfTest.class);
        suite.addTestSuite(GridFairAffinityFunctionSelfTest.class);
        suite.addTestSuite(IgniteFairAffinityDynamicCacheSelfTest.class);
        suite.addTestSuite(GridCacheAffinityBackupsSelfTest.class);
        suite.addTestSuite(IgniteCacheAffinitySelfTest.class);
        suite.addTestSuite(IgniteClientNodeAffinityTest.class);

        // Swap tests.
        suite.addTestSuite(GridCacheSwapPreloadSelfTest.class);
        suite.addTestSuite(GridCacheSwapReloadSelfTest.class);

        // Common tests.
        suite.addTestSuite(GridCacheConcurrentMapSelfTest.class);
        suite.addTestSuite(GridCacheAffinityMapperSelfTest.class);
        suite.addTestSuite(CacheAffinityCallSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityRoutingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMvccSelfTest.class, ignoredTests);
        suite.addTestSuite(GridCacheMvccPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheMvccManagerSelfTest.class);
        suite.addTestSuite(GridCacheP2PUndeploySelfTest.class);
        suite.addTestSuite(GridCacheConfigurationValidationSelfTest.class);
        suite.addTestSuite(GridCacheConfigurationConsistencySelfTest.class);
        suite.addTestSuite(GridCacheJdbcBlobStoreSelfTest.class);
        suite.addTestSuite(GridCacheJdbcBlobStoreMultithreadedSelfTest.class);
        suite.addTestSuite(CacheJdbcPojoStoreTest.class);
        suite.addTestSuite(CacheJdbcPojoStoreMultitreadedSelfTest.class);
        suite.addTestSuite(GridCacheBalancingStoreSelfTest.class);
        suite.addTestSuite(GridCacheAffinityApiSelfTest.class);
        suite.addTestSuite(GridCacheStoreValueBytesSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorSelfTest.class, ignoredTests);
        suite.addTestSuite(DataStreamerMultiThreadedSelfTest.class);
        suite.addTestSuite(DataStreamerMultinodeCreateCacheTest.class);
        suite.addTestSuite(DataStreamerImplSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridCacheEntryMemorySizeSelfTest.class, ignoredTests);
        suite.addTestSuite(GridCacheClearAllSelfTest.class);
        suite.addTestSuite(GridCacheObjectToStringSelfTest.class);
        suite.addTestSuite(GridCacheLoadOnlyStoreAdapterSelfTest.class);
        suite.addTestSuite(GridCacheGetStoreErrorSelfTest.class);
        suite.addTestSuite(StoreResourceInjectionSelfTest.class);
        suite.addTestSuite(CacheFutureExceptionSelfTest.class);
        suite.addTestSuite(GridCacheAsyncOperationsLimitSelfTest.class);
        suite.addTestSuite(IgniteCacheManyAsyncOperationsTest.class);
        suite.addTestSuite(GridCacheTtlManagerSelfTest.class);
        suite.addTestSuite(GridCacheLifecycleAwareSelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicStopBusySelfTest.class);
        suite.addTestSuite(IgniteCacheTransactionalStopBusySelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearCacheSelfTest.class);
        suite.addTestSuite(CacheAtomicNearUpdateTopologyChangeTest.class);
        suite.addTestSuite(CacheTxNearUpdateTopologyChangeTest.class);
        suite.addTestSuite(GridCacheStorePutxSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapMultiThreadedUpdateSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest.class);
        suite.addTestSuite(GridCacheColocatedTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheNearTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheMissingCommitVersionSelfTest.class);
        suite.addTestSuite(GridCacheEntrySetIterationPreloadingSelfTest.class);
        suite.addTestSuite(GridCacheMixedPartitionExchangeSelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicMessageRecoveryTest.class);
        suite.addTestSuite(IgniteCacheTxMessageRecoveryTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridCacheOffHeapTieredEvictionAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheOffHeapTieredEvictionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheOffHeapTieredAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheOffHeapTieredSelfTest.class, ignoredTests);
        suite.addTestSuite(GridCacheGlobalLoadTest.class);
        suite.addTestSuite(GridCachePartitionedLocalStoreSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedLocalStoreSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapLocalStoreSelfTest.class);
        suite.addTestSuite(GridCacheTxPartitionedLocalStoreSelfTest.class);
        suite.addTestSuite(IgniteCacheSystemTransactionsSelfTest.class);

        suite.addTest(IgniteCacheTcpClientDiscoveryTestSuite.suite());

        // Heuristic exception handling.
        suite.addTestSuite(GridCacheColocatedTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheNearTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheStopSelfTest.class);

        suite.addTestSuite(IgniteCacheNearLockValueSelfTest.class);

        return suite;
    }
}

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
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.context.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.*;
import org.apache.ignite.internal.processors.cache.local.*;
import org.apache.ignite.internal.processors.datastreamer.*;
import org.apache.ignite.testframework.*;

import java.util.*;

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
        suite.addTestSuite(CacheFutureExceptionSelfTest.class);
        suite.addTestSuite(GridCacheAsyncOperationsLimitSelfTest.class);
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

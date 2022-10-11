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
import org.apache.ignite.cache.IgniteCacheEntryProcessorSequentialCallTest;
import org.apache.ignite.cache.IgniteWarmupClosureSelfTest;
import org.apache.ignite.cache.store.GridCacheBalancingStoreSelfTest;
import org.apache.ignite.cache.store.GridCacheLoadOnlyStoreAdapterSelfTest;
import org.apache.ignite.cache.store.StoreArrayKeyTest;
import org.apache.ignite.cache.store.StoreResourceInjectionSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoWriteBehindConnectionLeakTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinarySelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinaryWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreMultitreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoWriteBehindStoreWithCoalescingTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreMultithreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreSelfTest;
import org.apache.ignite.cache.store.jdbc.JdbcTypesDefaultTransformerTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityCallSelfTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityKeyConfigurationMismatchTest;
import org.apache.ignite.internal.processors.cache.CacheEntryProcessorCopySelfTest;
import org.apache.ignite.internal.processors.cache.CacheFutureExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.CacheWithDifferentDataRegionConfigurationTest;
import org.apache.ignite.internal.processors.cache.DataStorageConfigurationValidationTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityApiSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAsyncOperationsLimitSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheObjectToStringSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheP2PUndeploySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStoreValueBytesSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridDataStorageConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicConcurrentUnorderedUpdateAllTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerEagerTtlDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheManyAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteClientAffinityAssignmentSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllLargeBatchSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllUpdateNonPreloadedPartitionSelfTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheAtomicExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheContinuousExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheIsolatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheP2PDisableExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCachePrivateExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheReplicatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheSharedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheTxExecutionContextTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCrossCacheTxStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheGetStoreErrorSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorExternalizableFailedTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorNonSerializableTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorPersistenceSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerClientReconnectAfterClusterRestartTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCommunicationSpiExceptionTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImplSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultinodeCreateCacheTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerStopCacheTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerTimeoutTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerUpdateAfterLoadTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite {
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

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryListenerAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryListenerAtomicReplicatedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryListenerTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryListenerTxReplicatedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryListenerEagerTtlDisabledTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteClientAffinityAssignmentSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicInvokeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledInvokeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicWithStoreInvokeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicConcurrentUnorderedUpdateAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxInvokeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEntryProcessorNonSerializableTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEntryProcessorExternalizableFailedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryProcessorCallTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledInvokeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCrossCacheTxStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheEntryProcessorSequentialCallTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheEntryProcessorCopySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePutAllLargeBatchSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePutAllUpdateNonPreloadedPartitionSelfTest.class, ignoredTests);

        // User's class loader tests.
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReplicatedExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContinuousExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheIsolatedExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheP2PDisableExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePrivateExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSharedExecutionContextTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, StoreArrayKeyTest.class, ignoredTests);

        // Warmup closure tests.
        GridTestUtils.addTestIfNeeded(suite, IgniteWarmupClosureSelfTest.class, ignoredTests);

        // Common tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheConcurrentMapSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityMapperSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAffinityCallSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityRoutingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMvccSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMvccPartitionedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMvccManagerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheP2PUndeploySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheConfigurationValidationSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheConfigurationConsistencySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAffinityKeyConfigurationMismatchTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridDataStorageConfigurationConsistencySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStorageConfigurationValidationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheWithDifferentDataRegionConfigurationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheJdbcBlobStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheJdbcBlobStoreMultithreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, JdbcTypesDefaultTransformerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoStoreTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoStoreBinaryMarshallerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinarySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoStoreBinaryMarshallerWithSqlEscapeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinaryWithSqlEscapeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoStoreMultitreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoWriteBehindStoreWithCoalescingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcPojoWriteBehindConnectionLeakTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheBalancingStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheStoreValueBytesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorPersistenceSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerUpdateAfterLoadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerMultinodeCreateCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerStopCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerImplSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerTimeoutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerClientReconnectAfterClusterRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerCommunicationSpiExceptionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheEntryMemorySizeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheClearAllSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheObjectToStringSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLoadOnlyStoreAdapterSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheGetStoreErrorSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, StoreResourceInjectionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheFutureExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAsyncOperationsLimitSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheManyAsyncOperationsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTtlManagerSelfTest.class, ignoredTests);

        return suite;
    }
}

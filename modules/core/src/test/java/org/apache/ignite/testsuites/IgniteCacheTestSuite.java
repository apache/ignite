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
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.cache.IgniteCacheEntryProcessorSequentialCallTest;
import org.apache.ignite.cache.IgniteWarmupClosureSelfTest;
import org.apache.ignite.cache.store.CacheStoreReadFromBackupTest;
import org.apache.ignite.cache.store.CacheStoreWriteErrorTest;
import org.apache.ignite.cache.store.CacheTransactionalStoreReadFromBackupTest;
import org.apache.ignite.cache.store.GridCacheBalancingStoreSelfTest;
import org.apache.ignite.cache.store.GridCacheLoadOnlyStoreAdapterSelfTest;
import org.apache.ignite.cache.store.GridStoreLoadCacheTest;
import org.apache.ignite.cache.store.StoreResourceInjectionSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinarySelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinaryWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreMultitreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreMultithreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreSelfTest;
import org.apache.ignite.cache.store.jdbc.JdbcTypesDefaultTransformerTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesMultipleConnectionsTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceMultipleConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalancePairedConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationSslBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessagesTest;
import org.apache.ignite.internal.managers.communication.IgniteVariousConnectionNumberTest;
import org.apache.ignite.internal.processors.cache.BinaryMetadataRegistrationInsideEntryProcessorTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityCallSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteQueueTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteSanitySelfTest;
import org.apache.ignite.internal.processors.cache.CacheFutureExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.CacheNamesSelfTest;
import org.apache.ignite.internal.processors.cache.CacheNamesWithSpecialCharactersTest;
import org.apache.ignite.internal.processors.cache.CachePutEventListenerErrorSelfTest;
import org.apache.ignite.internal.processors.cache.CacheTxFastFinishTest;
import org.apache.ignite.internal.processors.cache.DataStorageConfigurationValidationTest;
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
import org.apache.ignite.internal.processors.cache.GridCachePartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStopSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStorePutxSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStoreValueBytesSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSwapPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTxPartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridDataStorageConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerEagerTtlDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheManyAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNearLockValueSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTransactionalStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteClientAffinityAssignmentSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteGetNonPlainKeyReadThroughSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteIncompleteCacheObjectSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllLargeBatchSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllUpdateNonPreloadedPartitionSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteTxConfigCacheSelfTest;
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
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecovery10ConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryPairedConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheConnectionRecovery10ConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheConnectionRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageRecoveryIdleConnectionTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageWriteTimeoutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSystemTransactionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCrossCacheTxStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheGlobalLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidatorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheGetStoreErrorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorExternalizableFailedTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorNonSerializableTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorPersistenceSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerClientReconnectAfterClusterRestartTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImplSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultinodeCreateCacheTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerTimeoutTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerUpdateAfterLoadTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite.
 */
@RunWith(AllTests.class)
public class IgniteCacheTestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerAtomicReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerAtomicLocalTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerTxTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerTxReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerTxLocalTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerEagerTtlDisabledTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteClientAffinityAssignmentSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicWithStoreInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalWithStoreInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheEntryProcessorNonSerializableTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheEntryProcessorExternalizableFailedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryProcessorCallTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearEnabledInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalInvokeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCrossCacheTxStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryProcessorSequentialCallTest.class));

        // TODO GG-11148: include test when implemented.
        // Test fails due to incorrect handling of CacheConfiguration#getCopyOnRead() and
        // CacheObjectContext#storeValue() properties. Heap storage should be redesigned in this ticket.
        //GridTestUtils.addTestIfNeeded(suite, CacheEntryProcessorCopySelfTest.class, ignoredTests);

        suite.addTest(new JUnit4TestAdapter(IgnitePutAllLargeBatchSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePutAllUpdateNonPreloadedPartitionSelfTest.class));

        // User's class loader tests.
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionedExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReplicatedExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContinuousExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheIsolatedExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheP2PDisableExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePrivateExecutionContextTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSharedExecutionContextTest.class, ignoredTests);

        // Warmup closure tests.
        suite.addTest(new JUnit4TestAdapter(IgniteWarmupClosureSelfTest.class));

        // Swap tests.
        suite.addTest(new JUnit4TestAdapter(GridCacheSwapPreloadSelfTest.class));

        // Common tests.
        suite.addTest(new JUnit4TestAdapter(CacheNamesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheNamesWithSpecialCharactersTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheConcurrentMapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAffinityMapperSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheAffinityCallSelfTest.class));
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityRoutingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMvccSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridCacheMvccPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMvccManagerSelfTest.class));
        // TODO GG-11141.
        // suite.addTest(new JUnit4TestAdapter(GridCacheP2PUndeploySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheConfigurationValidationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheConfigurationConsistencySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDataStorageConfigurationConsistencySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStorageConfigurationValidationTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheJdbcBlobStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheJdbcBlobStoreMultithreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcTypesDefaultTransformerTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreBinaryMarshallerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinarySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreBinaryMarshallerWithSqlEscapeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinaryWithSqlEscapeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreMultitreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBalancingStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAffinityApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheStoreValueBytesSelfTest.class));
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorPersistenceSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamerUpdateAfterLoadTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(DataStreamerMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStreamerMultinodeCreateCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStreamerImplSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStreamerTimeoutTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStreamerClientReconnectAfterClusterRestartTest.class));
        GridTestUtils.addTestIfNeeded(suite, GridCacheEntryMemorySizeSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridCacheClearAllSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheObjectToStringSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLoadOnlyStoreAdapterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheGetStoreErrorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(StoreResourceInjectionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheFutureExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAsyncOperationsLimitSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheManyAsyncOperationsTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheTtlManagerSelfTest.class));
        // TODO: ignite-4534
//        suite.addTest(new JUnit4TestAdapter(GridCacheTtlManagerEvictionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLifecycleAwareSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicStopBusySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTransactionalStopBusySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheAtomicNearUpdateTopologyChangeTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheTxNearUpdateTopologyChangeTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheStorePutxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheOffHeapMultiThreadedUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheColocatedTxStoreExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedTxStoreExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalTxStoreExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearTxStoreExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMissingCommitVersionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheEntrySetIterationPreloadingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMixedPartitionExchangeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicMessageRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicMessageRecoveryPairedConnectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicMessageRecovery10ConnectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxMessageRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheMessageWriteTimeoutTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheMessageRecoveryIdleConnectionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheConnectionRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheConnectionRecovery10ConnectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheGlobalLoadTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedLocalStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedLocalStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheTxPartitionedLocalStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheSystemTransactionsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheDeferredDeleteSanitySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheDeferredDeleteQueueTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionsStateValidatorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionsStateValidationTest.class));

        suite.addTest(IgniteCacheTcpClientDiscoveryTestSuite.suite());

        // Heuristic exception handling.
        suite.addTest(new JUnit4TestAdapter(GridCacheColocatedTxExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedTxExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalTxExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearTxExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheStopSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheNearLockValueSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CachePutEventListenerErrorSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteTxConfigCacheSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheTxFastFinishTest.class));

        //suite.addTest(new JUnit4TestAdapter(GridIoManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteVariousConnectionNumberTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCommunicationBalanceTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCommunicationBalancePairedConnectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCommunicationBalanceMultipleConnectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCommunicationSslBalanceTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteIoTestMessagesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDiagnosticMessagesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDiagnosticMessagesMultipleConnectionsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteIncompleteCacheObjectSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridStoreLoadCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreReadFromBackupTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreWriteErrorTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheTransactionalStoreReadFromBackupTest.class));

        //suite.addTest(new JUnit4TestAdapter(CacheAtomicSingleMessageCountSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheAtomicUsersAffinityMapperSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheClearLocallySelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheConcurrentGetCacheOnClientTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheFullTextQueryMultithreadedSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheKeyCheckNearEnabledSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheKeyCheckSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheLeakTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheMultiUpdateLockSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheMvccFlagsTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedUsersAffinityMapperSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheReturnValueTransferSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheSlowTxWarnTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheTtlManagerLoadTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheTxUsersAffinityMapperSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteInternalCacheRemoveTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCacheBinaryEntryProcessorSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCacheObjectPutSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCacheSerializationSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCacheStartStopLoadTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCachingProviderSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteOnePhaseCommitNearSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteStaticCacheStartSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(InterceptorWithKeepBinaryCacheFullApiTest.class));

        suite.addTest(new JUnit4TestAdapter(BinaryMetadataRegistrationInsideEntryProcessorTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteGetNonPlainKeyReadThroughSelfTest.class));

        return suite;
    }
}

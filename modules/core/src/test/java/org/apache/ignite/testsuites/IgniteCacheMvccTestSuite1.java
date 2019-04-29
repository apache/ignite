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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.IgniteCacheEntryProcessorSequentialCallTest;
import org.apache.ignite.cache.IgniteWarmupClosureSelfTest;
import org.apache.ignite.cache.store.CacheStoreReadFromBackupTest;
import org.apache.ignite.cache.store.GridCacheBalancingStoreSelfTest;
import org.apache.ignite.cache.store.GridStoreLoadCacheTest;
import org.apache.ignite.cache.store.StoreResourceInjectionSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreSelfTest;
import org.apache.ignite.cache.store.jdbc.JdbcTypesDefaultTransformerTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceMultipleConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalancePairedConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationSslBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessagesTest;
import org.apache.ignite.internal.managers.communication.IgniteVariousConnectionNumberTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityCallSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteQueueTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteSanitySelfTest;
import org.apache.ignite.internal.processors.cache.CacheMvccTxFastFinishTest;
import org.apache.ignite.internal.processors.cache.CacheTxFastFinishTest;
import org.apache.ignite.internal.processors.cache.DataStorageConfigurationValidationTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityApiSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAsyncOperationsLimitSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLifecycleAwareSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMissingCommitVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStopSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTcpClientDiscoveryMultiThreadedTest;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheManyAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMvccTxInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMvccTxNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteClientAffinityAssignmentSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteIncompleteCacheObjectSelfTest;
import org.apache.ignite.internal.processors.cache.binary.CacheKeepBinaryWithInterceptorTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAffinityRoutingBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinariesNearPartitionedByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinariesPartitionedOnlyByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheAtomicExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheContinuousExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheIsolatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheP2PDisableExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCachePrivateExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheReplicatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheSharedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicNearUpdateTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecovery10ConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryPairedConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheConnectionRecovery10ConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheConnectionRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageRecoveryIdleConnectionTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageWriteTimeoutTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidatorSelfTest;
import org.apache.ignite.internal.processors.cache.expiry.IgniteCacheAtomicLocalExpiryPolicyTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorExternalizableFailedTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorNonSerializableTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite1 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Set<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(CacheKeepBinaryWithInterceptorTest.class);
        ignoredTests.add(CacheEntryProcessorNonSerializableTest.class);
        ignoredTests.add(CacheEntryProcessorExternalizableFailedTest.class);
        ignoredTests.add(IgniteCacheEntryProcessorSequentialCallTest.class);
        ignoredTests.add(IgniteCacheEntryProcessorCallTest.class);
        ignoredTests.add(GridCacheConfigurationConsistencySelfTest.class);
        ignoredTests.add(IgniteCacheMessageRecoveryIdleConnectionTest.class);
        ignoredTests.add(IgniteCacheConnectionRecoveryTest.class);
        ignoredTests.add(IgniteCacheConnectionRecovery10ConnectionsTest.class);
        ignoredTests.add(CacheDeferredDeleteSanitySelfTest.class);
        ignoredTests.add(CacheDeferredDeleteQueueTest.class);
        ignoredTests.add(GridCacheStopSelfTest.class);
        ignoredTests.add(GridCacheBinariesNearPartitionedByteArrayValuesSelfTest.class);
        ignoredTests.add(GridCacheBinariesPartitionedOnlyByteArrayValuesSelfTest.class);

        // Atomic caches.
        ignoredTests.add(IgniteCacheEntryListenerAtomicTest.class);
        ignoredTests.add(IgniteCacheEntryListenerAtomicReplicatedTest.class);
        ignoredTests.add(IgniteCacheEntryListenerAtomicLocalTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        ignoredTests.add(IgniteCacheAtomicInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicWithStoreInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalWithStoreInvokeTest.class);
        ignoredTests.add(GridCachePartitionedLocalStoreSelfTest.class);
        ignoredTests.add(GridCacheReplicatedLocalStoreSelfTest.class);
        ignoredTests.add(CacheStoreReadFromBackupTest.class);

        ignoredTests.add(IgniteCacheAtomicExecutionContextTest.class);
        ignoredTests.add(IgniteCacheReplicatedExecutionContextTest.class);
        ignoredTests.add(IgniteCacheContinuousExecutionContextTest.class);
        ignoredTests.add(IgniteCacheIsolatedExecutionContextTest.class);
        ignoredTests.add(IgniteCacheP2PDisableExecutionContextTest.class);
        ignoredTests.add(IgniteCachePrivateExecutionContextTest.class);
        ignoredTests.add(IgniteCacheSharedExecutionContextTest.class);

        ignoredTests.add(IgniteCacheAtomicStopBusySelfTest.class);
        ignoredTests.add(GridCacheAtomicNearCacheSelfTest.class);
        ignoredTests.add(CacheAtomicNearUpdateTopologyChangeTest.class);
        ignoredTests.add(GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicMessageRecoveryTest.class);
        ignoredTests.add(IgniteCacheAtomicMessageRecoveryPairedConnectionsTest.class);
        ignoredTests.add(IgniteCacheAtomicMessageRecovery10ConnectionsTest.class);

        ignoredTests.add(GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseNearPartitionedAtomic.class);
        ignoredTests.add(GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseNearReplicatedAtomic.class);
        ignoredTests.add(GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseClientPartitionedAtomic.class);
        ignoredTests.add(GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseClientReplicatedAtomic.class);

        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest.class);

        // Irrelevant tests.
        ignoredTests.add(GridCacheMvccSelfTest.class); // This is about MvccCandidate, but not TxSnapshot.
        ignoredTests.add(GridCacheMvccPartitionedSelfTest.class); // This is about MvccCandidate, but not TxSnapshot.
        ignoredTests.add(GridCacheMvccManagerSelfTest.class); // This is about MvccCandidate, but not TxSnapshot.
        ignoredTests.add(GridCacheMissingCommitVersionSelfTest.class); // Mvcc tx states resides in TxLog.

        // Other non-Tx test.
        ignoredTests.add(GridCacheAffinityRoutingSelfTest.class);
        ignoredTests.add(GridCacheAffinityRoutingBinarySelfTest.class);
        ignoredTests.add(IgniteClientAffinityAssignmentSelfTest.class);
        ignoredTests.add(GridCacheConcurrentMapSelfTest.class);
        ignoredTests.add(CacheAffinityCallSelfTest.class);
        ignoredTests.add(GridCacheAffinityMapperSelfTest.class);
        ignoredTests.add(GridCacheAffinityApiSelfTest.class);

        ignoredTests.add(GridCacheConfigurationValidationSelfTest.class);

        ignoredTests.add(GridDataStorageConfigurationConsistencySelfTest.class);
        ignoredTests.add(DataStorageConfigurationValidationTest.class);
        ignoredTests.add(JdbcTypesDefaultTransformerTest.class);
        ignoredTests.add(GridCacheJdbcBlobStoreSelfTest.class);
        ignoredTests.add(CacheJdbcPojoStoreTest.class);
        ignoredTests.add(GridCacheBalancingStoreSelfTest.class);
        ignoredTests.add(GridStoreLoadCacheTest.class);

        ignoredTests.add(IgniteWarmupClosureSelfTest.class);
        ignoredTests.add(StoreResourceInjectionSelfTest.class);
        ignoredTests.add(GridCacheAsyncOperationsLimitSelfTest.class);
        ignoredTests.add(IgniteCacheManyAsyncOperationsTest.class);
        ignoredTests.add(GridCacheLifecycleAwareSelfTest.class);
        ignoredTests.add(IgniteCacheMessageWriteTimeoutTest.class);
        ignoredTests.add(GridCachePartitionsStateValidatorSelfTest.class);
        ignoredTests.add(IgniteVariousConnectionNumberTest.class);
        ignoredTests.add(IgniteIncompleteCacheObjectSelfTest.class);

        ignoredTests.add(IgniteCommunicationBalanceTest.class);
        ignoredTests.add(IgniteCommunicationBalancePairedConnectionsTest.class);
        ignoredTests.add(IgniteCommunicationBalanceMultipleConnectionsTest.class);
        ignoredTests.add(IgniteCommunicationSslBalanceTest.class);
        ignoredTests.add(IgniteIoTestMessagesTest.class);

        ignoredTests.add(GridCacheTcpClientDiscoveryMultiThreadedTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        ignoredTests.add(GridCacheOffHeapMultiThreadedUpdateSelfTest.class); // See GridCacheMvccMultiThreadedUpdateSelfTest.
        ignoredTests.add(CacheTxFastFinishTest.class); // See CacheMvccTxFastFinishTest.
        ignoredTests.add(IgniteCacheTxInvokeTest.class); // See IgniteCacheMvccTxInvokeTest.
        ignoredTests.add(IgniteCacheTxNearEnabledInvokeTest.class); // See IgniteCacheMvccTxNearEnabledInvokeTest.

        List<Class<?>> suite = new ArrayList<>(IgniteBinaryCacheTestSuite.suite(ignoredTests));

        // Add Mvcc clones.
        suite.add(GridCacheMvccMultiThreadedUpdateSelfTest.class);
        suite.add(CacheMvccTxFastFinishTest.class);
        suite.add(IgniteCacheMvccTxInvokeTest.class);
        suite.add(IgniteCacheMvccTxNearEnabledInvokeTest.class);

        return suite;
    }
}

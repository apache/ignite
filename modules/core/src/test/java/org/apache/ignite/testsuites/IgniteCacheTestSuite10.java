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
import org.apache.ignite.cache.store.CacheStoreReadFromBackupTest;
import org.apache.ignite.cache.store.CacheStoreWriteErrorTest;
import org.apache.ignite.cache.store.CacheTransactionalStoreReadFromBackupTest;
import org.apache.ignite.cache.store.GridStoreLoadCacheTest;
import org.apache.ignite.cache.store.jdbc.dialect.OracleDialectTest;
import org.apache.ignite.internal.IgniteInternalCacheRemoveTest;
import org.apache.ignite.internal.managers.communication.GridIoManagerSelfTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceMultipleConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalancePairedConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationSslBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteIoCommunicationMessageSerializationTest;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessagesTest;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImplTest;
import org.apache.ignite.internal.managers.communication.IgniteVariousConnectionNumberTest;
import org.apache.ignite.internal.managers.communication.MessageDirectTypeIdConflictTest;
import org.apache.ignite.internal.processors.cache.BinaryMetadataRegistrationInsideEntryProcessorTest;
import org.apache.ignite.internal.processors.cache.CacheAsyncContinuationExecutorTest;
import org.apache.ignite.internal.processors.cache.CacheAsyncContinuationSynchronousExecutorTest;
import org.apache.ignite.internal.processors.cache.CacheAtomicSingleMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteQueueTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteSanitySelfTest;
import org.apache.ignite.internal.processors.cache.CachePutEventListenerErrorSelfTest;
import org.apache.ignite.internal.processors.cache.CacheTxFastFinishTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicUsersAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheClearLocallySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheColocatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentGetCacheOnClientTest;
import org.apache.ignite.internal.processors.cache.GridCacheLeakTest;
import org.apache.ignite.internal.processors.cache.GridCacheLifecycleAwareSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMissingCommitVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMixedPartitionExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFlagsTest;
import org.apache.ignite.internal.processors.cache.GridCacheNearTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedUsersAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReturnValueTransferSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSlowTxWarnTest;
import org.apache.ignite.internal.processors.cache.GridCacheStopSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerLoadTest;
import org.apache.ignite.internal.processors.cache.GridCacheTxPartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTxUsersAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryEntryProcessorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNearLockValueSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheObjectPutSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSerializationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTransactionalStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachingProviderSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteGetNonPlainKeyReadThroughSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteIncompleteCacheObjectSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteOnePhaseCommitNearSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteStaticCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteTxConfigCacheSelfTest;
import org.apache.ignite.internal.processors.cache.InterceptorWithKeepBinaryCacheFullApiTest;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheGlobalLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidatorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsUpdateCountersAndSizeTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheConcurrentPutGetRemoveTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedStorePutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxExceptionSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite10 {
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

        GridTestUtils.addTestIfNeeded(suite, GridCacheTtlManagerEvictionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLifecycleAwareSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStopBusySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTransactionalStopBusySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearCacheSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAtomicNearUpdateTopologyChangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTxNearUpdateTopologyChangeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedStorePutSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheOffHeapMultiThreadedUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedTxStoreExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxStoreExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearTxStoreExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMissingCommitVersionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheEntrySetIterationPreloadingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMixedPartitionExchangeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicMessageRecoveryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicMessageRecoveryPairedConnectionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicMessageRecovery10ConnectionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxMessageRecoveryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheMessageWriteTimeoutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheMessageRecoveryIdleConnectionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConnectionRecoveryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConnectionRecovery10ConnectionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheGlobalLoadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedLocalStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedLocalStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTxPartitionedLocalStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSystemTransactionsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDeferredDeleteSanitySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDeferredDeleteQueueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionsStateValidatorSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionsStateValidationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionsUpdateCountersAndSizeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConcurrentPutGetRemoveTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OracleDialectTest.class, ignoredTests);

        suite.addAll(IgniteCacheTcpClientDiscoveryTestSuite.suite(ignoredTests));

        // Heuristic exception handling.
        GridTestUtils.addTestIfNeeded(suite, GridCacheColocatedTxExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearTxExceptionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheStopSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearLockValueSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CachePutEventListenerErrorSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTxConfigCacheSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheTxFastFinishTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteVariousConnectionNumberTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCommunicationBalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCommunicationBalancePairedConnectionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCommunicationBalanceMultipleConnectionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCommunicationSslBalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteIoTestMessagesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteIoTestMessagesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteMessageFactoryImplTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MessageDirectTypeIdConflictTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteIoCommunicationMessageSerializationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteIncompleteCacheObjectSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridStoreLoadCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreReadFromBackupTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreWriteErrorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTransactionalStoreReadFromBackupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridIoManagerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAtomicSingleMessageCountSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheClearLocallySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheConcurrentGetCacheOnClientTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheLeakTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMvccFlagsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReturnValueTransferSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheSlowTxWarnTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTtlManagerLoadTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicUsersAffinityMapperSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTxUsersAffinityMapperSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedUsersAffinityMapperSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteInternalCacheRemoveTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheBinaryEntryProcessorSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheObjectPutSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSerializationSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachingProviderSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOnePhaseCommitNearSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteStaticCacheStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, InterceptorWithKeepBinaryCacheFullApiTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, BinaryMetadataRegistrationInsideEntryProcessorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAsyncContinuationExecutorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAsyncContinuationSynchronousExecutorTest.class, ignoredTests);

        suite.add(IgniteGetNonPlainKeyReadThroughSelfTest.class);

        return suite;
    }
}

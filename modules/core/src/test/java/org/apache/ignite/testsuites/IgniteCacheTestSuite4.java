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
import org.apache.ignite.cache.store.CacheStoreListenerRWThroughDisabledAtomicCacheTest;
import org.apache.ignite.cache.store.CacheStoreListenerRWThroughDisabledTransactionalCacheTest;
import org.apache.ignite.cache.store.CacheStoreSessionListenerLifecycleSelfTest;
import org.apache.ignite.cache.store.CacheStoreSessionListenerWriteBehindEnabledTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListenerSelfTest;
import org.apache.ignite.internal.processors.GridCacheTxLoadFromStoreOnLockSelfTest;
import org.apache.ignite.internal.processors.cache.CacheClientStoreSelfTest;
import org.apache.ignite.internal.processors.cache.CacheConnectionLeakStoreTxTest;
import org.apache.ignite.internal.processors.cache.CacheEventWithTxLabelTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticReadCommittedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticRepeatableReadSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticSerializableSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticReadCommittedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticRepeatableReadSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticSerializableSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapMapEntrySelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughAtomicRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughLocalAtomicRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughLocalRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughReplicatedAtomicRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughReplicatedRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheRemoveAllSelfTest;
import org.apache.ignite.internal.processors.cache.CacheStopAndDestroySelfTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeDynamicStartAtomicTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeDynamicStartTxTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeStaticStartAtomicTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeStaticStartTxTest;
import org.apache.ignite.internal.processors.cache.CacheTxNotAllowReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.CrossCacheLockTest;
import org.apache.ignite.internal.processors.cache.GridCacheMarshallingNodeJoinSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheProcessorActiveTxTest;
import org.apache.ignite.internal.processors.cache.GridCacheStoreManagerDeserializationTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionMultinodeTest;
import org.apache.ignite.internal.processors.cache.GridLocalCacheStoreManagerDeserializationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicCopyOnReadDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationDefaultTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheContainsKeyAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDynamicStopSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughSingleNodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheReadThroughStoreCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStartTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxCopyOnReadDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPreloadNoWriteTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteClientCacheInitializationFailTest;
import org.apache.ignite.internal.processors.cache.IgniteDiscoDataHandlingInNewClusterTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheFilterTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheMultinodeTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartFailTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartNoExchangeTimeoutTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartStopConcurrentTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheWithConfigStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicClientCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteExchangeFutureHistoryTest;
import org.apache.ignite.internal.processors.cache.IgniteInternalCacheTypesTest;
import org.apache.ignite.internal.processors.cache.IgniteStartCacheInTransactionAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteStartCacheInTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteSystemCacheOnClientTest;
import org.apache.ignite.internal.processors.cache.MarshallerCacheJobRunNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAffinityEarlyTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDiscoveryDataConcurrentJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheGetFutureHangsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheGroupsPreloadTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheNoValueClassOnServerNodeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheResultIsNotNullOnPartitionLossTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheStartOnJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheFailedUpdateResponseTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSingleGetMessageTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtTxPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheLockFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheMultiTxLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCrossCacheTxSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheNearOnlyTxTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheNearReadCommittedTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridReplicatedTxPreloadTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLoaderWriterTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicStoreSessionTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicStoreSessionWriteBehindTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheJdbcBlobStoreNodeRestartTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLoaderWriterTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLocalLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLocalNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLocalNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLocalNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNearEnabledNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNearEnabledNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNearEnabledNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionWriteBehindCoalescingTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionWriteBehindTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryLocalAtomicSwapDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryLocalTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedTransactionalSelfTest;
import org.apache.ignite.internal.processors.query.ScanQueriesTopologyMappingTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite4 {
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

        // Multi node update.
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateNearEnabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLoadAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLocalLoadAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLoadAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLocalLoadAllTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLoaderWriterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLoaderWriterTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStoreSessionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreSessionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStoreSessionWriteBehindTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreSessionWriteBehindTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreSessionWriteBehindCoalescingTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLocalNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLocalNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetRemoveSkipStoreTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLocalNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLocalNoLoadPreviousValueTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLocalNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLocalNoWriteThroughTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicReplicatedPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLocalPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLocalPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxReplicatedPeekModesTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheInvokeReadThroughSingleNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheInvokeReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReadThroughStoreCallTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionMultinodeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearReadCommittedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicCopyOnReadDisabledTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxCopyOnReadDisabledTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxPreloadNoWriteTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheMultinodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartFailTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartCoordinatorFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheWithConfigStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheDynamicStopSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartStopConcurrentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConfigurationTemplateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConfigurationDefaultTemplateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicClientCacheStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartNoExchangeTimeoutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAffinityEarlyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheCreatePutMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheCreatePutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStartOnJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDiscoDataHandlingInNewClusterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDiscoveryDataConcurrentJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClientCacheInitializationFailTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ScanQueriesTopologyMappingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheFailedUpdateResponseTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheTxLoadFromStoreOnLockSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheMarshallingNodeJoinSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheJdbcBlobStoreNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLocalStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLocalStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledStoreValueTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheLockFailoverSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheMultiTxLockSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteInternalCacheTypesTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteExchangeFutureHistoryTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheNoValueClassOnServerNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteSystemCacheOnClientTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheRemoveAllSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryOptimisticReadCommittedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryOptimisticRepeatableReadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryOptimisticSerializableSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryPessimisticReadCommittedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryPessimisticRepeatableReadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryPessimisticSerializableSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTxNotAllowReadFromBackupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheStopAndDestroySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheOffheapMapEntrySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheJdbcStoreSessionListenerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreSessionListenerLifecycleSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreListenerRWThroughDisabledAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreListenerRWThroughDisabledTransactionalCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreSessionListenerWriteBehindEnabledTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheClientStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeStaticStartAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeStaticStartTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeDynamicStartAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeDynamicStartTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheConnectionLeakStoreTxTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheStoreManagerDeserializationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridLocalCacheStoreManagerDeserializationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteStartCacheInTransactionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteStartCacheInTransactionAtomicSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughReplicatedRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughReplicatedAtomicRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughLocalRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughLocalAtomicRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughAtomicRestartSelfTest.class, ignoredTests);

        // Versioned entry tests
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryLocalAtomicSwapDisabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryLocalTransactionalSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryPartitionedAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryPartitionedTransactionalSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryReplicatedAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryReplicatedTransactionalSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtTxPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearTxPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridReplicatedTxPreloadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGroupsPreloadTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheFilterTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CrossCacheLockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCrossCacheTxSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheGetFutureHangsSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSingleGetMessageTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReadFromBackupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, MarshallerCacheJobRunNodeRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearOnlyTxTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyAtomicTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheResultIsNotNullOnPartitionLossTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheEventWithTxLabelTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheProcessorActiveTxTest.class, ignoredTests);

        return suite;
    }
}

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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.cache.store.CacheStoreListenerRWThroughDisabledAtomicCacheTest;
import org.apache.ignite.cache.store.CacheStoreListenerRWThroughDisabledTransactionalCacheTest;
import org.apache.ignite.cache.store.CacheStoreSessionListenerLifecycleSelfTest;
import org.apache.ignite.cache.store.CacheStoreSessionListenerWriteBehindEnabledTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListenerSelfTest;
import org.apache.ignite.internal.processors.GridCacheTxLoadFromStoreOnLockSelfTest;
import org.apache.ignite.internal.processors.cache.CacheClientStoreSelfTest;
import org.apache.ignite.internal.processors.cache.CacheConnectionLeakStoreTxTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticReadCommittedSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticRepeatableReadSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticSerializableSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticReadCommittedSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticRepeatableReadSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticSerializableSeltTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite.
 */
@RunWith(AllTests.class)
public class IgniteCacheTestSuite4 {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 4");

        // Multi node update.
        suite.addTest(new JUnit4TestAdapter(GridCacheMultinodeUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMultinodeUpdateNearEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMultinodeUpdateAtomicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLoadAllTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalLoadAllTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLoadAllTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalLoadAllTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLoaderWriterTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLoaderWriterTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicStoreSessionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxStoreSessionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicStoreSessionWriteBehindTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxStoreSessionWriteBehindTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxStoreSessionWriteBehindCoalescingTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNoReadThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledNoReadThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalNoReadThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNoReadThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearEnabledNoReadThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalNoReadThroughTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNoLoadPreviousValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalNoLoadPreviousValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNoLoadPreviousValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearEnabledNoLoadPreviousValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalNoLoadPreviousValueTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNoWriteThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledNoWriteThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalNoWriteThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNoWriteThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearEnabledNoWriteThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalNoWriteThroughTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicReplicatedPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxReplicatedPeekModesTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheInvokeReadThroughSingleNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheInvokeReadThroughTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReadThroughStoreCallTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheVersionMultinodeTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheNearReadCommittedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicCopyOnReadDisabledTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxCopyOnReadDisabledTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxPreloadNoWriteTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheStartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheMultinodeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheStartFailTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheStartCoordinatorFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheWithConfigStartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDynamicStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheStartStopConcurrentTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheConfigurationTemplateTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheConfigurationDefaultTemplateTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicClientCacheStartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheStartNoExchangeTimeoutTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheAffinityEarlyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheCreatePutMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheCreatePutTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStartOnJoinTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheStartTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheDiscoveryDataConcurrentJoinTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientCacheInitializationFailTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheFailedUpdateResponseTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheTxLoadFromStoreOnLockSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheMarshallingNodeJoinSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheJdbcBlobStoreNodeRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalStoreValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicStoreValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledStoreValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalStoreValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxStoreValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearEnabledStoreValueTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheLockFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheMultiTxLockSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteInternalCacheTypesTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteExchangeFutureHistoryTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheNoValueClassOnServerNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSystemCacheOnClientTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheRemoveAllSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetEntryOptimisticReadCommittedSeltTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetEntryOptimisticRepeatableReadSeltTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetEntryOptimisticSerializableSeltTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetEntryPessimisticReadCommittedSeltTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetEntryPessimisticRepeatableReadSeltTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetEntryPessimisticSerializableSeltTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheTxNotAllowReadFromBackupTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheStopAndDestroySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheOffheapMapEntrySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheJdbcStoreSessionListenerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreSessionListenerLifecycleSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreListenerRWThroughDisabledAtomicCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreListenerRWThroughDisabledTransactionalCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreSessionListenerWriteBehindEnabledTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheClientStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreUsageMultinodeStaticStartAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreUsageMultinodeStaticStartTxTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreUsageMultinodeDynamicStartAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheStoreUsageMultinodeDynamicStartTxTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheConnectionLeakStoreTxTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheStoreManagerDeserializationTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLocalCacheStoreManagerDeserializationTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteStartCacheInTransactionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteStartCacheInTransactionAtomicSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheReadThroughRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReadThroughReplicatedRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReadThroughReplicatedAtomicRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReadThroughLocalRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReadThroughLocalAtomicRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReadThroughAtomicRestartSelfTest.class));

        // Versioned entry tests
        suite.addTest(new JUnit4TestAdapter(CacheVersionedEntryLocalAtomicSwapDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheVersionedEntryLocalTransactionalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheVersionedEntryPartitionedAtomicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheVersionedEntryPartitionedTransactionalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheVersionedEntryReplicatedAtomicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheVersionedEntryReplicatedTransactionalSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheDhtTxPreloadSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearTxPreloadSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridReplicatedTxPreloadTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGroupsPreloadTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheFilterTest.class));

        suite.addTest(new JUnit4TestAdapter(CrossCacheLockTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCrossCacheTxSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheGetFutureHangsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheSingleGetMessageTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReadFromBackupTest.class));

        suite.addTest(new JUnit4TestAdapter(MarshallerCacheJobRunNodeRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheNearOnlyTxTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheContainsKeyAtomicTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheResultIsNotNullOnPartitionLossTest.class));

        return suite;
    }
}

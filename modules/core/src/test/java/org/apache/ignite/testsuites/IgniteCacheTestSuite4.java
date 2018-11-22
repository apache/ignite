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
import org.apache.ignite.internal.processors.cache.CashEventWithTxLabelTest;
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

import static org.apache.ignite.testframework.GridTestUtils.addTestIfNeeded;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite4 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Ignored tests.
     * @return IgniteCache test suite.
     */
    public static TestSuite suite(HashSet<Class> ignoredTests) {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 4");

        // Multi node update.
        addTestIfNeeded(suite, GridCacheMultinodeUpdateSelfTest.class, ignoredTests);//
        addTestIfNeeded(suite, GridCacheMultinodeUpdateNearEnabledSelfTest.class, ignoredTests);//
        addTestIfNeeded(suite, GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class, ignoredTests);//
        addTestIfNeeded(suite, GridCacheMultinodeUpdateAtomicSelfTest.class, ignoredTests);//
        addTestIfNeeded(suite, GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicLoadAllTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicLocalLoadAllTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLoadAllTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLocalLoadAllTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicLoaderWriterTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLoaderWriterTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicStoreSessionTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxStoreSessionTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicStoreSessionWriteBehindTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxStoreSessionWriteBehindTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxStoreSessionWriteBehindCoalescingTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicNoReadThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoReadThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicLocalNoReadThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNoReadThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoReadThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLocalNoReadThroughTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicNoLoadPreviousValueTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicLocalNoLoadPreviousValueTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNoLoadPreviousValueTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoLoadPreviousValueTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLocalNoLoadPreviousValueTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicNoWriteThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoWriteThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicLocalNoWriteThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNoWriteThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoWriteThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLocalNoWriteThroughTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheAtomicPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicNearPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicReplicatedPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheAtomicLocalPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxNearPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxLocalPeekModesTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheTxReplicatedPeekModesTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheInvokeReadThroughSingleNodeTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheInvokeReadThroughTest.class, ignoredTests);//
        addTestIfNeeded(suite, IgniteCacheReadThroughStoreCallTest.class, ignoredTests);//
        addTestIfNeeded(suite, GridCacheVersionMultinodeTest.class, ignoredTests);//

        addTestIfNeeded(suite, IgniteCacheNearReadCommittedTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheAtomicCopyOnReadDisabledTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheTxCopyOnReadDisabledTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheTxPreloadNoWriteTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteDynamicCacheStartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicCacheMultinodeTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicCacheStartFailTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicCacheStartCoordinatorFailoverTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicCacheWithConfigStartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheDynamicStopSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicCacheStartStopConcurrentTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheConfigurationTemplateTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheConfigurationDefaultTemplateTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicClientCacheStartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteDynamicCacheStartNoExchangeTimeoutTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheAffinityEarlyTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheCreatePutMultiNodeSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheCreatePutTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStartOnJoinTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheStartTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheDiscoveryDataConcurrentJoinTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteClientCacheInitializationFailTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheFailedUpdateResponseTest.class, ignoredTests);

        addTestIfNeeded(suite, GridCacheTxLoadFromStoreOnLockSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, GridCacheMarshallingNodeJoinSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheJdbcBlobStoreNodeRestartTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheAtomicLocalStoreValueTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheAtomicStoreValueTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledStoreValueTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheTxLocalStoreValueTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheTxStoreValueTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheTxNearEnabledStoreValueTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheLockFailoverSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheMultiTxLockSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteInternalCacheTypesTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteExchangeFutureHistoryTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheNoValueClassOnServerNodeTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteSystemCacheOnClientTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheRemoveAllSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGetEntryOptimisticReadCommittedSeltTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGetEntryOptimisticRepeatableReadSeltTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGetEntryOptimisticSerializableSeltTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGetEntryPessimisticReadCommittedSeltTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGetEntryPessimisticRepeatableReadSeltTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGetEntryPessimisticSerializableSeltTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheTxNotAllowReadFromBackupTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheStopAndDestroySelfTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheOffheapMapEntrySelfTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheJdbcStoreSessionListenerSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreSessionListenerLifecycleSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreListenerRWThroughDisabledAtomicCacheTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreListenerRWThroughDisabledTransactionalCacheTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreSessionListenerWriteBehindEnabledTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheClientStoreSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreUsageMultinodeStaticStartAtomicTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreUsageMultinodeStaticStartTxTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreUsageMultinodeDynamicStartAtomicTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheStoreUsageMultinodeDynamicStartTxTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheConnectionLeakStoreTxTest.class, ignoredTests);

        addTestIfNeeded(suite, GridCacheStoreManagerDeserializationTest.class, ignoredTests);
        addTestIfNeeded(suite, GridLocalCacheStoreManagerDeserializationTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteStartCacheInTransactionSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteStartCacheInTransactionAtomicSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheReadThroughRestartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheReadThroughReplicatedRestartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheReadThroughReplicatedAtomicRestartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheReadThroughLocalRestartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheReadThroughLocalAtomicRestartSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheReadThroughAtomicRestartSelfTest.class, ignoredTests);

        // Versioned entry tests
        addTestIfNeeded(suite, CacheVersionedEntryLocalAtomicSwapDisabledSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheVersionedEntryLocalTransactionalSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheVersionedEntryPartitionedAtomicSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheVersionedEntryPartitionedTransactionalSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheVersionedEntryReplicatedAtomicSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheVersionedEntryReplicatedTransactionalSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, GridCacheDhtTxPreloadSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, GridCacheNearTxPreloadSelfTest.class, ignoredTests);
        addTestIfNeeded(suite, GridReplicatedTxPreloadTest.class, ignoredTests);
        addTestIfNeeded(suite, CacheGroupsPreloadTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteDynamicCacheFilterTest.class, ignoredTests);

        addTestIfNeeded(suite, CrossCacheLockTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCrossCacheTxSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheGetFutureHangsSelfTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheSingleGetMessageTest.class, ignoredTests);
        addTestIfNeeded(suite, IgniteCacheReadFromBackupTest.class, ignoredTests);

        addTestIfNeeded(suite, MarshallerCacheJobRunNodeRestartTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheNearOnlyTxTest.class, ignoredTests);

        addTestIfNeeded(suite, IgniteCacheContainsKeyAtomicTest.class, ignoredTests);

        addTestIfNeeded(suite, CacheResultIsNotNullOnPartitionLossTest.class, ignoredTests);

        suite.addTestSuite(CashEventWithTxLabelTest.class);

        return suite;
    }
}

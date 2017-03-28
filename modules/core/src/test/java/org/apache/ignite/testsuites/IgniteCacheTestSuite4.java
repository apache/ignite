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

import junit.framework.TestSuite;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListenerSelfTest;
import org.apache.ignite.internal.processors.GridCacheTxLoadFromStoreOnLockSelfTest;
import org.apache.ignite.internal.processors.cache.CacheClientStoreSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticReadCommittedSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticRepeatableReadSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticSerializableSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticReadCommittedSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticRepeatableReadSeltTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticSerializableSeltTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapMapEntrySelfTest;
import org.apache.ignite.internal.processors.cache.CachePutIfAbsentTest;
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
import org.apache.ignite.internal.processors.cache.CacheSwapUnswapGetTest;
import org.apache.ignite.internal.processors.cache.CacheSwapUnswapGetTestSmallQueueSize;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPrimaryWriteOrderNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPrimaryWriteOrderStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationDefaultTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDynamicStopSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughSingleNodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheReadThroughStoreCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxCopyOnReadDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPreloadNoWriteTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheFilterTest;
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
import org.apache.ignite.internal.processors.cache.distributed.CacheGetFutureHangsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheNoValueClassOnServerNodeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSingleGetMessageTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCacheWriteSynchronizationModesMultithreadedTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
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
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionWriteBehindTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryLocalAtomicSwapDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryLocalTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedAtomicOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedTransactionalOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedAtomicOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedTransactionalOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedTransactionalSelfTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite4 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 4");

        // Multi node update.
        suite.addTestSuite(GridCacheMultinodeUpdateSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class);

        suite.addTestSuite(IgniteCacheAtomicLoadAllTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalLoadAllTest.class);
        suite.addTestSuite(IgniteCacheTxLoadAllTest.class);
        suite.addTestSuite(IgniteCacheTxLocalLoadAllTest.class);

        suite.addTestSuite(IgniteCacheAtomicLoaderWriterTest.class);
        suite.addTestSuite(IgniteCacheTxLoaderWriterTest.class);

        suite.addTestSuite(IgniteCacheAtomicStoreSessionTest.class);
        suite.addTestSuite(IgniteCacheTxStoreSessionTest.class);
        suite.addTestSuite(IgniteCacheAtomicStoreSessionWriteBehindTest.class);
        suite.addTestSuite(IgniteCacheTxStoreSessionWriteBehindTest.class);

        suite.addTestSuite(IgniteCacheAtomicNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheTxLocalNoReadThroughTest.class);

        suite.addTestSuite(IgniteCacheAtomicNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheTxNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheTxLocalNoLoadPreviousValueTest.class);

        suite.addTestSuite(IgniteCacheAtomicNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheTxLocalNoWriteThroughTest.class);

        suite.addTestSuite(IgniteCacheAtomicPeekModesTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearPeekModesTest.class);
        suite.addTestSuite(IgniteCacheAtomicReplicatedPeekModesTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxNearPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxLocalPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxReplicatedPeekModesTest.class);

        suite.addTestSuite(IgniteCacheInvokeReadThroughSingleNodeTest.class);
        suite.addTestSuite(IgniteCacheInvokeReadThroughTest.class);
        suite.addTestSuite(IgniteCacheReadThroughStoreCallTest.class);
        suite.addTestSuite(GridCacheVersionMultinodeTest.class);

        suite.addTestSuite(IgniteCacheNearReadCommittedTest.class);
        suite.addTestSuite(IgniteCacheAtomicCopyOnReadDisabledTest.class);
        suite.addTestSuite(IgniteCacheTxCopyOnReadDisabledTest.class);

        suite.addTestSuite(IgniteCacheTxPreloadNoWriteTest.class);

        suite.addTestSuite(IgniteDynamicCacheStartSelfTest.class);
        suite.addTestSuite(IgniteDynamicCacheWithConfigStartSelfTest.class);
        suite.addTestSuite(IgniteCacheDynamicStopSelfTest.class);
        suite.addTestSuite(IgniteDynamicCacheStartStopConcurrentTest.class);
        suite.addTestSuite(IgniteCacheConfigurationTemplateTest.class);
        suite.addTestSuite(IgniteCacheConfigurationDefaultTemplateTest.class);
        suite.addTestSuite(IgniteDynamicClientCacheStartSelfTest.class);
        suite.addTestSuite(IgniteDynamicCacheStartNoExchangeTimeoutTest.class);
        suite.addTestSuite(CacheAffinityEarlyTest.class);
        suite.addTestSuite(IgniteCacheCreatePutMultiNodeSelfTest.class);
        suite.addTestSuite(IgniteCacheCreatePutTest.class);

        suite.addTestSuite(GridCacheTxLoadFromStoreOnLockSelfTest.class);

        suite.addTestSuite(GridCacheMarshallingNodeJoinSelfTest.class);

        suite.addTestSuite(IgniteCacheJdbcBlobStoreNodeRestartTest.class);

        suite.addTestSuite(IgniteCacheAtomicLocalStoreValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicStoreValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledStoreValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderStoreValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderNearEnabledStoreValueTest.class);
        suite.addTestSuite(IgniteCacheTxLocalStoreValueTest.class);
        suite.addTestSuite(IgniteCacheTxStoreValueTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledStoreValueTest.class);

        suite.addTestSuite(IgniteCacheLockFailoverSelfTest.class);
        suite.addTestSuite(IgniteCacheMultiTxLockSelfTest.class);

        suite.addTestSuite(IgniteInternalCacheTypesTest.class);

        suite.addTestSuite(IgniteExchangeFutureHistoryTest.class);

        suite.addTestSuite(CacheNoValueClassOnServerNodeTest.class);
        suite.addTestSuite(IgniteSystemCacheOnClientTest.class);

        suite.addTestSuite(CacheRemoveAllSelfTest.class);
        suite.addTestSuite(CacheGetEntryOptimisticReadCommittedSeltTest.class);
        suite.addTestSuite(CacheGetEntryOptimisticRepeatableReadSeltTest.class);
        suite.addTestSuite(CacheGetEntryOptimisticSerializableSeltTest.class);
        suite.addTestSuite(CacheGetEntryPessimisticReadCommittedSeltTest.class);
        suite.addTestSuite(CacheGetEntryPessimisticRepeatableReadSeltTest.class);
        suite.addTestSuite(CacheGetEntryPessimisticSerializableSeltTest.class);
        suite.addTestSuite(CacheTxNotAllowReadFromBackupTest.class);

        suite.addTestSuite(CacheStopAndDestroySelfTest.class);

        suite.addTestSuite(CacheOffheapMapEntrySelfTest.class);

        suite.addTestSuite(CacheJdbcStoreSessionListenerSelfTest.class);

        suite.addTestSuite(CacheClientStoreSelfTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeStaticStartAtomicTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeStaticStartTxTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeDynamicStartAtomicTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeDynamicStartTxTest.class);

        suite.addTestSuite(GridCacheStoreManagerDeserializationTest.class);
        suite.addTestSuite(GridLocalCacheStoreManagerDeserializationTest.class);

        suite.addTestSuite(IgniteStartCacheInTransactionSelfTest.class);
        suite.addTestSuite(IgniteStartCacheInTransactionAtomicSelfTest.class);

        suite.addTestSuite(CacheReadThroughRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughReplicatedRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughReplicatedAtomicRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughLocalRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughLocalAtomicRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughAtomicRestartSelfTest.class);

        // Versioned entry tests
        suite.addTestSuite(CacheVersionedEntryLocalAtomicSwapDisabledSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryLocalTransactionalSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryPartitionedAtomicSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryPartitionedTransactionalSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryPartitionedAtomicOffHeapSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryPartitionedTransactionalOffHeapSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryReplicatedAtomicSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryReplicatedTransactionalSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryReplicatedAtomicOffHeapSelfTest.class);
        suite.addTestSuite(CacheVersionedEntryReplicatedTransactionalOffHeapSelfTest.class);

        suite.addTestSuite(CacheSwapUnswapGetTest.class);
        suite.addTestSuite(CacheSwapUnswapGetTestSmallQueueSize.class);

        suite.addTestSuite(GridCacheDhtTxPreloadSelfTest.class);
        suite.addTestSuite(GridCacheNearTxPreloadSelfTest.class);
        suite.addTestSuite(GridReplicatedTxPreloadTest.class);

        suite.addTestSuite(IgniteDynamicCacheFilterTest.class);

        suite.addTestSuite(CrossCacheLockTest.class);
        suite.addTestSuite(IgniteCrossCacheTxSelfTest.class);

        suite.addTestSuite(CacheGetFutureHangsSelfTest.class);

        suite.addTestSuite(IgniteCacheSingleGetMessageTest.class);
        suite.addTestSuite(IgniteCacheReadFromBackupTest.class);

        suite.addTestSuite(IgniteCacheGetCustomCollectionsSelfTest.class);
        suite.addTestSuite(IgniteCacheLoadRebalanceEvictionSelfTest.class);
        suite.addTestSuite(IgniteCachePrimarySyncTest.class);
        suite.addTestSuite(IgniteTxCachePrimarySyncTest.class);
        suite.addTestSuite(IgniteTxCacheWriteSynchronizationModesMultithreadedTest.class);
        suite.addTestSuite(CachePutIfAbsentTest.class);

        suite.addTestSuite(MarshallerCacheJobRunNodeRestartTest.class);

        suite.addTestSuite(IgniteCacheNearOnlyTxTest.class);

        return suite;
    }
}
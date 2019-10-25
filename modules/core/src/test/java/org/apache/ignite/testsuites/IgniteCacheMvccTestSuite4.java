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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.store.CacheStoreListenerRWThroughDisabledAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.CacheConnectionLeakStoreTxTest;
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
import org.apache.ignite.internal.processors.cache.CacheReadThroughReplicatedAtomicRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeDynamicStartAtomicTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeStaticStartAtomicTest;
import org.apache.ignite.internal.processors.cache.CacheTxNotAllowReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionMultinodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicCopyOnReadDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationDefaultTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheContainsKeyAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughSingleNodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStartTest;
import org.apache.ignite.internal.processors.cache.IgniteClientCacheInitializationFailTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartNoExchangeTimeoutTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartStopConcurrentTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicClientCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteExchangeFutureHistoryTest;
import org.apache.ignite.internal.processors.cache.IgniteInternalCacheTypesTest;
import org.apache.ignite.internal.processors.cache.IgniteStartCacheInTransactionAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteSystemCacheOnClientTest;
import org.apache.ignite.internal.processors.cache.MarshallerCacheJobRunNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDiscoveryDataConcurrentJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheGetFutureHangsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheGroupsPreloadTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheNoValueClassOnServerNodeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheResultIsNotNullOnPartitionLossTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheFailedUpdateResponseTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSingleGetMessageTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCrossCacheMvccTxSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCrossCacheTxSelfTest;
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
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryLocalAtomicSwapDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedAtomicSelfTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite4 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>(128);

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(GridCacheVersionMultinodeTest.class);
        ignoredTests.add(IgniteCacheCreatePutTest.class);
        ignoredTests.add(IgniteClientCacheInitializationFailTest.class);
        ignoredTests.add(IgniteCacheFailedUpdateResponseTest.class);
        ignoredTests.add(CacheGetEntryPessimisticRepeatableReadSelfTest.class);
        ignoredTests.add(CacheTxNotAllowReadFromBackupTest.class);
        ignoredTests.add(CacheOffheapMapEntrySelfTest.class);
        ignoredTests.add(CacheGroupsPreloadTest.class);
        ignoredTests.add(CacheConnectionLeakStoreTxTest.class);
        ignoredTests.add(IgniteCacheInvokeReadThroughTest.class);
        ignoredTests.add(IgniteCacheInvokeReadThroughSingleNodeTest.class);
        ignoredTests.add(IgniteDynamicCacheStartSelfTest.class);
        ignoredTests.add(IgniteDynamicClientCacheStartSelfTest.class);
        ignoredTests.add(IgniteDynamicCacheStartNoExchangeTimeoutTest.class);
        ignoredTests.add(IgniteCacheSingleGetMessageTest.class);
        ignoredTests.add(IgniteCacheReadFromBackupTest.class);

        // Optimistic tx tests.
        ignoredTests.add(CacheGetEntryOptimisticReadCommittedSelfTest.class);
        ignoredTests.add(CacheGetEntryOptimisticRepeatableReadSelfTest.class);
        ignoredTests.add(CacheGetEntryOptimisticSerializableSelfTest.class);

        // Irrelevant Tx tests.
        ignoredTests.add(CacheGetEntryPessimisticReadCommittedSelfTest.class);
        ignoredTests.add(CacheGetEntryPessimisticSerializableSelfTest.class);

        // Atomic cache tests.
        ignoredTests.add(GridCacheMultinodeUpdateAtomicSelfTest.class);
        ignoredTests.add(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicLoadAllTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalLoadAllTest.class);
        ignoredTests.add(IgniteCacheAtomicLoaderWriterTest.class);
        ignoredTests.add(IgniteCacheAtomicStoreSessionTest.class);
        ignoredTests.add(IgniteCacheAtomicStoreSessionWriteBehindTest.class);
        ignoredTests.add(IgniteCacheAtomicNoReadThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledNoReadThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalNoReadThroughTest.class);
        ignoredTests.add(CacheGetRemoveSkipStoreTest.class);
        ignoredTests.add(IgniteCacheAtomicNoLoadPreviousValueTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalNoLoadPreviousValueTest.class);
        ignoredTests.add(IgniteCacheAtomicNoWriteThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledNoWriteThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalNoWriteThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicNearPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicReplicatedPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicCopyOnReadDisabledTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalStoreValueTest.class);
        ignoredTests.add(IgniteCacheAtomicStoreValueTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledStoreValueTest.class);
        ignoredTests.add(CacheStoreListenerRWThroughDisabledAtomicCacheTest.class);
        ignoredTests.add(CacheStoreUsageMultinodeStaticStartAtomicTest.class);
        ignoredTests.add(CacheStoreUsageMultinodeDynamicStartAtomicTest.class);
        ignoredTests.add(IgniteStartCacheInTransactionAtomicSelfTest.class);
        ignoredTests.add(CacheReadThroughReplicatedAtomicRestartSelfTest.class);
        ignoredTests.add(CacheReadThroughLocalAtomicRestartSelfTest.class);
        ignoredTests.add(CacheReadThroughAtomicRestartSelfTest.class);
        ignoredTests.add(CacheVersionedEntryLocalAtomicSwapDisabledSelfTest.class);
        ignoredTests.add(CacheVersionedEntryPartitionedAtomicSelfTest.class);
        ignoredTests.add(CacheGetFutureHangsSelfTest.class);
        ignoredTests.add(IgniteCacheContainsKeyAtomicTest.class);
        ignoredTests.add(CacheVersionedEntryReplicatedAtomicSelfTest.class);
        ignoredTests.add(CacheResultIsNotNullOnPartitionLossTest.class);

        // Other non-tx tests.
        ignoredTests.add(IgniteDynamicCacheStartStopConcurrentTest.class);
        ignoredTests.add(IgniteCacheConfigurationDefaultTemplateTest.class);
        ignoredTests.add(IgniteCacheStartTest.class);
        ignoredTests.add(CacheDiscoveryDataConcurrentJoinTest.class);
        ignoredTests.add(IgniteCacheJdbcBlobStoreNodeRestartTest.class);
        ignoredTests.add(IgniteInternalCacheTypesTest.class);
        ignoredTests.add(IgniteExchangeFutureHistoryTest.class);
        ignoredTests.add(CacheNoValueClassOnServerNodeTest.class);
        ignoredTests.add(IgniteSystemCacheOnClientTest.class);
        ignoredTests.add(MarshallerCacheJobRunNodeRestartTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        // TODO IGNITE-10175: refactor these tests (use assume) to support both mvcc and non-mvcc modes after moving to JUnit4/5.
        ignoredTests.add(IgniteCrossCacheTxSelfTest.class);

        List<Class<?>> suite = new ArrayList<>(IgniteCacheTestSuite4.suite(ignoredTests));

        // Add Mvcc clones.
        suite.add(IgniteCrossCacheMvccTxSelfTest.class);

        return suite;
    }
}

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
import org.apache.ignite.cache.store.CacheStoreListenerRWThroughDisabledTransactionalCacheTest;
import org.apache.ignite.cache.store.CacheStoreSessionListenerWriteBehindEnabledTest;
import org.apache.ignite.internal.processors.cache.CacheClientStoreSelfTest;
import org.apache.ignite.internal.processors.cache.CacheConnectionLeakStoreTxTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticSerializableSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticReadCommittedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticSerializableSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapMapEntrySelfTest;
import org.apache.ignite.internal.processors.cache.CacheOperationContextTransactionalLockTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughReplicatedAtomicRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughReplicatedRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeDynamicStartTxTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeStaticStartTxTest;
import org.apache.ignite.internal.processors.cache.CacheTxNotAllowReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.CacheVersionedEntryTransactionalLockTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionMultinodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicCopyOnReadDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationDefaultTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationTemplateTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDynamicStopSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughSingleNodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInvokeReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStartTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheFilterTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheMultinodeTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartFailTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheWithConfigStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteExchangeFutureHistoryTest;
import org.apache.ignite.internal.processors.cache.IgniteStartCacheInTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteSystemCacheOnClientTest;
import org.apache.ignite.internal.processors.cache.LockTxEntryOneNodeTest;
import org.apache.ignite.internal.processors.cache.MarshallerCacheJobRunNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDirectoryNameTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDiscoveryDataConcurrentJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheGroupsPreloadTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheFailedUpdateResponseTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtTxPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheMultiTxLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCrossCacheTxSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicStoreSessionWriteBehindTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheJdbcBlobStoreNodeRestartTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxLoaderWriterTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNearEnabledNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionWriteBehindCoalescingTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedAtomicSelfTest;
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
     * @return Test suite.
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

        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateNearEnabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLoadAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLoadAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxLoaderWriterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreSessionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStoreSessionWriteBehindTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreSessionWriteBehindCoalescingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxReplicatedPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheInvokeReadThroughSingleNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheInvokeReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionMultinodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicCopyOnReadDisabledTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheMultinodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartFailTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheWithConfigStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheDynamicStopSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConfigurationTemplateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheConfigurationDefaultTemplateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheCreatePutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheStartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDiscoveryDataConcurrentJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ScanQueriesTopologyMappingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheFailedUpdateResponseTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheJdbcBlobStoreNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheMultiTxLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteExchangeFutureHistoryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteSystemCacheOnClientTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryOptimisticSerializableSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryPessimisticReadCommittedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryPessimisticSerializableSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheTxNotAllowReadFromBackupTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheOffheapMapEntrySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreListenerRWThroughDisabledTransactionalCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreSessionListenerWriteBehindEnabledTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheClientStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeStaticStartTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeDynamicStartTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheConnectionLeakStoreTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteStartCacheInTransactionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughReplicatedRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughReplicatedAtomicRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryPartitionedAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryReplicatedTransactionalSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryTransactionalLockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LockTxEntryOneNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheOperationContextTransactionalLockTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtTxPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearTxPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGroupsPreloadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheFilterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCrossCacheTxSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MarshallerCacheJobRunNodeRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDirectoryNameTest.class, ignoredTests);

        return suite;
    }
}

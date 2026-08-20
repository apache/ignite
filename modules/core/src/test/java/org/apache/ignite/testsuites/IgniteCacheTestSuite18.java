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
import org.apache.ignite.cache.store.CacheStoreSessionListenerLifecycleSelfTest;
import org.apache.ignite.cache.store.CacheStoreWithIgniteTxFailureTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListenerSelfTest;
import org.apache.ignite.internal.processors.GridCacheTxLoadFromStoreOnLockSelfTest;
import org.apache.ignite.internal.processors.cache.CacheEventWithTxLabelTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticReadCommittedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryOptimisticRepeatableReadSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetEntryPessimisticRepeatableReadSelfTest;
import org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest;
import org.apache.ignite.internal.processors.cache.CacheReadThroughAtomicRestartSelfTest;
import org.apache.ignite.internal.processors.cache.CacheRemoveAllSelfTest;
import org.apache.ignite.internal.processors.cache.CacheStopAndDestroySelfTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeDynamicStartAtomicTest;
import org.apache.ignite.internal.processors.cache.CacheStoreUsageMultinodeStaticStartAtomicTest;
import org.apache.ignite.internal.processors.cache.CrossCacheLockTest;
import org.apache.ignite.internal.processors.cache.GridCacheMarshallingNodeJoinSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheProcessorActiveTxTest;
import org.apache.ignite.internal.processors.cache.GridCacheStoreManagerDeserializationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStoreValueTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheContainsKeyAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheReadThroughStoreCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxCopyOnReadDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPreloadNoWriteTest;
import org.apache.ignite.internal.processors.cache.IgniteClientCacheInitializationFailTest;
import org.apache.ignite.internal.processors.cache.IgniteDiscoDataHandlingInNewClusterTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartNoExchangeTimeoutTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartStopConcurrentTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicClientCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteInternalCacheTypesTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAffinityEarlyTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheGetFutureHangsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheNoValueClassOnServerNodeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheResultIsNotNullOnPartitionLossTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheStartOnJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheReadFromBackupTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSingleGetMessageTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheLockFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheNearOnlyTxTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheNearReadCommittedTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridReplicatedTxPreloadTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLoaderWriterTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicStoreSessionTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNearEnabledNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNearEnabledNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheTxStoreSessionWriteBehindTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryPartitionedTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.version.CacheVersionedEntryReplicatedAtomicSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Split off from {@link IgniteCacheTestSuite4} to reduce the single-suite runtime in CI;
 * contains an independent subset of the same test classes.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite18 {
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

        GridTestUtils.addTestIfNeeded(suite, GridCacheMultinodeUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicLoaderWriterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStoreSessionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxStoreSessionWriteBehindTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetRemoveSkipStoreTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearEnabledNoLoadPreviousValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNoWriteThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicReplicatedPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxPeekModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReadThroughStoreCallTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearReadCommittedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxCopyOnReadDisabledTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxPreloadNoWriteTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartCoordinatorFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartStopConcurrentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicClientCacheStartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDynamicCacheStartNoExchangeTimeoutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheAffinityEarlyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheCreatePutMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStartOnJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDiscoDataHandlingInNewClusterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClientCacheInitializationFailTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTxLoadFromStoreOnLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheMarshallingNodeJoinSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNearEnabledStoreValueTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheLockFailoverSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteInternalCacheTypesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheNoValueClassOnServerNodeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheRemoveAllSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryOptimisticReadCommittedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryOptimisticRepeatableReadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetEntryPessimisticRepeatableReadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStopAndDestroySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheJdbcStoreSessionListenerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreSessionListenerLifecycleSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreListenerRWThroughDisabledAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreWithIgniteTxFailureTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeStaticStartAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreUsageMultinodeDynamicStartAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheStoreManagerDeserializationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheReadThroughAtomicRestartSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryPartitionedTransactionalSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheVersionedEntryReplicatedAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridReplicatedTxPreloadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CrossCacheLockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheGetFutureHangsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSingleGetMessageTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheReadFromBackupTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheNearOnlyTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheResultIsNotNullOnPartitionLossTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheEventWithTxLabelTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheProcessorActiveTxTest.class, ignoredTests);

        return suite;
    }
}

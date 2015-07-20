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

import junit.framework.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.integration.*;

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

        suite.addTestSuite(IgniteCacheInvokeReadThroughTest.class);
        suite.addTestSuite(GridCacheVersionMultinodeTest.class);

        suite.addTestSuite(IgniteCacheNearReadCommittedTest.class);
        suite.addTestSuite(IgniteCacheAtomicCopyOnReadDisabledTest.class);
        suite.addTestSuite(IgniteCacheTxCopyOnReadDisabledTest.class);

        suite.addTestSuite(IgniteCacheTxPreloadNoWriteTest.class);

        suite.addTestSuite(IgniteDynamicCacheStartSelfTest.class);
        suite.addTestSuite(IgniteDynamicCacheWithConfigStartSelfTest.class);
        suite.addTestSuite(IgniteCacheDynamicStopSelfTest.class);
        suite.addTestSuite(IgniteCacheConfigurationTemplateTest.class);
        suite.addTestSuite(IgniteCacheConfigurationDefaultTemplateTest.class);
        suite.addTestSuite(IgniteDynamicClientCacheStartSelfTest.class);
        suite.addTestSuite(IgniteDynamicCacheStartNoExchangeTimeoutTest.class);

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

        suite.addTestSuite(CacheStopAndDestroySelfTest.class);

        suite.addTestSuite(CacheOffheapMapEntrySelfTest.class);

        suite.addTestSuite(CacheJdbcStoreSessionListenerSelfTest.class);

        suite.addTestSuite(CacheClientStoreSelfTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeStaticStartAtomicTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeStaticStartTxTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeDynamicStartAtomicTest.class);
        suite.addTestSuite(CacheStoreUsageMultinodeDynamicStartTxTest.class);

        suite.addTestSuite(IgniteStartCacheInTransactionSelfTest.class);
        suite.addTestSuite(IgniteStartCacheInTransactionAtomicSelfTest.class);

        suite.addTestSuite(CacheReadThroughRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughReplicatedRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughReplicatedAtomicRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughLocalRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughLocalAtomicRestartSelfTest.class);
        suite.addTestSuite(CacheReadThroughAtomicRestartSelfTest.class);

        return suite;
    }
}

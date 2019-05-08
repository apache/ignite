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
import org.apache.ignite.internal.processors.cache.CachePutIfAbsentTest;
import org.apache.ignite.internal.processors.cache.GridCacheLongRunningTransactionDiagnosticsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCacheWriteSynchronizationModesMultithreadedTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
import org.apache.ignite.internal.processors.cache.transactions.PartitionUpdateCounterTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDataConsistencyOnCommitFailureTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest;
import org.apache.ignite.internal.stat.IoStatisticsCachePersistenceSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsCacheSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsManagerSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanImplSelfTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStatePutTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateTwoPrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateUpdatesOrderTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateWithFilterTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite9 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "false");

        TestSuite suite = new TestSuite("IgniteCache Test Suite part 9");

        suite.addTestSuite(IgniteCacheGetCustomCollectionsSelfTest.class);
        suite.addTestSuite(IgniteCacheLoadRebalanceEvictionSelfTest.class);
        suite.addTestSuite(IgniteCachePrimarySyncTest.class);
        suite.addTestSuite(IgniteTxCachePrimarySyncTest.class);
        suite.addTestSuite(IgniteTxCacheWriteSynchronizationModesMultithreadedTest.class);
        suite.addTestSuite(CachePutIfAbsentTest.class);

        suite.addTestSuite(CacheAtomicPrimarySyncBackPressureTest.class);

        suite.addTestSuite(IgniteTxConcurrentRemoveObjectsTest.class);

        suite.addTestSuite(TxDataConsistencyOnCommitFailureTest.class);

        suite.addTestSuite(GridCacheLongRunningTransactionDiagnosticsTest.class);

        suite.addTestSuite(CleanupRestoredCachesSlowTest.class);

        // Update counters and historical rebalance.
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryOneBackupTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStatePutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateTwoPrimaryTwoBackupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateWithFilterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionUpdateCounterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateUpdatesOrderTest.class, ignoredTests);

        return suite;
    }
}

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
import org.apache.ignite.cache.BreakRebalanceChainTest;
import org.apache.ignite.cache.NotOptimizedRebalanceTest;
import org.apache.ignite.cache.RebalanceCancellationTest;
import org.apache.ignite.cache.RebalanceCompleteDuringExchangeTest;
import org.apache.ignite.cache.ResetLostPartitionTest;
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse;
import org.apache.ignite.internal.processors.cache.distributed.CachePageWriteLockUnlockTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.IgniteRebalanceOnCachesStoppingOrDestroyingTest;
import org.apache.ignite.internal.processors.cache.persistence.CorruptedTreeFailureHandlingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheEntriesExpirationTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRemoveDuringRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRestartAfterFailedToWriteMetaPageTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsSpuriousRebalancingOnNodeJoinTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTaskCancelingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheWalDisabledOnRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageReplacementDuringPartitionClearTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPartitionPreloadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsStartWIthEmptyArchive;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsTransactionsHangTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.HistoricalReservationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRebalanceRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManagerTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToFileDumpProcessorTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.HeapArrayLockLogTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.OffHeapLockLogTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.HeapArrayLockStackTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.OffHeapLockStackTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileDownloaderTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite4 {
    /**
     * @return Suite.
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

        addRealPageStoreTestsNotForDirectIo(suite, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, FileDownloaderTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsTaskCancelingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPartitionPreloadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ResetLostPartitionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteRebalanceOnCachesStoppingOrDestroyingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePageWriteLockUnlockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheWalDisabledOnRebalancingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsStartWIthEmptyArchive.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CorruptedTreeFailureHandlingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RebalanceCancellationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, NotOptimizedRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, BreakRebalanceChainTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, WalRebalanceRestartTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsRestartAfterFailedToWriteMetaPageTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsRemoveDuringRebalancingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsSpuriousRebalancingOnNodeJoinTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RebalanceCompleteDuringExchangeTest.class, ignoredTests);

        // Page lock tracker tests.
        GridTestUtils.addTestIfNeeded(suite, PageLockTrackerManagerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SharedPageLockTrackerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ToFileDumpProcessorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, HeapArrayLockLogTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, HeapArrayLockStackTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OffHeapLockLogTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OffHeapLockStackTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, HistoricalReservationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheEntriesExpirationTest.class, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsNotForDirectIo(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsTransactionsHangTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPageReplacementDuringPartitionClearTest.class, ignoredTests);

        // Rebalancing test
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes.class, ignoredTests);

        // Integrity test.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsRecoveryAfterFileCorruptionTest.class, ignoredTests);
    }
}

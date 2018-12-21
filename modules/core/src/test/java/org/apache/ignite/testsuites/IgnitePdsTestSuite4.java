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

import java.util.Collection;
import junit.framework.TestSuite;
import org.apache.ignite.cache.ResetLostPartitionTest;
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse;
import org.apache.ignite.internal.processors.cache.distributed.CachePageWriteLockUnlockTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.IgniteRebalanceOnCachesStoppingOrDestroyingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTaskCancelingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheWalDisabledOnRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageEvictionDuringPartitionClearTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPartitionPreloadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsTransactionsHangTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileDownloaderTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class IgnitePdsTestSuite4 extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Ignored tests.
     * @return Suite.
     */
    public static TestSuite suite(Collection<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite 4");

        addRealPageStoreTestsNotForDirectIo(suite, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, FileDownloaderTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsTaskCancelingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPartitionPreloadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ResetLostPartitionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteRebalanceOnCachesStoppingOrDestroyingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePageWriteLockUnlockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheWalDisabledOnRebalancingTest.class, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsNotForDirectIo(TestSuite suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsTransactionsHangTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPageEvictionDuringPartitionClearTest.class, ignoredTests);

        // Rebalancing test
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes.class, ignoredTests);

        // Integrity test.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsRecoveryAfterFileCorruptionTest.class, ignoredTests);
    }
}

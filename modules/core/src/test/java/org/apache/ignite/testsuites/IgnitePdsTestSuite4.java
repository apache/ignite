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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 *
 */
@RunWith(AllTests.class)
public class IgnitePdsTestSuite4 {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite 4");

        addRealPageStoreTestsNotForDirectIo(suite);

        suite.addTest(new JUnit4TestAdapter(FileDownloaderTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsTaskCancelingTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsPartitionPreloadTest.class));

        suite.addTest(new JUnit4TestAdapter(ResetLostPartitionTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteRebalanceOnCachesStoppingOrDestroyingTest.class));

        suite.addTest(new JUnit4TestAdapter(CachePageWriteLockUnlockTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheWalDisabledOnRebalancingTest.class));

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     */
    private static void addRealPageStoreTestsNotForDirectIo(TestSuite suite) {
        suite.addTest(new JUnit4TestAdapter(IgnitePdsTransactionsHangTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsPageEvictionDuringPartitionClearTest.class));

        // Rebalancing test
        suite.addTest(new JUnit4TestAdapter(IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes.class));

        // Integrity test.
        suite.addTest(new JUnit4TestAdapter(IgnitePdsRecoveryAfterFileCorruptionTest.class));
    }
}

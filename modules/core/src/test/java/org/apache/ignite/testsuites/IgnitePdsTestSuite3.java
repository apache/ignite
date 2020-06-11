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
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsNodeJoinWithCachesStopping;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionFilesDestroyTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWacModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageEvictionDuringPartitionClearTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsTransactionsHangTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.CorruptedCheckpointReservationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithDedicatedWorkerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithMmapBufferSelfTest;

/**
 *
 */
public class IgnitePdsTestSuite3 extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite 3");

        addRealPageStoreTestsNotForDirectIo(suite);

        addRealPageStoreTestsLongRunning(suite);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     */
    public static void addRealPageStoreTestsNotForDirectIo(TestSuite suite) {
        // Rebalancing test
        suite.addTestSuite(IgnitePdsContinuousRestartTest.class);
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     */
    private static void addRealPageStoreTestsLongRunning(TestSuite suite) {
        suite.addTestSuite(IgnitePdsTransactionsHangTest.class);

        suite.addTestSuite(IgnitePdsPageEvictionDuringPartitionClearTest.class);

        suite.addTestSuite(IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes.class);

        // Integrity test.
        suite.addTestSuite(IgnitePdsRecoveryAfterFileCorruptionTest.class);

        suite.addTestSuite(IgnitePdsPartitionFilesDestroyTest.class);

        suite.addTestSuite(LocalWalModeChangeDuringRebalancingSelfTest.class);

        suite.addTestSuite(LocalWacModeNoChangeDuringRebalanceOnNonNodeAssignTest.class);

        suite.addTestSuite(IgniteWalFlushFsyncSelfTest.class);

        suite.addTestSuite(IgniteWalFlushFsyncWithDedicatedWorkerSelfTest.class);

        suite.addTestSuite(IgniteWalFlushFsyncWithMmapBufferSelfTest.class);

//        https://ggsystems.atlassian.net/browse/GG-26123
//        suite.addTestSuite(CorruptedCheckpointReservationTest.class);

        suite.addTestSuite(IgnitePdsNodeJoinWithCachesStopping.class);
    }
}

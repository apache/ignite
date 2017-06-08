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
import org.apache.ignite.cache.database.IgnitePdsAtomicCacheRebalancingTest;
import org.apache.ignite.cache.database.IgnitePdsTxCacheRebalancingTest;
import org.apache.ignite.cache.database.IgnitePdsContinuousRestartTest;
import org.apache.ignite.cache.database.IgnitePdsPageSizesTest;
import org.apache.ignite.cache.database.IgnitePdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.cache.database.IgnitePersistenceMetricsSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreSchemaLoadTest;
import org.apache.ignite.cache.database.db.IgnitePdsPageEvictionDuringPartitionClearTest;
import org.apache.ignite.cache.database.db.IgnitePdsTransactionsHangTest;
import org.apache.ignite.cache.database.db.IgnitePdsWholeClusterRestartTest;
import org.apache.ignite.cache.database.db.IgnitePdsRebalancingOnNotStableTopologyTest;
import org.apache.ignite.cache.database.db.file.IgnitePdsNoActualWalHistoryTest;
import org.apache.ignite.cache.database.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.cache.database.db.wal.IgniteWalRecoveryTest;
import org.apache.ignite.cache.database.db.wal.WalRecoveryTxLogicalRecordsTest;
import org.apache.ignite.cache.database.db.wal.crc.IgniteDataIntegrityTests;

/**
 *
 */
public class IgnitePdsTestSuite2 extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite persistent Store Test Suite 2");

        // Integrity test.
        suite.addTestSuite(IgniteDataIntegrityTests.class);
        suite.addTestSuite(IgnitePdsRecoveryAfterFileCorruptionTest.class);
        suite.addTestSuite(IgnitePdsPageSizesTest.class);

        // Metrics test.
        suite.addTestSuite(IgnitePersistenceMetricsSelfTest.class);

        // WAL recovery test.
        suite.addTestSuite(WalRecoveryTxLogicalRecordsTest.class);
        suite.addTestSuite(IgniteWalRecoveryTest.class);

        suite.addTestSuite(IgnitePdsTransactionsHangTest.class);

        suite.addTestSuite(IgnitePdsNoActualWalHistoryTest.class);

        suite.addTestSuite(IgnitePdsRebalancingOnNotStableTopologyTest.class);

        suite.addTestSuite(IgnitePdsWholeClusterRestartTest.class);

        suite.addTestSuite(IgnitePdsPageEvictionDuringPartitionClearTest.class);

        // Rebalancing test
        suite.addTestSuite(IgnitePdsAtomicCacheRebalancingTest.class);
        suite.addTestSuite(IgnitePdsTxCacheRebalancingTest.class);
        suite.addTestSuite(IgniteWalHistoryReservationsTest.class);

        suite.addTestSuite(IgnitePdsContinuousRestartTest.class);

        suite.addTestSuite(IgnitePersistentStoreSchemaLoadTest.class);
        suite.addTestSuite(IgnitePersistentStoreDataStructuresTest.class);

        return suite;
    }
}

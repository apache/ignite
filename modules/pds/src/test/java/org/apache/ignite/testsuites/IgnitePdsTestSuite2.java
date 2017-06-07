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
import org.apache.ignite.cache.database.IgnitePersistenceMetricsSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreAtomicCacheRebalancingTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreContinuousRestartSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.cache.database.IgnitePersistentStorePageSizesTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreRecoveryAfterFileCorruptionTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreSchemaLoadTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreTxCacheRebalancingTest;
import org.apache.ignite.cache.database.db.DbPageEvictionDuringPartitionClearSelfTest;
import org.apache.ignite.cache.database.db.IgniteDbWholeClusterRestartSelfTest;
import org.apache.ignite.cache.database.db.RebalancingOnNotStableTopologyTest;
import org.apache.ignite.cache.database.db.TransactionsHangTest;
import org.apache.ignite.cache.database.db.file.IgniteNoActualWalHistorySelfTest;
import org.apache.ignite.cache.database.db.file.IgniteWalHistoryReservationsSelfTest;
import org.apache.ignite.cache.database.db.file.IgniteWalRecoverySelfTest;
import org.apache.ignite.cache.database.db.file.WalRecoveryTxLogicalRecordsTest;
import org.apache.ignite.cache.database.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoLoadSelfTest;

/**
 *
 */
public class IgnitePdsTestSuite2 extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite persistent Store Test Suite");

        // Integrity test.
        suite.addTestSuite(IgniteDataIntegrityTests.class);
        suite.addTestSuite(IgnitePersistentStoreRecoveryAfterFileCorruptionTest.class);
        suite.addTestSuite(IgnitePersistentStorePageSizesTest.class);

        // Metrics test.
        suite.addTestSuite(IgnitePersistenceMetricsSelfTest.class);

        // WAL recovery test.
        suite.addTestSuite(WalRecoveryTxLogicalRecordsTest.class);
        suite.addTestSuite(IgniteWalRecoverySelfTest.class);

        suite.addTestSuite(TransactionsHangTest.class);

        suite.addTestSuite(IgniteNoActualWalHistorySelfTest.class);

        suite.addTestSuite(RebalancingOnNotStableTopologyTest.class);

        suite.addTestSuite(IgniteDbWholeClusterRestartSelfTest.class);

        suite.addTestSuite(DbPageEvictionDuringPartitionClearSelfTest.class);

        // Rebalancing test
        suite.addTestSuite(IgnitePersistentStoreAtomicCacheRebalancingTest.class);
        suite.addTestSuite(IgnitePersistentStoreTxCacheRebalancingTest.class);
        suite.addTestSuite(IgniteWalHistoryReservationsSelfTest.class);

        suite.addTestSuite(IgnitePersistentStoreContinuousRestartSelfTest.class);

        suite.addTestSuite(IgnitePersistentStoreSchemaLoadTest.class);
        suite.addTestSuite(IgnitePersistentStoreDataStructuresTest.class);

        return suite;
    }
}

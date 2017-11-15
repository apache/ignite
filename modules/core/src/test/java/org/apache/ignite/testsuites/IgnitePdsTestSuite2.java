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
import org.apache.ignite.internal.processors.cache.persistence.IgniteDataStorageMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsExchangeDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPageSizesTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageEvictionDuringPartitionClearTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsRebalancingOnNotStableTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsTransactionsHangTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWholeClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushDefaultSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlySelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalSerializerVersionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IgniteWalReaderTest;

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
        suite.addTestSuite(IgniteDataStorageMetricsSelfTest.class);

        suite.addTestSuite(IgnitePdsTransactionsHangTest.class);

        suite.addTestSuite(IgnitePdsRebalancingOnNotStableTopologyTest.class);

        suite.addTestSuite(IgnitePdsWholeClusterRestartTest.class);

        suite.addTestSuite(IgnitePdsPageEvictionDuringPartitionClearTest.class);

        // Rebalancing test
        suite.addTestSuite(IgniteWalHistoryReservationsTest.class);

        suite.addTestSuite(IgnitePdsContinuousRestartTest.class);

        suite.addTestSuite(IgnitePdsContinuousRestartTestWithSharedGroupAndIndexes.class);

        suite.addTestSuite(IgnitePersistentStoreDataStructuresTest.class);

        // Failover test
        suite.addTestSuite(IgniteWalFlushFailoverTest.class);

        suite.addTestSuite(IgniteWalFlushDefaultSelfTest.class);

        suite.addTestSuite(IgniteWalFlushBackgroundSelfTest.class);

        suite.addTestSuite(IgniteWalFlushLogOnlySelfTest.class);

        suite.addTestSuite(IgniteWalReaderTest.class);

        suite.addTestSuite(IgnitePdsExchangeDuringCheckpointTest.class);

        // new style folders with generated consistent ID test
        suite.addTestSuite(IgniteUidAsConsistentIdMigrationTest.class);

        suite.addTestSuite(IgniteWalSerializerVersionTest.class);

        return suite;
    }
}

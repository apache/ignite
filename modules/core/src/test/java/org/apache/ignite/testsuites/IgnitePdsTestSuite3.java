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
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedCacheDataTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedStoreTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsExchangeDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPageSizesTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClientAffinityAssignmentWithBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAbsentEvictionNodeOutOfBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAllBaselineNodesOnlineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOfflineBaselineNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOnlineNodeOutOfBaselineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsRebalancingOnNotStableTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsUnusedWalSegmentsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWholeClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.IgniteCheckpointDirtyPagesForLowLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlySelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlyWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFormatFileFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorSwitchSegmentTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalSerializerVersionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IgniteWalReaderTest;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 *
 */
public class IgnitePdsTestSuite3 extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite persistent Store Test Suite 3");

        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "true");

        addRealPageStoreTests(suite);

        // BaselineTopology tests
        suite.addTestSuite(IgniteAllBaselineNodesOnlineFullApiSelfTest.class);
        suite.addTestSuite(IgniteOfflineBaselineNodeFullApiSelfTest.class);
        suite.addTestSuite(IgniteOnlineNodeOutOfBaselineFullApiSelfTest.class);
        suite.addTestSuite(ClientAffinityAssignmentWithBaselineTest.class);
        suite.addTestSuite(IgniteAbsentEvictionNodeOutOfBaselineTest.class);


        return suite;
    }


    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * NOTE: These tests are also executed using I/O plugins.
     *
     * @param suite suite to add tests into.
     */
    public static void addRealPageStoreTests(TestSuite suite) {
        suite.addTestSuite(IgnitePdsPageSizesTest.class);

        // Metrics test.
        suite.addTestSuite(IgniteDataStorageMetricsSelfTest.class);

        suite.addTestSuite(IgnitePdsRebalancingOnNotStableTopologyTest.class);

        suite.addTestSuite(IgnitePdsWholeClusterRestartTest.class);

        // Rebalancing test
        suite.addTestSuite(IgniteWalHistoryReservationsTest.class);

        suite.addTestSuite(IgnitePersistentStoreDataStructuresTest.class);

        // Failover test
        suite.addTestSuite(IgniteWalFlushFailoverTest.class);

        suite.addTestSuite(IgniteWalFlushBackgroundSelfTest.class);

        suite.addTestSuite(IgniteWalFlushBackgroundWithMmapBufferSelfTest.class);

        suite.addTestSuite(IgniteWalFlushLogOnlySelfTest.class);

        suite.addTestSuite(IgniteWalFlushLogOnlyWithMmapBufferSelfTest.class);

        suite.addTestSuite(IgniteWalFormatFileFailoverTest.class);

        // Test suite uses Standalone WAL iterator to verify PDS content.
        suite.addTestSuite(IgniteWalReaderTest.class);

        suite.addTestSuite(IgnitePdsExchangeDuringCheckpointTest.class);

        suite.addTestSuite(IgnitePdsUnusedWalSegmentsTest.class);

        // new style folders with generated consistent ID test
        suite.addTestSuite(IgniteUidAsConsistentIdMigrationTest.class);

        suite.addTestSuite(IgniteWalSerializerVersionTest.class);

        suite.addTestSuite(WalCompactionTest.class);

        suite.addTestSuite(IgniteCheckpointDirtyPagesForLowLoadTest.class);

        suite.addTestSuite(IgnitePdsCorruptedStoreTest.class);

        suite.addTestSuite(IgnitePdsCorruptedCacheDataTest.class);

        suite.addTestSuite(IgniteWalIteratorSwitchSegmentTest.class);

        suite.addTestSuite(IgniteWalIteratorExceptionDuringReadTest.class);
    }
}

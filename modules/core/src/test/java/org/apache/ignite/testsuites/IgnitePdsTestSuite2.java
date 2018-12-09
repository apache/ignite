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
import org.apache.ignite.internal.processors.cache.persistence.IgniteDataStorageMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheStartStopWithFreqCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedStoreTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsExchangeDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPageSizesTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionFilesDestroyTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionsStateRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteRebalanceScheduleResendPartitionsTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWacModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClientAffinityAssignmentWithBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAbsentEvictionNodeOutOfBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAllBaselineNodesOnlineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOfflineBaselineNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOnlineNodeOutOfBaselineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsRebalancingOnNotStableTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWholeClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowHistoricalRebalanceSmallHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.IgniteCheckpointDirtyPagesForLowLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.FsyncWalRolloverDoesNotBlockTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteNodeStoppedDuringDisableWALTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWALTailIsReachedDuringIterationOverArchiveTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithDedicatedWorkerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlySelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlyWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFormatFileFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorSwitchSegmentTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalSerializerVersionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalDeletionArchiveFsyncTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalDeletionArchiveLogOnlyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverTypesTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteFsyncReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteStandaloneWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgnitePureJavaCrcCompatibility;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IgniteWalReaderTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIteratorTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 *
 */
@RunWith(AllTests.class)
public class IgnitePdsTestSuite2 {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite persistent Store Test Suite 2");

        // Integrity test.
        suite.addTest(new JUnit4TestAdapter(IgniteDataIntegrityTests.class));
        suite.addTest(new JUnit4TestAdapter(IgniteStandaloneWalIteratorInvalidCrcTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteReplayWalIteratorInvalidCrcTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteFsyncReplayWalIteratorInvalidCrcTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePureJavaCrcCompatibility.class));

        addRealPageStoreTests(suite);

        addRealPageStoreTestsNotForDirectIo(suite);

        // BaselineTopology tests
        suite.addTest(new JUnit4TestAdapter(IgniteAllBaselineNodesOnlineFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteOfflineBaselineNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteOnlineNodeOutOfBaselineFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientAffinityAssignmentWithBaselineTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteAbsentEvictionNodeOutOfBaselineTest.class));

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     */
    private static void addRealPageStoreTestsNotForDirectIo(TestSuite suite) {
        suite.addTest(new JUnit4TestAdapter(IgnitePdsPartitionFilesDestroyTest.class));

        suite.addTest(new JUnit4TestAdapter(LocalWalModeChangeDuringRebalancingSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(LocalWacModeNoChangeDuringRebalanceOnNonNodeAssignTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushFsyncSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushFsyncWithDedicatedWorkerSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushFsyncWithMmapBufferSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheStartStopWithFreqCheckpointTest.class));
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * NOTE: These tests are also executed using I/O plugins.
     *
     * @param suite suite to add tests into.
     */
    public static void addRealPageStoreTests(TestSuite suite) {
        suite.addTest(new JUnit4TestAdapter(IgnitePdsPageSizesTest.class));

        // Metrics test.
        suite.addTest(new JUnit4TestAdapter(IgniteDataStorageMetricsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsRebalancingOnNotStableTopologyTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsWholeClusterRestartTest.class));

        // Rebalancing test
        suite.addTest(new JUnit4TestAdapter(IgniteWalHistoryReservationsTest.class));

        suite.addTest(new JUnit4TestAdapter(SlowHistoricalRebalanceSmallHistoryTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePersistentStoreDataStructuresTest.class));

        // Failover test
        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushFailoverTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushBackgroundSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushBackgroundWithMmapBufferSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushLogOnlySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFlushLogOnlyWithMmapBufferSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalFormatFileFailoverTest.class));

        // Test suite uses Standalone WAL iterator to verify PDS content.
        suite.addTest(new JUnit4TestAdapter(IgniteWalReaderTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsExchangeDuringCheckpointTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsReserveWalSegmentsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsReserveWalSegmentsWithCompactionTest.class));

        // new style folders with generated consistent ID test
        suite.addTest(new JUnit4TestAdapter(IgniteUidAsConsistentIdMigrationTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalSerializerVersionTest.class));

        suite.addTestSuite(WalCompactionTest.class);

        suite.addTest(new JUnit4TestAdapter(WalDeletionArchiveFsyncTest.class));
        suite.addTest(new JUnit4TestAdapter(WalDeletionArchiveLogOnlyTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCheckpointDirtyPagesForLowLoadTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCorruptedStoreTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalIteratorSwitchSegmentTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalIteratorExceptionDuringReadTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteNodeStoppedDuringDisableWALTest.class));

        suite.addTest(new JUnit4TestAdapter(StandaloneWalRecordsIteratorTest.class));

        //suite.addTest(new JUnit4TestAdapter(IgniteWalRecoverySeveralRestartsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteRebalanceScheduleResendPartitionsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWALTailIsReachedDuringIterationOverArchiveTest.class));

        suite.addTest(new JUnit4TestAdapter(WalRolloverTypesTest.class));

        suite.addTest(new JUnit4TestAdapter(FsyncWalRolloverDoesNotBlockTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsPartitionsStateRecoveryTest.class));
    }
}

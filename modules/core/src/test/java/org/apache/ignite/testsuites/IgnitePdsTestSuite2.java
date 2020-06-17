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
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedStoreTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheWithoutCheckpointsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsExchangeDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPageSizesTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionsStateRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteRebalanceScheduleResendPartitionsTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.WalPreloadingConcurrentTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClientAffinityAssignmentWithBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAbsentEvictionNodeOutOfBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAllBaselineNodesOnlineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOfflineBaselineNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOnlineNodeOutOfBaselineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.FullHistRebalanceOnClientStopTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsRebalancingOnNotStableTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsUnusedWalSegmentsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWholeClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowHistoricalRebalanceSmallHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointFailBeforeWriteMarkTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointFreeListTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.IgniteCheckpointDirtyPagesForLowLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteNodeStoppedDuringDisableWALTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWALTailIsReachedDuringIterationOverArchiveTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlySelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlyWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFormatFileFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorSwitchSegmentTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceLoggingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalReplayingAfterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalSerializerVersionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionNoArchiverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionSwitchOnTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteFsyncReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgnitePureJavaCrcCompatibility;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteStandaloneWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IgniteWalReaderTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileDownloaderTest;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListCachingTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIteratorTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIteratorTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScannerTest;

/**
 *
 */
public class IgnitePdsTestSuite2 extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite persistent Store Test Suite 2");

        // Integrity test.
        suite.addTestSuite(IgniteDataIntegrityTests.class);
        suite.addTestSuite(IgniteStandaloneWalIteratorInvalidCrcTest.class);
        suite.addTestSuite(IgniteReplayWalIteratorInvalidCrcTest.class);
        suite.addTestSuite(IgniteFsyncReplayWalIteratorInvalidCrcTest.class);
        suite.addTestSuite(IgnitePureJavaCrcCompatibility.class);

        addRealPageStoreTests(suite);

        // BaselineTopology tests
        suite.addTestSuite(IgniteAllBaselineNodesOnlineFullApiSelfTest.class);
        suite.addTestSuite(IgniteOfflineBaselineNodeFullApiSelfTest.class);
        suite.addTestSuite(IgniteOnlineNodeOutOfBaselineFullApiSelfTest.class);
        suite.addTestSuite(ClientAffinityAssignmentWithBaselineTest.class);
        suite.addTestSuite(IgniteAbsentEvictionNodeOutOfBaselineTest.class);

        suite.addTestSuite(IgnitePdsDestroyCacheWithoutCheckpointsTest.class);

        suite.addTestSuite(FileDownloaderTest.class);

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
        suite.addTestSuite(StandaloneWalRecordsIteratorTest.class);

        suite.addTestSuite(IgnitePdsPageSizesTest.class);

        // Metrics test.
        suite.addTestSuite(IgniteDataStorageMetricsSelfTest.class);

        suite.addTestSuite(IgnitePdsRebalancingOnNotStableTopologyTest.class);

        suite.addTestSuite(IgnitePdsWholeClusterRestartTest.class);

        // Rebalancing test
        suite.addTestSuite(IgniteWalHistoryReservationsTest.class);

        suite.addTestSuite(SlowHistoricalRebalanceSmallHistoryTest.class);

        suite.addTestSuite(IgnitePersistentStoreDataStructuresTest.class);

        suite.addTestSuite(FullHistRebalanceOnClientStopTest.class);

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
        suite.addTestSuite(IgnitePdsReserveWalSegmentsWithCompactionTest.class);

        suite.addTestSuite(IgniteWalReplayingAfterRestartTest.class);

        // new style folders with generated consistent ID test
        suite.addTestSuite(IgniteUidAsConsistentIdMigrationTest.class);

        suite.addTestSuite(IgniteWalSerializerVersionTest.class);

        suite.addTestSuite(WalCompactionTest.class);
        suite.addTestSuite(WalCompactionNoArchiverTest.class);
        suite.addTestSuite(WalCompactionSwitchOnTest.class);

        suite.addTestSuite(IgniteCheckpointDirtyPagesForLowLoadTest.class);

        suite.addTestSuite(IgnitePdsCorruptedStoreTest.class);

        suite.addTestSuite(LocalWalModeChangeDuringRebalancingSelfTest.class);

        suite.addTestSuite(CheckpointFreeListTest.class);

        suite.addTestSuite(FreeListCachingTest.class);

        suite.addTestSuite(CheckpointFailBeforeWriteMarkTest.class);

        suite.addTestSuite(IgniteWalIteratorSwitchSegmentTest.class);

        suite.addTestSuite(IgniteWalIteratorExceptionDuringReadTest.class);

        suite.addTestSuite(IgniteNodeStoppedDuringDisableWALTest.class);

        suite.addTestSuite(FilteredWalIteratorTest.class);

        suite.addTestSuite(WalScannerTest.class);

        //suite.addTestSuite(IgniteWalRecoverySeveralRestartsTest.class);

        suite.addTestSuite(IgniteRebalanceScheduleResendPartitionsTest.class);

        suite.addTestSuite(IgniteWALTailIsReachedDuringIterationOverArchiveTest.class);

        suite.addTestSuite(IgnitePdsPartitionsStateRecoveryTest.class);

        suite.addTestSuite(WalPreloadingConcurrentTest.class);

        suite.addTestSuite(IgniteWalRebalanceLoggingTest.class);
    }
}

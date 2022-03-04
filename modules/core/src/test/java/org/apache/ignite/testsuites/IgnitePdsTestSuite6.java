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
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheStartStopWithFreqCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionFilesDestroyTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.MaintenanceRegistrySimpleTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClientAffinityAssignmentWithBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClusterActivationEventTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClusterActivationEventWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAbsentEvictionNodeOutOfBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAllBaselineNodesOnlineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOfflineBaselineNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOnlineNodeOutOfBaselineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointMarkerReadingErrorOnStartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithDedicatedWorkerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalArchiveSizeConfigurationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteFsyncReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgnitePureJavaCrcCompatibility;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteStandaloneWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteWithoutArchiverWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cluster.ClusterStateChangeEventTest;
import org.apache.ignite.internal.processors.cluster.ClusterStateChangeEventWithPersistenceTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite6 {
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

        // Integrity test.
        GridTestUtils.addTestIfNeeded(suite, IgniteDataIntegrityTests.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteStandaloneWalIteratorInvalidCrcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteReplayWalIteratorInvalidCrcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteFsyncReplayWalIteratorInvalidCrcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePureJavaCrcCompatibility.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWithoutArchiverWalIteratorInvalidCrcTest.class, ignoredTests);

        addRealPageStoreTestsNotForDirectIo(suite, ignoredTests);

        // BaselineTopology tests
        GridTestUtils.addTestIfNeeded(suite, IgniteAllBaselineNodesOnlineFullApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOfflineBaselineNodeFullApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOnlineNodeOutOfBaselineFullApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClientAffinityAssignmentWithBaselineTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteAbsentEvictionNodeOutOfBaselineTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterActivationEventTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterActivationEventWithPersistenceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateChangeEventTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateChangeEventWithPersistenceTest.class, ignoredTests);

        // Maintenance tests
        GridTestUtils.addTestIfNeeded(suite, MaintenanceRegistrySimpleTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, WalArchiveSizeConfigurationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CheckpointMarkerReadingErrorOnStartTest.class, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsNotForDirectIo(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPartitionFilesDestroyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LocalWalModeChangeDuringRebalancingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncWithDedicatedWorkerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncWithMmapBufferSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheStartStopWithFreqCheckpointTest.class, ignoredTests);
    }
}

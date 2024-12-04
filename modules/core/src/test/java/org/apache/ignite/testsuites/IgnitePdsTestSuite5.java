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
import org.apache.ignite.internal.processors.cache.persistence.wal.WalRotatedIdPartRecordTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite5 {
    /**
     * @return IgniteCache test suite.
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

//        // Basic PageMemory tests.
//        GridTestUtils.addTestIfNeeded(suite, PageMemoryNoLoadSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageMemoryImplNoLoadTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageMemoryNoStoreLeakTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageMemoryLazyAllocationTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageMemoryLazyAllocationWithPDSTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IndexStoragePageMemoryImplTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageMemoryImplTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageIdDistributionTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, TrackingPageIOTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PageIOFreeSizeTest.class, ignoredTests);
//
//        // BTree tests with store page memory.
//        GridTestUtils.addTestIfNeeded(suite, BPlusTreePageMemoryImplTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, BPlusTreeReuseListPageMemoryImplTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, SegmentedRingByteBufferTest.class, ignoredTests);
//
//        // Write throttling
//        GridTestUtils.addTestIfNeeded(suite, PagesWriteThrottleSmokeTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, SpeedBasedThrottleBreakdownTest.class, ignoredTests);
//
//        // Discovery data handling on node join and old cluster abnormal shutdown
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDiscoDataHandlingInNewClusterTest.class, ignoredTests);
//
//        // Metrics
//        GridTestUtils.addTestIfNeeded(suite, FillFactorMetricTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, UsedPagesMetricTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, UsedPagesMetricTestPersistence.class, ignoredTests);
//
//        // WAL delta consistency
//        GridTestUtils.addTestIfNeeded(suite, CpTriggeredWalDeltaConsistencyTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ExplicitWalDeltaConsistencyTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, SysPropWalDeltaConsistencyTest.class, ignoredTests);
//
//        // Binary meta tests.
//        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, SegmentAwareTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, WalEnableDisableWithNodeShutdownTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalEnableDisableWithRestartsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, WalArchiveConsistencyTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, RestorePartitionStateTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, FileWriteAheadLogManagerSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteCacheDatabaseSharedManagerSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, WalCompactionNotificationsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, WalPageRecordCompactionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, WalRotatedIdPartRecordTest.class, ignoredTests);

        return suite;
    }
}

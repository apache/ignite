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
import org.apache.ignite.cdc.CdcCacheConfigOnRestartTest;
import org.apache.ignite.cdc.CdcNonDefaultWorkDirTest;
import org.apache.ignite.cdc.CdcPushMetricsExporterTest;
import org.apache.ignite.cdc.CdcSelfTest;
import org.apache.ignite.cdc.RestartWithWalForceArchiveTimeoutTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedStoreTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsNoSpaceLeftOnDeviceTest;
import org.apache.ignite.internal.processors.cache.persistence.WALPreloadingWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowHistoricalRebalanceSmallHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointListenerForRegionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.IgniteCheckpointDirtyPagesForLowLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.FsyncWalRolloverDoesNotBlockTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceLoggingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIteratorTest;
import org.apache.ignite.internal.util.io.GridFileUtilsTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite2 {
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

        addRealPageStoreTests(suite, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * NOTE: These tests are also executed using I/O plugins.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    public static void addRealPageStoreTests(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, CdcCacheConfigOnRestartTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CdcNonDefaultWorkDirTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CdcPushMetricsExporterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CdcSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CheckpointListenerForRegionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, FilteredWalIteratorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, FsyncWalRolloverDoesNotBlockTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridFileUtilsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCheckpointDirtyPagesForLowLoadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCorruptedStoreTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsNoSpaceLeftOnDeviceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushBackgroundSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalIteratorExceptionDuringReadTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWalRebalanceLoggingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RestartWithWalForceArchiveTimeoutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SlowHistoricalRebalanceSmallHistoryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, WALPreloadingWithCompactionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, WalCompactionTest.class, ignoredTests);
    }
}

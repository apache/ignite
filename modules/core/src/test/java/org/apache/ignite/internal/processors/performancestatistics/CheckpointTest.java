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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED;
import static org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl.DATAREGION_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests checkpoint performance statistics.
 */
public class CheckpointTest extends AbstractPerformanceStatisticsTest {
    /** Ignite. */
    private static IgniteEx srv;

    /** Listener test logger. */
    private static ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningLog);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(10 * 1024 * 1024)
                .setCheckpointPageBufferSize(1024 * 1024)
                .setMetricsEnabled(true)
                .setPersistenceEnabled(true))
            .setWalMode(WALMode.BACKGROUND)
            .setCheckpointFrequency(1_000)
            .setWriteThrottlingEnabled(true)
            .setCheckpointThreads(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        listeningLog = new ListeningTestLogger(log);

        cleanPersistenceDir();

        cleanPerformanceStatisticsDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param ignite Ignite instance.
     * @return {@code totalThrottlingTime} metric for the default region.
     */
    private LongAdderMetric totalThrottlingTime(IgniteEx ignite) {
        MetricRegistry mreg = ignite.context().metric().registry(metricName(DATAREGION_METRICS_PREFIX,
            ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName()));

        LongAdderMetric totalThrottlingTime = mreg.findMetric("TotalThrottlingTime");

        assertNotNull(totalThrottlingTime);

        return totalThrottlingTime;
    }

    /** @throws Exception If failed. */
    @Test
    public void testCheckpoint() throws Exception {
        srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        MetricRegistry mreg = srv.context().metric().registry(DATASTORAGE_METRIC_PREFIX);

        AtomicLongMetric mLastCpBeforeLockDuration = mreg.findMetric("LastCheckpointBeforeLockDuration");
        AtomicLongMetric mLastCpLockWaitDuration = mreg.findMetric("LastCheckpointLockWaitDuration");
        AtomicLongMetric mLastCpListenersExecDuration = mreg.findMetric("LastCheckpointListenersExecuteDuration");
        AtomicLongMetric mLastCpMarcDuration = mreg.findMetric("LastCheckpointMarkDuration");
        AtomicLongMetric mLastCpLockHoldDuration = mreg.findMetric("LastCheckpointLockHoldDuration");
        AtomicLongMetric mLastCpPagesWriteDuration = mreg.findMetric("LastCheckpointPagesWriteDuration");
        AtomicLongMetric mLastCpFsyncDuration = mreg.findMetric("LastCheckpointFsyncDuration");
        AtomicLongMetric mLastCpWalRecordFsyncDuration = mreg.findMetric("LastCheckpointWalRecordFsyncDuration");
        AtomicLongMetric mLastCpWriteEntryDuration = mreg.findMetric("LastCheckpointWriteEntryDuration");
        AtomicLongMetric mLastCpSplitAndSortPagesDuration =
            mreg.findMetric("LastCheckpointSplitAndSortPagesDuration");
        AtomicLongMetric mLastCpDuration = mreg.findMetric("LastCheckpointDuration");
        AtomicLongMetric mLastCpStart = mreg.findMetric("LastCheckpointStart");
        AtomicLongMetric mLastCpTotalPages = mreg.findMetric("LastCheckpointTotalPagesNumber");
        AtomicLongMetric mLastCpDataPages = mreg.findMetric("LastCheckpointDataPagesNumber");
        AtomicLongMetric mLastCpCOWPages = mreg.findMetric("LastCheckpointCopiedOnWritePagesNumber");

        startCollectStatistics();

        LogListener lsnr = LogListener.matches("Checkpoint finished")
            .build();

        listeningLog.registerListener(lsnr);

        forceCheckpoint();

        assertTrue(waitForCondition(lsnr::check, TIMEOUT));

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)srv.context().cache().context().database();

        db.checkpointReadLock();

        try {
            AtomicLong expLastCpBeforeLockDuration = new AtomicLong();
            AtomicLong expLastCpLockWaitDuration = new AtomicLong();
            AtomicLong expLastCpListenersExecDuration = new AtomicLong();
            AtomicLong expLastCpMarkDuration = new AtomicLong();
            AtomicLong expLastCpLockHoldDuration = new AtomicLong();
            AtomicLong expLastCpPagesWriteDuration = new AtomicLong();
            AtomicLong expLastCpFsyncDuration = new AtomicLong();
            AtomicLong expLastCpWalRecordFsyncDuration = new AtomicLong();
            AtomicLong expLastCpWriteEntryDuration = new AtomicLong();
            AtomicLong expLastCpSplitAndSortPagesDuration = new AtomicLong();
            AtomicLong expLastTotalDuration = new AtomicLong();
            AtomicLong expLastcCheckpointStartTime = new AtomicLong();
            AtomicInteger expPagesSize = new AtomicInteger();
            AtomicInteger expDataPagesWritten = new AtomicInteger();
            AtomicInteger expCOWPagesWritten = new AtomicInteger();

            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void checkpoint(long beforeLockDuration, long lockWaitDuration,
                    long listenersExecDuration, long markDuration, long lockHoldDuration, long pagesWriteDuration,
                    long fsyncDuration, long walCpRecordFsyncDuration, long writeCpEntryDuration,
                    long splitAndSortCpPagesDuration, long totalDuration, long cpStartTime, int pagesSize,
                    int dataPagesWritten, int cowPagesWritten) {
                    expLastCpBeforeLockDuration.set(beforeLockDuration);
                    expLastCpLockWaitDuration.set(lockWaitDuration);
                    expLastCpListenersExecDuration.set(listenersExecDuration);
                    expLastCpMarkDuration.set(markDuration);
                    expLastCpLockHoldDuration.set(lockHoldDuration);
                    expLastCpPagesWriteDuration.set(pagesWriteDuration);
                    expLastCpFsyncDuration.set(fsyncDuration);
                    expLastCpWalRecordFsyncDuration.set(walCpRecordFsyncDuration);
                    expLastCpWriteEntryDuration.set(writeCpEntryDuration);
                    expLastCpSplitAndSortPagesDuration.set(splitAndSortCpPagesDuration);
                    expLastTotalDuration.set(totalDuration);
                    expLastcCheckpointStartTime.set(cpStartTime);
                    expPagesSize.set(pagesSize);
                    expDataPagesWritten.set(dataPagesWritten);
                    expCOWPagesWritten.set(cowPagesWritten);
                }
            });

            assertEquals(expLastCpBeforeLockDuration.get(), mLastCpBeforeLockDuration.value());
            assertEquals(expLastCpLockWaitDuration.get(), mLastCpLockWaitDuration.value());
            assertEquals(expLastCpListenersExecDuration.get(), mLastCpListenersExecDuration.value());
            assertEquals(expLastCpMarkDuration.get(), mLastCpMarcDuration.value());
            assertEquals(expLastCpLockHoldDuration.get(), mLastCpLockHoldDuration.value());
            assertEquals(expLastCpPagesWriteDuration.get(), mLastCpPagesWriteDuration.value());
            assertEquals(expLastCpFsyncDuration.get(), mLastCpFsyncDuration.value());
            assertEquals(expLastCpWalRecordFsyncDuration.get(), mLastCpWalRecordFsyncDuration.value());
            assertEquals(expLastCpWriteEntryDuration.get(), mLastCpWriteEntryDuration.value());
            assertEquals(expLastCpSplitAndSortPagesDuration.get(), mLastCpSplitAndSortPagesDuration.value());
            assertEquals(expLastTotalDuration.get(), mLastCpDuration.value());
            assertEquals(expLastcCheckpointStartTime.get(), mLastCpStart.value());
            assertEquals(expPagesSize.get(), mLastCpTotalPages.value());
            assertEquals(expDataPagesWritten.get(), mLastCpDataPages.value());
            assertEquals(expCOWPagesWritten.get(), mLastCpCOWPages.value());
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /** @throws Exception if failed. */
    @Test
    public void testThrottleSpeedBased() throws Exception {
        srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        startCollectStatistics();

        AtomicBoolean run = new AtomicBoolean(true);

        IgniteCache<Long, Long> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridTestUtils.runAsync(() -> {
            for (long i = 0; run.get(); i++)
                cache.put(i, i);
        }, "loader");

        assertTrue(waitForCondition(() -> totalThrottlingTime(srv).value() > 0, TIMEOUT));

        run.set(false);

        AtomicBoolean checker = new AtomicBoolean();

        stopCollectStatisticsAndRead(new TestHandler(){
            @Override public void throttling(long startTime, long endTime) {
                checker.set(true);
            }
        });

        assertTrue(checker.get());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED, value = "TARGET_RATIO_BASED")
    public void testThrottleTargetRatioBased() throws Exception {
        testThrottleSpeedBased();
    }
}

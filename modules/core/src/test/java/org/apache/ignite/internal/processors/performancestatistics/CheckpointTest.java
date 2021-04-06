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
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl.DATAREGION_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests checkpoint performance statistics.
 */
public class CheckpointTest extends AbstractPerformanceStatisticsTest {
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
            .setWriteThrottlingEnabled(true)
            .setCheckpointFrequency(1_000L)
            .setCheckpointThreads(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        listeningLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCheckpoint() throws Exception {
        LogListener node_started = LogListener.matches("reason='node started'")
            .build();

        LogListener lsnr = LogListener.matches("Checkpoint finished")
            .build();

        listeningLog.registerListener(node_started);
        listeningLog.registerListener(lsnr);

        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        // wait for checkpoint to finish on node start
        assertTrue(waitForCondition(node_started::check, TIMEOUT));
        assertTrue(waitForCondition(lsnr::check, TIMEOUT));

        MetricRegistry mreg = srv.context().metric().registry(DATASTORAGE_METRIC_PREFIX);

        AtomicLongMetric mLastBeforeLockDuration = mreg.findMetric("LastCheckpointBeforeLockDuration");
        AtomicLongMetric mLastLockWaitDuration = mreg.findMetric("LastCheckpointLockWaitDuration");
        AtomicLongMetric mLastListenersExecDuration = mreg.findMetric("LastCheckpointListenersExecuteDuration");
        AtomicLongMetric mLastMarcDuration = mreg.findMetric("LastCheckpointMarkDuration");
        AtomicLongMetric mLastLockHoldDuration = mreg.findMetric("LastCheckpointLockHoldDuration");
        AtomicLongMetric mLastPagesWriteDuration = mreg.findMetric("LastCheckpointPagesWriteDuration");
        AtomicLongMetric mLastFsyncDuration = mreg.findMetric("LastCheckpointFsyncDuration");
        AtomicLongMetric mLastWalRecordFsyncDuration = mreg.findMetric("LastCheckpointWalRecordFsyncDuration");
        AtomicLongMetric mLastWriteEntryDuration = mreg.findMetric("LastCheckpointWriteEntryDuration");
        AtomicLongMetric mLastSplitAndSortPagesDuration =
            mreg.findMetric("LastCheckpointSplitAndSortPagesDuration");
        AtomicLongMetric mLastDuration = mreg.findMetric("LastCheckpointDuration");
        AtomicLongMetric mLastStart = mreg.findMetric("LastCheckpointStart");
        AtomicLongMetric mLastTotalPages = mreg.findMetric("LastCheckpointTotalPagesNumber");
        AtomicLongMetric mLastDataPages = mreg.findMetric("LastCheckpointDataPagesNumber");
        AtomicLongMetric mLastCOWPages = mreg.findMetric("LastCheckpointCopiedOnWritePagesNumber");

        startCollectStatistics();

        lsnr = LogListener.matches("Checkpoint finished")
            .build();

        listeningLog.registerListener(lsnr);

        forceCheckpoint();

        assertTrue(waitForCondition(lsnr::check, TIMEOUT));

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)srv.context().cache().context().database();

        AtomicInteger cntr = new AtomicInteger();

        db.checkpointReadLock();

        try {
            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void checkpoint(long beforeLockDuration, long lockWaitDuration,
                    long listenersExecDuration, long markDuration, long lockHoldDuration, long pagesWriteDuration,
                    long fsyncDuration, long walCpRecordFsyncDuration, long writeCpEntryDuration,
                    long splitAndSortCpPagesDuration, long totalDuration, long cpStartTime, int pagesSize,
                    int dataPagesWritten, int cowPagesWritten) {
                    assertEquals(mLastBeforeLockDuration.value(), beforeLockDuration);
                    assertEquals(mLastLockWaitDuration.value(), lockWaitDuration);
                    assertEquals(mLastListenersExecDuration.value(), listenersExecDuration);
                    assertEquals(mLastMarcDuration.value(), markDuration);
                    assertEquals(mLastLockHoldDuration.value(), lockHoldDuration);
                    assertEquals(mLastPagesWriteDuration.value(), pagesWriteDuration);
                    assertEquals(mLastFsyncDuration.value(), fsyncDuration);
                    assertEquals(mLastWalRecordFsyncDuration.value(), walCpRecordFsyncDuration);
                    assertEquals(mLastWriteEntryDuration.value(), writeCpEntryDuration);
                    assertEquals(mLastSplitAndSortPagesDuration.value(), splitAndSortCpPagesDuration);
                    assertEquals(mLastDuration.value(), totalDuration);
                    assertEquals(mLastStart.value(), cpStartTime);
                    assertEquals(mLastTotalPages.value(), pagesSize);
                    assertEquals(mLastDataPages.value(), dataPagesWritten);
                    assertEquals(mLastCOWPages.value(), cowPagesWritten);

                    cntr.incrementAndGet();
                }
            });

            assertEquals(1, cntr.get());
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /** @throws Exception if failed. */
    @Test
    public void testThrottleSpeedBased() throws Exception {
        checkThrottling();
    }

    /** @throws Exception if failed. */
    @Test
    @WithSystemProperty(key = IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED, value = "TARGET_RATIO_BASED")
    public void testThrottleTargetRatioBased() throws Exception {
        checkThrottling();
    }

    /** @throws Exception if failed. */
    public void checkThrottling() throws Exception {
        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        startCollectStatistics();

        AtomicBoolean run = new AtomicBoolean(true);

        IgniteCache<Long, Long> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        MetricRegistry mreg = srv.context().metric().registry(
            metricName(DATAREGION_METRICS_PREFIX, DFLT_DATA_REG_DEFAULT_NAME));

        LongAdderMetric totalThrottlingTime = mreg.findMetric("TotalThrottlingTime");

        GridTestUtils.runAsync(() -> {
            for (long i = 0; run.get(); i++)
                cache.put(i, i);
        });

        assertTrue(waitForCondition(() -> totalThrottlingTime.value() > 0, TIMEOUT));

        run.set(false);

        AtomicBoolean checker = new AtomicBoolean();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void pagesWriteThrottle(long startTime, long endTime) {
                checker.set(true);
            }
        });

        assertTrue(checker.get());
    }
}

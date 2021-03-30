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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;

/**
 * Tests checkpoint performance statistics.
 */
public class CheckpointTest extends AbstractPerformanceStatisticsTest {
    /** Ignite. */
    private static IgniteEx srv;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setCheckpointFrequency(10 * 1000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCheckpoint() throws Exception {
        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        srv.cluster().state(ClusterState.INACTIVE);

        AtomicBoolean checked = new AtomicBoolean(false);

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void checkpoint(long beforeLockDuration, long lockWaitDuration, long listenersExecDuration,
                long markDuration, long lockHoldDuration, long pagesWriteDuration, long fsyncDuration,
                long walCpRecordFsyncDuration, long writeCheckpointEntryDuration, long splitAndSortCpPagesDuration,
                long totalDuration, long checkpointStartTime, int pagesSize, int dataPagesWritten, int cowPagesWritten) {
                checked.set(true);
            }
        });

        assertTrue(checked.get());
    }
    /** @throws Exception If failed. */
    @Test
    public void testCheckpoint2() throws Exception {
        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        MetricRegistry mreg = srv.context().metric().registry(DATASTORAGE_METRIC_PREFIX);

        AtomicLongMetric lastCpBeforeLockDuration = mreg.findMetric("LastCheckpointBeforeLockDuration");
        AtomicLongMetric lastCpLockWaitDuration = mreg.findMetric("LastCheckpointLockWaitDuration");
        AtomicLongMetric lastCpListenersExecuteDuration = mreg.findMetric("LastCheckpointListenersExecuteDuration");

        AtomicLongMetric lastCpMarcDuration = mreg.findMetric("LastCheckpointMarkDuration");
        AtomicLongMetric lastCpLockHoldDuration = mreg.findMetric("LastCheckpointLockHoldDuration");
        AtomicLongMetric lastCpPagesWriteDuration = mreg.findMetric("LastCheckpointPagesWriteDuration");
        AtomicLongMetric lastCpFsyncDuration = mreg.findMetric("LastCheckpointFsyncDuration");

        AtomicLongMetric lastCpWalRecordFsyncDuration = mreg.findMetric("LastCheckpointWalRecordFsyncDuration");
        AtomicLongMetric lastCpWriteEntryDuration = mreg.findMetric("LastCheckpointWriteEntryDuration");
        AtomicLongMetric lastCpSplitAndSortPagesDuration =
            mreg.findMetric("LastCheckpointSplitAndSortPagesDuration");

        AtomicLongMetric lastCpDuration = mreg.findMetric("LastCheckpointDuration");
        AtomicLongMetric lastCpStart = mreg.findMetric("LastCheckpointStart");
        AtomicLongMetric lastCpTotalPages = mreg.findMetric("LastCheckpointTotalPagesNumber");
        AtomicLongMetric lastCpDataPages = mreg.findMetric("LastCheckpointDataPagesNumber");
        AtomicLongMetric lastCpCOWPages = mreg.findMetric("LastCheckpointCopiedOnWritePagesNumber");

//        srv.cluster().state(ClusterState.INACTIVE);

        TimeUnit.SECONDS.sleep(15);

        AtomicBoolean checked = new AtomicBoolean(false);

        AtomicLong expLastCpBeforeLockDuration = new AtomicLong();
        AtomicLong expLastCpLockWaitDuration = new AtomicLong();
        AtomicLong expLastCpListenersExecuteDuration = new AtomicLong();

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
            @Override public void checkpoint(long beforeLockDuration, long lockWaitDuration, long listenersExecDuration,
                long markDuration, long lockHoldDuration, long pagesWriteDuration, long fsyncDuration,
                long walCpRecordFsyncDuration, long writeCheckpointEntryDuration, long splitAndSortCpPagesDuration,
                long totalDuration, long checkpointStartTime, int pagesSize, int dataPagesWritten, int cowPagesWritten) {
                expLastCpBeforeLockDuration.set(beforeLockDuration);
                expLastCpLockWaitDuration.set(lockWaitDuration);
                expLastCpListenersExecuteDuration.set(listenersExecDuration);

                expLastCpMarkDuration.set(markDuration);
                expLastCpLockHoldDuration.set(lockHoldDuration);
                expLastCpPagesWriteDuration.set(pagesWriteDuration);
                expLastCpFsyncDuration.set(fsyncDuration);

                expLastCpWalRecordFsyncDuration.set(walCpRecordFsyncDuration);
                expLastCpWriteEntryDuration.set(writeCheckpointEntryDuration);
                expLastCpSplitAndSortPagesDuration.set(splitAndSortCpPagesDuration);

                expLastTotalDuration.set(totalDuration);
                expLastcCheckpointStartTime.set(checkpointStartTime);
                expPagesSize.set(pagesSize);
                expDataPagesWritten.set(dataPagesWritten);
                expCOWPagesWritten.set(cowPagesWritten);

                checked.set(true);
            }
        });

        assertTrue(checked.get());

        assertEquals(expLastCpBeforeLockDuration.get(), lastCpBeforeLockDuration.value());
        assertEquals(expLastCpLockWaitDuration.get(), lastCpLockWaitDuration.value());
        assertEquals(expLastCpListenersExecuteDuration.get(), lastCpListenersExecuteDuration.value());

        assertEquals(expLastCpMarkDuration.get(), lastCpMarcDuration.value());
        assertEquals(expLastCpLockHoldDuration.get(), lastCpLockHoldDuration.value());
        assertEquals(expLastCpPagesWriteDuration.get(), lastCpPagesWriteDuration.value());
        assertEquals(expLastCpFsyncDuration.get(), lastCpFsyncDuration.value());

        assertEquals(expLastCpWalRecordFsyncDuration.get(), lastCpWalRecordFsyncDuration.value());
        assertEquals(expLastCpWriteEntryDuration.get(), lastCpWriteEntryDuration.value());
        assertEquals(expLastCpSplitAndSortPagesDuration.get(), lastCpSplitAndSortPagesDuration.value());

        assertEquals(expLastTotalDuration.get(), lastCpDuration.value());
        assertEquals(expLastcCheckpointStartTime.get(), lastCpStart.value());
        assertEquals(expPagesSize.get(), lastCpTotalPages.value());
        assertEquals(expDataPagesWritten.get(), lastCpDataPages.value());
        assertEquals(expCOWPagesWritten.get(), lastCpCOWPages.value());
    }
}

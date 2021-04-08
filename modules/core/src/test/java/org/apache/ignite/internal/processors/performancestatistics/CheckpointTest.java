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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
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

    /** Slow checkpoint enabled. */
    private static final AtomicBoolean slowCheckpointEnabled = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningLog);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(15 * 1024 * 1024)
                .setCheckpointPageBufferSize(1024 * 1024)
                .setMetricsEnabled(true)
                .setPersistenceEnabled(true))
            .setWriteThrottlingEnabled(true)
            .setFileIOFactory(new SlowCheckpointFileIOFactory())
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
                @Override public void checkpoint(UUID nodeId, long beforeLockDuration, long lockWaitDuration,
                    long listenersExecDuration, long markDuration, long lockHoldDuration, long pagesWriteDuration,
                    long fsyncDuration, long walCpRecordFsyncDuration, long writeCpEntryDuration,
                    long splitAndSortCpPagesDuration, long totalDuration, long cpStartTime, int pagesSize,
                    int dataPagesWritten, int cowPagesWritten) {
                    assertEquals(srv.localNode().id(), nodeId);
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

        MetricRegistry mreg = srv.context().metric().registry(
            metricName(DATAREGION_METRICS_PREFIX, DFLT_DATA_REG_DEFAULT_NAME));

        LongAdderMetric totalThrottlingTime = mreg.findMetric("TotalThrottlingTime");

        IgniteCache<Long, Long> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        long keysCnt = 1024L;

        slowCheckpointEnabled.set(true);

        try {
            GridTestUtils.runAsync(() -> {
                while (slowCheckpointEnabled.get()) {
                    long l = ThreadLocalRandom.current().nextLong(keysCnt);

                    cache.put(l, l);
                }
            });

            assertTrue(waitForCondition(() -> totalThrottlingTime.value() > 0, TIMEOUT));
        }
        finally {
            slowCheckpointEnabled.set(false);
        }

        stopCollectStatistics();

        long now = System.currentTimeMillis();

        AtomicBoolean checker = new AtomicBoolean();

        readFiles(statisticsFiles(), new TestHandler() {
            @Override public void pagesWriteThrottle(UUID nodeId, long startTime, long endTime) {
                assertEquals(srv.localNode().id(), nodeId);

                assertTrue(0L < startTime && startTime <= endTime && endTime < now);

                checker.set(true);
            }
        });

        assertTrue(checker.get());
    }

    /**
    * Create File I/O that emulates poor checkpoint write speed.
    */
    private static class SlowCheckpointFileIOFactory implements FileIOFactory {
        /** Default checkpoint park nanos. */
        private static final int CHECKPOINT_PARK_NANOS = 50_000_000;

        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(java.io.File file, OpenOption... openOption) throws IOException {
            final FileIO delegate = delegateFactory.create(file, openOption);

            return new FileIODecorator(delegate) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    parkIfNeeded();

                    return delegate.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    parkIfNeeded();

                    return delegate.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    parkIfNeeded();

                    return delegate.write(buf, off, len);
                }

                /**
                 * Parks current checkpoint thread if slow mode is enabled.
                 */
                private void parkIfNeeded() {
                    if (slowCheckpointEnabled.get() && Thread.currentThread().getName().contains("checkpoint"))
                        LockSupport.parkNanos(CHECKPOINT_PARK_NANOS);
                }
            };
        }
    }
}

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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowCheckpointMetadataFileIOFactory;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
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
    /** Slow checkpoint enabled. */
    private static final AtomicBoolean slowCheckpointEnabled = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(10 * 1024 * 1024)
                .setCheckpointPageBufferSize(1024 * 1024)
                .setMetricsEnabled(true)
                .setPersistenceEnabled(true))
            .setWriteThrottlingEnabled(true)
            .setFileIOFactory(new SlowCheckpointMetadataFileIOFactory(slowCheckpointEnabled, 500_000))
            .setCheckpointThreads(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        slowCheckpointEnabled.set(false);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCheckpoint() throws Exception {
        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        MetricRegistry mreg = srv.context().metric().registry(DATASTORAGE_METRIC_PREFIX);

        LongMetric lastStart = mreg.findMetric("LastCheckpointStart");

        // Wait for checkpoint to finish on node start.
        assertTrue(waitForCondition(() -> 0 < lastStart.value(), TIMEOUT));

        lastStart.reset();

        startCollectStatistics();

        forceCheckpoint();

        assertTrue(waitForCondition(() -> 0 < lastStart.value(), TIMEOUT));

        AtomicInteger cnt = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void checkpoint(
                UUID nodeId,
                long beforeLockDuration,
                long lockWaitDuration,
                long listenersExecDuration,
                long markDuration,
                long lockHoldDuration,
                long pagesWriteDuration,
                long fsyncDuration,
                long walCpRecordFsyncDuration,
                long writeCpEntryDuration,
                long splitAndSortCpPagesDuration,
                long totalDuration,
                long cpStartTime,
                int pagesSize,
                int dataPagesWritten,
                int cowPagesWritten
            ) {
                assertEquals(srv.localNode().id(), nodeId);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointBeforeLockDuration").value(),
                    beforeLockDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointLockWaitDuration").value(),
                    lockWaitDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointListenersExecuteDuration").value(),
                    listenersExecDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointMarkDuration").value(), markDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointLockHoldDuration").value(),
                    lockHoldDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointPagesWriteDuration").value(),
                    pagesWriteDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointFsyncDuration").value(), fsyncDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointWalRecordFsyncDuration").value(),
                    walCpRecordFsyncDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointWriteEntryDuration").value(),
                    writeCpEntryDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointSplitAndSortPagesDuration").value(),
                    splitAndSortCpPagesDuration);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointDuration").value(), totalDuration);
                assertEquals(lastStart.value(), cpStartTime);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointTotalPagesNumber").value(), pagesSize);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointDataPagesNumber").value(),
                    dataPagesWritten);
                assertEquals(mreg.<LongMetric>findMetric("LastCheckpointCopiedOnWritePagesNumber").value(),
                    cowPagesWritten);

                cnt.incrementAndGet();
            }
        });

        assertEquals(1, cnt.get());
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

        IgniteCache<Long, Long> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        long start = U.currentTimeMillis();

        MetricRegistry mreg = srv.context().metric().registry(
            metricName(DATAREGION_METRICS_PREFIX, DFLT_DATA_REG_DEFAULT_NAME));

        LongAdderMetric totalThrottlingTime = mreg.findMetric("TotalThrottlingTime");

        startCollectStatistics();

        AtomicBoolean stop = new AtomicBoolean();

        slowCheckpointEnabled.set(true);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get())
                cache.put(ThreadLocalRandom.current().nextLong(1024), ThreadLocalRandom.current().nextLong());
        });

        assertTrue(waitForCondition(() -> 0 < totalThrottlingTime.value(), TIMEOUT));

        stop.set(true);

        AtomicInteger cnt = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void pagesWriteThrottle(UUID nodeId, long endTime, long duration) {
                assertEquals(srv.localNode().id(), nodeId);

                assertTrue(start <= endTime);
                assertTrue(duration >= 0);

                cnt.incrementAndGet();
            }
        });

        assertTrue(cnt.get() > 0);

        fut.get(TIMEOUT);
    }
}

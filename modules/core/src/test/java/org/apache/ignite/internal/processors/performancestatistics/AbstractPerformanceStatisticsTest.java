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

import java.io.File;
import java.lang.management.ThreadInfo;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.PerformanceStatisticsMBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.WRITER_THREAD_NAME;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Ignite performance statistics abstract test.
 */
public abstract class AbstractPerformanceStatisticsTest extends GridCommonAbstractTest {
    /** */
    public static final long TIMEOUT = 30_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPerformanceStatisticsDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPerformanceStatisticsDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPerformanceStatisticsDir();
    }

    /** Cleans performance statistics directory. */
    public static void cleanPerformanceStatisticsDir() throws Exception {
        U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, true);
    }

    /** Starts collecting performance statistics. */
    public static void startCollectStatistics() throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        statisticsMBean(grids.get(0).name()).start();

        waitForStatisticsEnabled(true);
    }

    /** Rotate file collecting performance statistics. */
    public static void rotateCollectStatistics() throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        statisticsMBean(grids.get(0).name()).rotate();
    }

    /** Stops collecting performance statistics. */
    public static void stopCollectStatistics() throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        statisticsMBean(grids.get(0).name()).stop();

        waitForStatisticsEnabled(false);
    }

    /** Stops and reads collecting performance statistics. */
    public static void stopCollectStatisticsAndRead(TestHandler... handlers) throws Exception {
        stopCollectStatistics();

        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

        readFiles(singletonList(dir), handlers);
    }

    /** Reads collecting performance statistics files. */
    public static void readFiles(List<File> files, TestHandler... handlers) throws Exception {
        new FilePerformanceStatisticsReader(handlers).read(files);
    }

    /**
     * @param files Performance statistics files.
     */
    File getMainStatisticsFile(List<File> files) {
        File file = files.stream()
            .filter(file1 -> file1.getName()
                .matches("node-" + nodeId(0) + ".prf"))
            .findFirst().orElse(null);

        assertNotNull(file);

        return file;
    }

    /** Wait for statistics started/stopped in the cluster. */
    public static void waitForStatisticsEnabled(boolean performanceStatsEnabled) throws Exception {
        assertTrue(waitForCondition(() -> {
            List<Ignite> grids = G.allGrids();

            for (Ignite grid : grids)
                if (performanceStatsEnabled != statisticsMBean(grid.name()).started())
                    return false;

            // Make sure that writer flushed data and stopped.
            if (!performanceStatsEnabled) {
                for (long id : U.getThreadMx().getAllThreadIds()) {
                    ThreadInfo info = U.getThreadMx().getThreadInfo(id);

                    if (info != null && info.getThreadState() != Thread.State.TERMINATED &&
                        info.getThreadName().startsWith(WRITER_THREAD_NAME))
                        return false;
                }
            }

            return true;
        }, TIMEOUT));
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Ignite performance statistics MBean.
     */
    protected static PerformanceStatisticsMBean statisticsMBean(String igniteInstanceName) {
        return getMxBean(igniteInstanceName, "PerformanceStatistics", PerformanceStatisticsMBeanImpl.class,
            PerformanceStatisticsMBean.class);
    }

    /** @return Performance statistics files. */
    public static List<File> statisticsFiles() throws Exception {
        File perfStatDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

        return FilePerformanceStatisticsReader.resolveFiles(singletonList(perfStatDir));
    }

    /** Test performance statistics handler. */
    public static class TestHandler implements PerformanceStatisticsHandler {
        /** {@inheritDoc} */
        @Override public void cacheStart(UUID nodeId, int cacheId, String name) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
            long duration) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
            boolean commited) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
            long duration, boolean success) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
            long logicalReads, long physicalReads) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void queryRows(UUID nodeId, GridCacheQueryType type, UUID qryNodeId, long id, String action,
            long rows) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void queryProperty(UUID nodeId, GridCacheQueryType type, UUID qryNodeId, long id, String name,
            String val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
            int affPartId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
            boolean timedOut) {
            // No-op.
        }

        /** {@inheritDoc} */
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
            long recoveryDataWriteDuration,
            long totalDuration,
            long cpStartTime,
            int pagesSize,
            int dataPagesWritten,
            int cowPagesWritten
        ) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void pagesWriteThrottle(UUID nodeId, long endTime, long duration) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void systemView(UUID id, String name, List<String> schema, List<Object> row) {
            // No-op.
        }
    }

    /** Client type to run load from. */
    enum ClientType {
        /** Server node. */
        SERVER,

        /** Client node. */
        CLIENT,

        /** Thin client. */
        THIN_CLIENT;
    }
}

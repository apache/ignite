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

package org.apache.ignite.internal.processors.jobmetrics;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JOBS_METRICS_CONCURRENCY_LEVEL;

/**
 * Processes job metrics.
 */
public class GridJobMetricsProcessor extends GridProcessorAdapter {
    /** */
    private static final int CONCURRENCY_LEVEL = Integer.getInteger(IGNITE_JOBS_METRICS_CONCURRENCY_LEVEL, 64);

    /** Time to live. */
    private final long expireTime;

    /** Maximum size. */
    private final int histSize;

    /** */
    private final int queSize;

    /** */
    private volatile long idleTimer = U.currentTimeMillis();

    /** */
    private final AtomicBoolean isIdle = new AtomicBoolean(true);

    /** */
    private volatile InternalMetrics metrics;

    /**
     * @param ctx Grid kernal context.
     */
    public GridJobMetricsProcessor(GridKernalContext ctx) {
        super(ctx);

        expireTime = ctx.config().getMetricsExpireTime();
        histSize = ctx.config().getMetricsHistorySize();

        assert histSize > 0 : histSize;

        final int minSize = histSize / CONCURRENCY_LEVEL + 1;
        int size = 1;

        // Need to find power of 2 size.
        while (size < minSize)
            size <<= 1;

        queSize = size;

        reset();
    }

    /**
     * Internal metrics object for atomic replacement.
     */
    private class InternalMetrics {
        /** */
        private volatile long totalIdleTime;

        /** */
        private volatile long curIdleTime;

        /** */
        private final SnapshotsQueue[] snapshotsQueues;

        /**
         *
         */
        InternalMetrics() {
            if (CONCURRENCY_LEVEL < 0)
                snapshotsQueues = null;
            else {
                snapshotsQueues = new SnapshotsQueue[CONCURRENCY_LEVEL];

                for (int i = 0; i < snapshotsQueues.length; i++)
                    snapshotsQueues[i] = new SnapshotsQueue(queSize);
            }
        }
    }

    /**
     * Resets metrics.
     */
    public void reset() {
        metrics = new InternalMetrics();
    }

    /**
     * Gets metrics history size.
     *
     * @return Maximum metrics queue size.
     */
    int getHistorySize() {
        return histSize;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        assertParameter(histSize > 0, "metricsHistorySize > 0");
        assertParameter(expireTime > 0, "metricsExpireTime > 0");

        if (metrics.snapshotsQueues == null)
            throw new IgniteCheckedException("Invalid concurrency level configured " +
                "(is 'IGNITE_JOBS_METRICS_CONCURRENCY_LEVEL' system property properly set?).");

        if (log.isDebugEnabled())
            log.debug("Job metrics processor started [histSize=" + histSize +
                ", concurLvl=" + CONCURRENCY_LEVEL +
                ", expireTime=" + expireTime + ']');
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Job metrics processor stopped.");
    }

    /**
     * Gets latest metrics.
     *
     * @return Latest metrics.
     */
    public GridJobMetrics getJobMetrics() {
        long now = U.currentTimeMillis();

        SnapshotReducer rdc = new SnapshotReducer();

        InternalMetrics im = metrics;

        for (SnapshotsQueue q : im.snapshotsQueues)
            q.reduce(rdc, now);

        GridJobMetrics m = rdc.reduce();

        // Set idle times.
        m.setCurrentIdleTime(im.curIdleTime);
        m.setTotalIdleTime(im.totalIdleTime);

        return m;
    }

    /**
     * @param metrics New metrics.
     */
    public void addSnapshot(GridJobMetricsSnapshot metrics) {
        assert metrics != null;

        InternalMetrics m = this.metrics;

        m.snapshotsQueues[ThreadLocalRandom8.current().nextInt(m.snapshotsQueues.length)].add(metrics);

        // Handle current and total idle times.
        long idleTimer0 = idleTimer;

        if (metrics.getActiveJobs() > 0) {
            if (isIdle.get() && isIdle.compareAndSet(true, false)) {
                long now = U.currentTimeMillis();

                // Node started to execute jobs after being idle.
                m.totalIdleTime += now - idleTimer0;

                m.curIdleTime = 0;
            }
        }
        else {
            long now = U.currentTimeMillis();

            if (!isIdle.compareAndSet(false, true)) {
                // Node is still idle.
                m.curIdleTime += now - idleTimer0;

                m.totalIdleTime += now - idleTimer0;
            }

            // Reset timer.
            idleTimer = now;
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Job metrics processor processor memory stats [grid=" + ctx.gridName() + ']');
    }

    /**
     *
     */
    private class SnapshotsQueue {
        /** */
        private int idx;

        /** */
        private final GridJobMetricsSnapshot[] snapshots;

        /** */
        private final int mask;

        /** */
        private int totalFinishedJobs;

        /** */
        private int totalCancelledJobs;

        /** */
        private int totalRejectedJobs;

        /**
         * @param size Size (should be power of 2).
         */
        private SnapshotsQueue(int size) {
            assert size > 0 : size;

            snapshots = new GridJobMetricsSnapshot[size];

            mask = size - 1;
        }

        /**
         * @param s Snapshot to add.
         */
        synchronized void add(GridJobMetricsSnapshot s) {
            snapshots[idx++ & mask] = s;

            totalFinishedJobs += s.getFinishedJobs();
            totalCancelledJobs += s.getCancelJobs();
            totalRejectedJobs += s.getRejectJobs();
        }

        /**
         * @param rdc Reducer.
         * @param now Timestamp.
         */
        synchronized void reduce(SnapshotReducer rdc, long now) {
            assert rdc != null;

            for (GridJobMetricsSnapshot s : snapshots) {
                if (s == null)
                    break;

                if (now - s.getTimestamp() > expireTime)
                    continue;

                rdc.collect(s);
            }

            rdc.collectTotals(totalFinishedJobs, totalCancelledJobs, totalRejectedJobs);
        }
    }

    /**
     *
     */
    private static class SnapshotReducer implements IgniteReducer<GridJobMetricsSnapshot, GridJobMetrics> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridJobMetrics m = new GridJobMetrics();

        /** */
        private int cnt;

        /** */
        private int totalActiveJobs;

        /** */
        private int totalWaitingJobs;

        /** */
        private int totalStartedJobs;

        /** */
        private int totalCancelledJobs;

        /** */
        private int totalRejectedJobs;

        /** */
        private int totalFinishedJobs;

        /** */
        private long totalExecTime;

        /** */
        private long totalWaitTime;

        /** */
        private double totalCpuLoad;

        /** */
        private GridJobMetricsSnapshot lastSnapshot;

        /**
         * {@inheritDoc}
         */
        @Override public boolean collect(GridJobMetricsSnapshot s) {
            assert s != null;

            cnt++;

            if (lastSnapshot == null || lastSnapshot.getTimestamp() < s.getTimestamp())
                lastSnapshot = s;

            // Maximums.
            if (m.getMaximumActiveJobs() < s.getActiveJobs())
                m.setMaximumActiveJobs(s.getActiveJobs());

            if (m.getMaximumWaitingJobs() < s.getPassiveJobs())
                m.setMaximumWaitingJobs(s.getPassiveJobs());

            if (m.getMaximumCancelledJobs() < s.getCancelJobs())
                m.setMaximumCancelledJobs(s.getCancelJobs());

            if (m.getMaximumRejectedJobs() < s.getRejectJobs())
                m.setMaximumRejectedJobs(s.getRejectJobs());

            if (m.getMaximumJobWaitTime() < s.getMaximumWaitTime())
                m.setMaximumJobWaitTime(s.getMaximumWaitTime());

            if (m.getMaximumJobExecuteTime() < s.getMaximumExecutionTime())
                m.setMaxJobExecutionTime(s.getMaximumExecutionTime());

            // Totals.
            totalActiveJobs += s.getActiveJobs();
            totalCancelledJobs += s.getCancelJobs();
            totalWaitingJobs += s.getPassiveJobs();
            totalRejectedJobs += s.getRejectJobs();
            totalWaitTime += s.getWaitTime();
            totalExecTime += s.getExecutionTime();
            totalStartedJobs += s.getStartedJobs();
            totalFinishedJobs += s.getFinishedJobs();
            totalCpuLoad += s.getCpuLoad();

            return true;
        }

        /**
         * @param totalFinishedJobs Finished jobs.
         * @param totalCancelledJobs Cancelled jobs.
         * @param totalRejectedJobs Rejected jobs.
         */
        void collectTotals(int totalFinishedJobs, int totalCancelledJobs, int totalRejectedJobs) {
            // Totals.
            m.setTotalExecutedJobs(m.getTotalExecutedJobs() + totalFinishedJobs);
            m.setTotalCancelledJobs(m.getTotalCancelledJobs() + totalCancelledJobs);
            m.setTotalRejectedJobs(m.getTotalRejectedJobs() + totalRejectedJobs);
        }

        /** {@inheritDoc} */
        @Override public GridJobMetrics reduce() {
            // Current metrics.
            if (lastSnapshot != null) {
                m.setCurrentActiveJobs(lastSnapshot.getActiveJobs());
                m.setCurrentWaitingJobs(lastSnapshot.getPassiveJobs());
                m.setCurrentCancelledJobs(lastSnapshot.getCancelJobs());
                m.setCurrentRejectedJobs(lastSnapshot.getRejectJobs());
                m.setCurrentJobExecutionTime(lastSnapshot.getMaximumExecutionTime());
                m.setCurrentJobWaitTime(lastSnapshot.getMaximumWaitTime());
            }

            // Averages.
            if (cnt > 0) {
                m.setAverageActiveJobs((float)totalActiveJobs / cnt);
                m.setAverageWaitingJobs((float)totalWaitingJobs / cnt);
                m.setAverageCancelledJobs((float)totalCancelledJobs / cnt);
                m.setAverageRejectedJobs((float)totalRejectedJobs / cnt);
                m.setAverageCpuLoad(totalCpuLoad / cnt);
            }

            m.setAverageJobExecutionTime(totalFinishedJobs > 0 ? (double)totalExecTime / totalFinishedJobs : 0);
            m.setAverageJobWaitTime(totalStartedJobs > 0 ? (double)totalWaitTime / totalStartedJobs : 0);

            return m;
        }
    }
}
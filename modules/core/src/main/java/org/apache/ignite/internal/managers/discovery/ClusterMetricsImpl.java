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

package org.apache.ignite.internal.managers.discovery;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetrics;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.internal.managers.communication.GridIoManager.COMM_METRICS;
import static org.apache.ignite.internal.managers.communication.GridIoManager.OUTBOUND_MSG_QUEUE_CNT;
import static org.apache.ignite.internal.managers.communication.GridIoManager.RCVD_BYTES_CNT;
import static org.apache.ignite.internal.managers.communication.GridIoManager.RCVD_MSGS_CNT;
import static org.apache.ignite.internal.managers.communication.GridIoManager.SENT_BYTES_CNT;
import static org.apache.ignite.internal.managers.communication.GridIoManager.SENT_MSG_CNT;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager.LAST_DATA_VER;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.DAEMON_THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PEAK_THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_DURATION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TOTAL_STARTED_THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.UP_TIME;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TOTAL_EXEC_TASKS;

/**
 * Cluster metrics proxy.
 */
public class ClusterMetricsImpl implements ClusterMetrics {
    /** */
    private static final RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

    /** Job metrics. */
    private volatile GridJobMetrics jobMetrics;

    /** Job metrics update time. */
    private volatile long jobMetricsUpdateTime;

    /** Job metrics mutex. */
    private final Object jobMetricsMux = new Object();

    /** Kernel context. */
    private final GridKernalContext ctx;

    /** Node start time. */
    private final long nodeStartTime;

    /** Total executed tasks metric. */
    private final LongMetric execTasks;

    /** GC CPU load. */
    private final DoubleMetric gcCpuLoad;

    /** CPU load. */
    private final DoubleMetric cpuLoad;

    /** Up time. */
    private final LongMetric upTime;

    /** Available processors count. */
    private final int availableProcessors;

    /**
     * Metric reflecting heap {@link MemoryUsage#getInit()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric heapInit;

    /**
     * Metric reflecting heap {@link MemoryUsage#getUsed()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric heapUsed;

    /**
     * Metric reflecting heap {@link MemoryUsage#getCommitted()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric heapCommitted;

    /**
     * Metric reflecting heap {@link MemoryUsage#getMax()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric heapMax;

    /**
     * Metric reflecting heap {@link MemoryUsage#getInit()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric nonHeapInit;

    /**
     * Metric reflecting heap {@link MemoryUsage#getUsed()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric nonHeapUsed;

    /**
     * Metric reflecting heap {@link MemoryUsage#getCommitted()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric nonHeapCommitted;

    /**
     * Metric reflecting heap {@link MemoryUsage#getMax()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric nonHeapMax;

    /**
     * Metric reflecting {@link ThreadMXBean#getThreadCount()}.
     *
     * @see GridMetricManager
     */
    private final IntMetric threadCnt;

    /** Last data version metric. */
    private final LongMetric lastDataVer;

    /** Sent messages count metric. */
    private final IntMetric sentMsgsCnt;

    /** Sent bytes count metric. */
    private final LongMetric sentBytesCnt;

    /** Received messages count metric. */
    private final IntMetric rcvdMsgsCnt;

    /** Received bytes count metric. */
    private final LongMetric rcvdBytesCnt;

    /** Outbound message queue size metric. */
    private final IntMetric outboundMsgCnt;

    /**
     * Metric reflecting {@link ThreadMXBean#getPeakThreadCount()}.
     *
     * @see GridMetricManager
     */
    private final IntMetric peakThreadCnt;

    /**
     * Metric reflecting {@link ThreadMXBean#getTotalStartedThreadCount()}.
     *
     * @see GridMetricManager
     */
    private final LongMetric totalStartedThreadCnt;

    /**
     * Metric reflecting {@link ThreadMXBean#getDaemonThreadCount()}}.
     *
     * @see GridMetricManager
     */
    private final IntMetric daemonThreadCnt;

    /** Current PME duration in milliseconds. */
    private final LongMetric pmeDuration;

    /**
     * @param ctx Kernel context.
     * @param nodeStartTime Node start time.
     */
    public ClusterMetricsImpl(GridKernalContext ctx, long nodeStartTime) {
        this.ctx = ctx;
        this.nodeStartTime = nodeStartTime;

        MetricRegistry mreg = ctx.metric().registry(SYS_METRICS);

        gcCpuLoad = mreg.findMetric(GC_CPU_LOAD);
        cpuLoad = mreg.findMetric(CPU_LOAD);
        upTime = mreg.findMetric(UP_TIME);

        threadCnt = mreg.findMetric(THREAD_CNT);
        peakThreadCnt = mreg.findMetric(PEAK_THREAD_CNT);
        totalStartedThreadCnt = mreg.findMetric(TOTAL_STARTED_THREAD_CNT);
        daemonThreadCnt = mreg.findMetric(DAEMON_THREAD_CNT);
        execTasks = mreg.findMetric(TOTAL_EXEC_TASKS);

        availableProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

        heapInit = mreg.findMetric(metricName("memory", "heap", "init"));
        heapUsed = mreg.findMetric(metricName("memory", "heap", "used"));
        heapCommitted = mreg.findMetric(metricName("memory", "heap", "committed"));
        heapMax = mreg.findMetric(metricName("memory", "heap", "max"));

        nonHeapInit = mreg.findMetric(metricName("memory", "nonheap", "init"));
        nonHeapUsed = mreg.findMetric(metricName("memory", "nonheap", "used"));
        nonHeapCommitted = mreg.findMetric(metricName("memory", "nonheap", "committed"));
        nonHeapMax = mreg.findMetric(metricName("memory", "nonheap", "max"));

        MetricRegistry pmeReg = ctx.metric().registry(PME_METRICS);

        pmeDuration = pmeReg.findMetric(PME_DURATION);

        lastDataVer = ctx.metric().registry(CACHE_METRICS).findMetric(LAST_DATA_VER);

        MetricRegistry ioReg = ctx.metric().registry(COMM_METRICS);

        sentMsgsCnt = ioReg.findMetric(SENT_MSG_CNT);
        sentBytesCnt = ioReg.findMetric(SENT_BYTES_CNT);
        rcvdMsgsCnt = ioReg.findMetric(RCVD_MSGS_CNT);
        rcvdBytesCnt = ioReg.findMetric(RCVD_BYTES_CNT);
        outboundMsgCnt = ioReg.findMetric(OUTBOUND_MSG_QUEUE_CNT);
    }

    /** {@inheritDoc} */
    @Override public long getLastUpdateTime() {
        return jobMetricsUpdateTime == 0 ? U.currentTimeMillis() : jobMetricsUpdateTime;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveJobs() {
        return jobMetrics().getMaximumActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobs() {
        return jobMetrics().getCurrentActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageActiveJobs() {
        return jobMetrics().getAverageActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumWaitingJobs() {
        return jobMetrics().getMaximumWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitingJobs() {
        return jobMetrics().getCurrentWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageWaitingJobs() {
        return jobMetrics().getAverageWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return jobMetrics().getMaximumRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return jobMetrics().getCurrentRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return jobMetrics().getAverageRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return jobMetrics().getTotalRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCancelledJobs() {
        return jobMetrics().getMaximumCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentCancelledJobs() {
        return jobMetrics().getCurrentCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageCancelledJobs() {
        return jobMetrics().getAverageCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalCancelledJobs() {
        return jobMetrics().getTotalCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedJobs() {
        return jobMetrics().getTotalExecutedJobs();
    }

    /** {@inheritDoc} */
    @Override public long getTotalJobsExecutionTime() {
        return jobMetrics().getTotalJobsExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobWaitTime() {
        return jobMetrics().getMaximumJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobWaitTime() {
        return jobMetrics().getCurrentJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobWaitTime() {
        return jobMetrics().getAverageJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobExecuteTime() {
        return jobMetrics().getMaximumJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobExecuteTime() {
        return jobMetrics().getCurrentJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobExecuteTime() {
        return jobMetrics().getAverageJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedTasks() {
        return (int)execTasks.value();
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return getUpTime() - getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getTotalIdleTime() {
        return jobMetrics().getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentIdleTime() {
        return jobMetrics().getCurrentIdleTime();
    }

    /** {@inheritDoc} */
    @Override public float getBusyTimePercentage() {
        return 1 - getIdleTimePercentage();
    }

    /** {@inheritDoc} */
    @Override public float getIdleTimePercentage() {
        return getTotalIdleTime() / (float)getUpTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalCpus() {
        return availableProcessors;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return cpuLoad.value();
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return jobMetrics().getAverageCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return gcCpuLoad.value();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return heapInit.value();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return heapUsed.value();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return heapCommitted.value();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return heapMax.value();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return heapMax.value();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return nonHeapInit.value();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        Collection<GridCacheAdapter<?, ?>> caches = ctx.cache().internalCaches();

        long nonHeapUsed = this.nonHeapUsed.value();

        for (GridCacheAdapter<?, ?> cache : caches)
            if (cache.context().statisticsEnabled() && cache.context().started()
                && cache.context().affinity().affinityTopologyVersion().topologyVersion() > 0)
                nonHeapUsed += cache.metrics0().getOffHeapAllocatedSize();

        return nonHeapUsed;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return nonHeapCommitted.value();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return nonHeapMax.value();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        return nonHeapMax.value();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return upTime.value();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return rt.getStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return nodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return threadCnt.value();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return peakThreadCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return totalStartedThreadCnt.value();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return daemonThreadCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return lastDataVer.value();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return sentMsgsCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return sentBytesCnt.value();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return rcvdMsgsCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return rcvdBytesCnt.value();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return outboundMsgCnt.value();
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentPmeDuration() {
        return pmeDuration.value();
    }

    /**
     * Job metrics
     */
    public GridJobMetrics jobMetrics() {
        if (jobMetrics == null)
            synchronized (jobMetricsMux) {
                if (jobMetrics == null) {
                    jobMetricsUpdateTime = U.currentTimeMillis();

                    jobMetrics = ctx.jobMetric().getJobMetrics();
                }
            }

        return jobMetrics;
    }
}

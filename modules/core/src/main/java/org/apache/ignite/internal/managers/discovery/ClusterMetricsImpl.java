/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetrics;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.DAEMON_THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PEAK_THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TOTAL_STARTED_THREAD_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.UP_TIME;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Cluster metrics proxy
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

    /**
     * @param ctx Kernel context.
     * @param nodeStartTime Node start time.
     */
    public ClusterMetricsImpl(GridKernalContext ctx, long nodeStartTime) {
        this.ctx = ctx;
        this.nodeStartTime = nodeStartTime;

        MetricRegistry mreg = ctx.metric().registry(SYS_METRICS);

        gcCpuLoad = (DoubleMetric)mreg.findMetric(GC_CPU_LOAD);
        cpuLoad = (DoubleMetric)mreg.findMetric(CPU_LOAD);
        upTime = (LongMetric)mreg.findMetric(UP_TIME);

        threadCnt = (IntMetric)mreg.findMetric(THREAD_CNT);
        peakThreadCnt = (IntMetric)mreg.findMetric(PEAK_THREAD_CNT);
        totalStartedThreadCnt = (LongMetric)mreg.findMetric(TOTAL_STARTED_THREAD_CNT);
        daemonThreadCnt = (IntMetric)mreg.findMetric(DAEMON_THREAD_CNT);

        availableProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

        heapInit = (LongMetric)mreg.findMetric(metricName("memory", "heap", "init"));
        heapUsed = (LongMetric)mreg.findMetric(metricName("memory", "heap", "used"));
        heapCommitted = (LongMetric)mreg.findMetric(metricName("memory", "heap", "committed"));
        heapMax = (LongMetric)mreg.findMetric(metricName("memory", "heap", "max"));

        nonHeapInit = (LongMetric)mreg.findMetric(metricName("memory", "nonheap", "init"));
        nonHeapUsed = (LongMetric)mreg.findMetric(metricName("memory", "nonheap", "used"));
        nonHeapCommitted = (LongMetric)mreg.findMetric(metricName("memory", "nonheap", "committed"));
        nonHeapMax = (LongMetric)mreg.findMetric(metricName("memory", "nonheap", "max"));
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
        return ctx.task().getTotalExecutedTasks();
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
        return ctx.cache().lastDataVersion();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return ctx.io().getSentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return ctx.io().getSentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return ctx.io().getReceivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return ctx.io().getReceivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return ctx.io().getOutboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentPmeDuration() {
        GridDhtPartitionsExchangeFuture future = ctx.cache().context().exchange().lastTopologyFuture();
        
        return (future == null || future.isDone()) ?
            0 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - future.getInitTime());
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

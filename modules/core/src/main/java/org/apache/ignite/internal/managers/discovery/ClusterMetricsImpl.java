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
import java.lang.management.RuntimeMXBean;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetrics;
import org.apache.ignite.internal.processors.metric.sources.ComputeMetricSource;
import org.apache.ignite.internal.processors.metric.sources.PartitionExchangeMetricSource;
import org.apache.ignite.internal.processors.metric.sources.SystemMetricSource;
import org.apache.ignite.internal.processors.metric.sources.CommunicationMetricSource;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.metric.sources.ComputeMetricSource.COMPUTE_METRICS;
import static org.apache.ignite.internal.processors.metric.sources.SystemMetricSource.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.sources.CommunicationMetricSource.COMM_METRICS;

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

    /** Available processors count. */
    private final int availableProcessors;

    /** System metric source. */
    private final SystemMetricSource sysMetricSrc;

    /** TCP communication metric source. */
    private final CommunicationMetricSource tcpCommMetricSrc;

    /**
     * @param ctx Kernel context.
     * @param nodeStartTime Node start time.
     */
    public ClusterMetricsImpl(GridKernalContext ctx, long nodeStartTime) {
        this.ctx = ctx;
        this.nodeStartTime = nodeStartTime;

        sysMetricSrc = ctx.metric().source(SYS_METRICS);
        tcpCommMetricSrc = ctx.metric().source(COMM_METRICS);

        availableProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
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
        ComputeMetricSource src = ctx.metric().source(COMPUTE_METRICS);

        return src != null ? (int)src.executedTasks() : 0;
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
        return sysMetricSrc.cpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return jobMetrics().getAverageCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return sysMetricSrc.gcCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.heap();

        return mem == null ? -1 : mem.init();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.heap();

        return mem == null ? -1 : mem.used();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.heap();

        return mem == null ? -1 : mem.committed();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.heap();

        return mem == null ? -1 : mem.max();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        //TODO: why is the same as max?
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.heap();

        return mem == null ? -1 : mem.max();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.nonHeap();

        return mem == null ? -1 : mem.init();
    }

    /** {@inheritDoc} */
    //TODO: Consider removing due to it is aggregated value
    @Override public long getNonHeapMemoryUsed() {
        Collection<GridCacheAdapter<?, ?>> caches = ctx.cache().internalCaches();

        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.nonHeap();

        long nonHeapUsed = mem.used();

        for (GridCacheAdapter<?, ?> cache : caches)
            if (cache.context().statisticsEnabled() && cache.context().started()
                && cache.context().affinity().affinityTopologyVersion().topologyVersion() > 0)
                nonHeapUsed += cache.metrics0().getOffHeapAllocatedSize();

        return nonHeapUsed;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.nonHeap();

        return mem == null ? -1 : mem.committed();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.nonHeap();

        return mem == null ? -1 : mem.max();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        SystemMetricSource.MemoryUsageMetrics mem = sysMetricSrc.nonHeap();

        return mem == null ? -1 : mem.max();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return sysMetricSrc.upTime();
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
        return sysMetricSrc.threadCount();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return sysMetricSrc.peakThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return sysMetricSrc.totalStartedThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return sysMetricSrc.daemonThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return sysMetricSrc.lastDataVersion();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return tcpCommMetricSrc.sentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return tcpCommMetricSrc.sentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return tcpCommMetricSrc.receivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return tcpCommMetricSrc.receivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return tcpCommMetricSrc.outboundMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentPmeDuration() {
        PartitionExchangeMetricSource pmeMetricSrc = ctx.cache().context().exchange().metricSource();

        return pmeMetricSrc !=null ? pmeMetricSrc.currentPmeDuration() : -1L;
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

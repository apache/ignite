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

import java.util.Collection;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetrics;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cluster metrics proxy
 */
public class ClusterMetricsImpl implements ClusterMetrics {
    /** Job metrics. */
    private volatile GridJobMetrics jobMetrics;

    /** Job metrics update time. */
    private volatile long jobMetricsUpdateTime;

    /** Job metrics mutex. */
    private final Object jobMetricsMux = new Object();

    /** Kernel context. */
    private final GridKernalContext ctx;

    /** VM Metrics. */
    private final GridLocalMetrics vmMetrics;

    /** Node start time. */
    private final long nodeStartTime;

    /**
     * @param ctx Kernel context.
     * @param vmMetrics VM metrics.
     * @param nodeStartTime Node start time;
     */
    public ClusterMetricsImpl(GridKernalContext ctx, GridLocalMetrics vmMetrics, long nodeStartTime) {
        this.ctx = ctx;
        this.vmMetrics = vmMetrics;
        this.nodeStartTime = nodeStartTime;
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
        return vmMetrics.getAvailableProcessors();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return vmMetrics.getCurrentCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return jobMetrics().getAverageCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return vmMetrics.getCurrentGcCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return vmMetrics.getHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return vmMetrics.getHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return vmMetrics.getHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return vmMetrics.getHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return vmMetrics.getHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return vmMetrics.getNonHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        Collection<GridCacheAdapter<?, ?>> caches = ctx.cache().internalCaches();

        long nonHeapUsed = vmMetrics.getNonHeapMemoryUsed();

        for (GridCacheAdapter<?, ?> cache : caches)
            if (cache.context().statisticsEnabled() && cache.context().started()
                && cache.context().affinity().affinityTopologyVersion().topologyVersion() > 0)
                nonHeapUsed += cache.metrics0().getOffHeapAllocatedSize();

        return nonHeapUsed;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return vmMetrics.getNonHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return vmMetrics.getNonHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        return vmMetrics.getNonHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return vmMetrics.getUptime();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return vmMetrics.getStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return nodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return vmMetrics.getThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return vmMetrics.getPeakThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return vmMetrics.getTotalStartedThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return vmMetrics.getDaemonThreadCount();
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

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

package org.apache.ignite.internal;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.ClusterLocalNodeMetricsMXBean;

/**
 * Local node metrics MBean.
 */
public class ClusterLocalNodeMetricsMXBeanImpl implements ClusterLocalNodeMetricsMXBean {
    /** Grid node. */
    private final ClusterNode node;

    /**
     * @param node Node to manage.
     */
    public ClusterLocalNodeMetricsMXBeanImpl(ClusterNode node) {
        assert node != null;

        this.node = node;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCpus() {
        return node.metrics().getTotalCpus();
    }

    /** {@inheritDoc} */
    @Override public float getAverageActiveJobs() {
        return node.metrics().getAverageActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageCancelledJobs() {
        return node.metrics().getAverageCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobExecuteTime() {
        return node.metrics().getAverageJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobWaitTime() {
        return node.metrics().getAverageJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return node.metrics().getAverageRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageWaitingJobs() {
        return node.metrics().getAverageWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public float getBusyTimePercentage() {
        return node.metrics().getBusyTimePercentage() * 100;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobs() {
        return node.metrics().getCurrentActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentCancelledJobs() {
        return node.metrics().getCurrentCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentIdleTime() {
        return node.metrics().getCurrentIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobExecuteTime() {
        return node.metrics().getCurrentJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobWaitTime() {
        return node.metrics().getCurrentJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return node.metrics().getCurrentRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitingJobs() {
        return node.metrics().getCurrentWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedTasks() {
        return node.metrics().getTotalExecutedTasks();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return node.metrics().getCurrentDaemonThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return node.metrics().getHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return node.metrics().getHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return node.metrics().getHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return node.metrics().getHeapMemoryTotal();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return node.metrics().getHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public float getIdleTimePercentage() {
        return node.metrics().getIdleTimePercentage() * 100;
    }

    /** {@inheritDoc} */
    @Override public long getLastUpdateTime() {
        return node.metrics().getLastUpdateTime();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveJobs() {
        return node.metrics().getMaximumActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCancelledJobs() {
        return node.metrics().getMaximumCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobExecuteTime() {
        return node.metrics().getMaximumJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobWaitTime() {
        return node.metrics().getMaximumJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return node.metrics().getMaximumRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumWaitingJobs() {
        return node.metrics().getMaximumWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return node.metrics().getNonHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return node.metrics().getNonHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return node.metrics().getNonHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        return node.metrics().getNonHeapMemoryTotal();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        return node.metrics().getNonHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return node.metrics().getMaximumThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return node.metrics().getStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return node.metrics().getNodeStartTime();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return node.metrics().getCurrentCpuLoad() * 100;
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return node.metrics().getAverageCpuLoad() * 100;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return node.metrics().getCurrentGcCpuLoad() * 100;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return node.metrics().getCurrentThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return node.metrics().getTotalBusyTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalCancelledJobs() {
        return node.metrics().getTotalCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedJobs() {
        return node.metrics().getTotalExecutedJobs();
    }

    /** {@inheritDoc} */
    @Override public long getTotalIdleTime() {
        return node.metrics().getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return node.metrics().getTotalRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return node.metrics().getTotalStartedThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return node.metrics().getUpTime();
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return node.metrics().getLastDataVersion();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return node.metrics().getSentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return node.metrics().getSentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return node.metrics().getReceivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return node.metrics().getReceivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return node.metrics().getOutboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return node.metrics().getTotalNodes();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterLocalNodeMetricsMXBeanImpl.class, this);
    }
}
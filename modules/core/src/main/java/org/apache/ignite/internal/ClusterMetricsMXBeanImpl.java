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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.ClusterMetricsMXBean;

/**
 * Cluster metrics MBean.
 */
public class ClusterMetricsMXBeanImpl implements ClusterMetricsMXBean {
    /** Grid cluster. */
    private final ClusterGroup cluster;

    /** Cached value of cluster metrics. */
    private volatile ClusterMetrics clusterMetricsSnapshot;

    /** Cluster metrics expire time. */
    private volatile long clusterMetricsExpireTime;

    /** Cluster metrics update mutex. */
    private final Object clusterMetricsMux = new Object();

    /**
     * @param cluster Cluster group to manage.
     */
    public ClusterMetricsMXBeanImpl(ClusterGroup cluster) {
        assert cluster != null;

        this.cluster = cluster;
    }

    /**
     * Gets a metrics snapshot for this cluster group.
     *
     * @return Metrics snapshot.
     */
    private ClusterMetrics metrics() {
        if (clusterMetricsExpireTime < System.currentTimeMillis()) {
            synchronized (clusterMetricsMux) {
                if (clusterMetricsExpireTime < System.currentTimeMillis()) {
                    clusterMetricsSnapshot = cluster.metrics();

                    clusterMetricsExpireTime = System.currentTimeMillis()
                        + cluster.ignite().configuration().getMetricsUpdateFrequency();
                }
            }
        }

        return clusterMetricsSnapshot;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCpus() {
        return metrics().getTotalCpus();
    }

    /** {@inheritDoc} */
    @Override public float getAverageActiveJobs() {
        return metrics().getAverageActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageCancelledJobs() {
        return metrics().getAverageCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobExecuteTime() {
        return metrics().getAverageJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobWaitTime() {
        return metrics().getAverageJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return metrics().getAverageRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageWaitingJobs() {
        return metrics().getAverageWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public float getBusyTimePercentage() {
        return metrics().getBusyTimePercentage() * 100;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobs() {
        return metrics().getCurrentActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentCancelledJobs() {
        return metrics().getCurrentCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentIdleTime() {
        return metrics().getCurrentIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobExecuteTime() {
        return metrics().getCurrentJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobWaitTime() {
        return metrics().getCurrentJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return metrics().getCurrentRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitingJobs() {
        return metrics().getCurrentWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedTasks() {
        return metrics().getTotalExecutedTasks();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return metrics().getCurrentDaemonThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return metrics().getHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return metrics().getHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return metrics().getHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return metrics().getHeapMemoryTotal();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return metrics().getHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public float getIdleTimePercentage() {
        return metrics().getIdleTimePercentage() * 100;
    }

    /** {@inheritDoc} */
    @Override public long getLastUpdateTime() {
        return metrics().getLastUpdateTime();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveJobs() {
        return metrics().getMaximumActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCancelledJobs() {
        return metrics().getMaximumCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobExecuteTime() {
        return metrics().getMaximumJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobWaitTime() {
        return metrics().getMaximumJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return metrics().getMaximumRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumWaitingJobs() {
        return metrics().getMaximumWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return metrics().getNonHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return metrics().getNonHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return metrics().getNonHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        return metrics().getNonHeapMemoryTotal();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        return metrics().getNonHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return metrics().getMaximumThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return metrics().getStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return metrics().getNodeStartTime();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return metrics().getCurrentCpuLoad() * 100;
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return metrics().getAverageCpuLoad() * 100;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return metrics().getCurrentGcCpuLoad() * 100;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return metrics().getCurrentThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return metrics().getTotalBusyTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalCancelledJobs() {
        return metrics().getTotalCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedJobs() {
        return metrics().getTotalExecutedJobs();
    }

    /** {@inheritDoc} */
    @Override public long getTotalJobsExecutionTime() {
        return metrics().getTotalJobsExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getTotalIdleTime() {
        return metrics().getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return metrics().getTotalRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return metrics().getTotalStartedThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return metrics().getUpTime();
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return metrics().getLastDataVersion();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return metrics().getSentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return metrics().getSentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return metrics().getReceivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return metrics().getReceivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return metrics().getOutboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return metrics().getTotalNodes();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentPmeDuration() {
        return metrics().getCurrentPmeDuration();
    }

    /** {@inheritDoc} */
    @Override public int getTotalBaselineNodes() {
        Collection<BaselineNode> baselineNodes = cluster.ignite().cluster().currentBaselineTopology();

        return baselineNodes != null ? baselineNodes.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public int getActiveBaselineNodes() {
        Collection<BaselineNode> baselineNodes = cluster.ignite().cluster().currentBaselineTopology();

        if (baselineNodes != null && !baselineNodes.isEmpty()) {
            Set<Object> bltIds = new HashSet<>(baselineNodes.size());

            for (BaselineNode baselineNode : baselineNodes)
                bltIds.add(baselineNode.consistentId());

            int count = 0;

            for (ClusterNode node : cluster.forServers().nodes())
                if (bltIds.contains(node.consistentId()))
                    count++;

            return count;
        }

        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTotalServerNodes() {
        return cluster.forServers().nodes().size();
    }

    /** {@inheritDoc} */
    @Override public int getTotalClientNodes() {
        return cluster.forClients().nodes().size();
    }

    /** {@inheritDoc} */
    @Override public long getTopologyVersion() {
        return cluster.ignite().cluster().topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public Set<String> attributeNames() {
        Set<String> attrs = new TreeSet<>();

        for (ClusterNode node : cluster.nodes())
            attrs.addAll(node.attributes().keySet());

        return attrs;
    }

    /** {@inheritDoc} */
    @Override public Set<String> attributeValues(String attrName) {
        Set<String> values = new TreeSet<>();

        for (ClusterNode node : cluster.nodes()) {
            Object val = node.attribute(attrName);

            if (val != null)
                values.add(val.toString());
        }

        return values;
    }

    /** {@inheritDoc} */
    @Override public Set<UUID> nodeIdsForAttribute(String attrName, String attrVal, boolean includeSrvs,
        boolean includeClients) {
        Set<UUID> nodes = new TreeSet<>();

        for (ClusterNode node : nodesList(includeSrvs, includeClients)) {
            Object val = node.attribute(attrName);

            if (val != null && val.toString().equals(attrVal))
                nodes.add(node.id());
        }

        return nodes;
    }

    /**
     * Get list with the specified node types.
     *
     * @param srv {@code True} to include server nodes.
     * @param client {@code True} to include client nodes.
     * @return List with the specified node types.
     */
    private List<ClusterNode> nodesList(boolean srv, boolean client) {
        List<ClusterNode> nodes = new ArrayList<>();

        if (srv)
            nodes.addAll(cluster.forServers().nodes());

        if (client)
            nodes.addAll(cluster.forClients().nodes());

        return nodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsMXBeanImpl.class, this);
    }
}

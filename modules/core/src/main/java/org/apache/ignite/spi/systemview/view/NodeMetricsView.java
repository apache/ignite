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

package org.apache.ignite.spi.systemview.view;

import java.util.Date;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Node metrics representation for a {@link SystemView}.
 */
public class NodeMetricsView {
    /** Cluster node. */
    private final ClusterNode node;

    /** Node metrics. */
    private final ClusterMetrics metrics;

    /**
     * @param node Cluster node.
     */
    public NodeMetricsView(ClusterNode node) {
        this.node = node;
        metrics = node.metrics();
    }

    /**
     * @return Node id.
     * @see ClusterNode#id()
     */
    @Order
    public UUID nodeId() {
        return node.id();
    }

    /**
     * @return Metrics last update time.
     * @see ClusterMetrics#getLastUpdateTime()
     */
    @Order(1)
    public Date lastUpdateTime() {
        return new Date(metrics.getLastUpdateTime());
    }

    /**
     * @return Maximum active jobs count.
     * @see ClusterMetrics#getMaximumActiveJobs()
     */
    @Order(2)
    public int maxActiveJobs() {
        return metrics.getMaximumActiveJobs();
    }

    /**
     * @return Current active jobs count.
     * @see ClusterMetrics#getCurrentActiveJobs()
     */
    @Order(3)
    public int curActiveJobs() {
        return metrics.getCurrentActiveJobs();
    }

    /**
     * @return Average active jobs count.
     * @see ClusterMetrics#getAverageActiveJobs()
     */
    @Order(4)
    public float avgActiveJobs() {
        return metrics.getAverageActiveJobs();
    }

    /**
     * @return Maximum waiting jobs count.
     * @see ClusterMetrics#getMaximumWaitingJobs()
     */
    @Order(5)
    public int maxWaitingJobs() {
        return metrics.getMaximumWaitingJobs();
    }

    /**
     * @return Current waiting jobs count.
     * @see ClusterMetrics#getCurrentWaitingJobs()
     */
    @Order(6)
    public int curWaitingJobs() {
        return metrics.getCurrentWaitingJobs();
    }

    /**
     * @return Average waiting jobs count.
     * @see ClusterMetrics#getAverageWaitingJobs()
     */
    @Order(7)
    public float avgWaitingJobs() {
        return metrics.getAverageWaitingJobs();
    }

    /**
     * @return Maximum number of jobs rejected at once.
     * @see ClusterMetrics#getMaximumRejectedJobs()
     */
    @Order(8)
    public int maxRejectedJobs() {
        return metrics.getMaximumRejectedJobs();
    }

    /**
     * @return Number of jobs rejected after more recent collision resolution operation.
     * @see ClusterMetrics#getCurrentRejectedJobs()
     */
    @Order(9)
    public int curRejectedJobs() {
        return metrics.getCurrentRejectedJobs();
    }

    /**
     * @return Average number of jobs this node rejects during collision resolution operations.
     * @see ClusterMetrics#getAverageRejectedJobs()
     */
    @Order(10)
    public float avgRejectedJobs() {
        return metrics.getAverageRejectedJobs();
    }

    /**
     * @return Total number of jobs this node rejects during collision resolution operations since node startup.
     * @see ClusterMetrics#getTotalRejectedJobs()
     */
    @Order(11)
    public int totalRejectedJobs() {
        return metrics.getTotalRejectedJobs();
    }

    /**
     * @return Maximum number of cancelled jobs.
     * @see ClusterMetrics#getMaximumCancelledJobs()
     */
    @Order(12)
    public int maxCanceledJobs() {
        return metrics.getMaximumCancelledJobs();
    }

    /**
     * @return Number of cancelled jobs that are still running.
     * @see ClusterMetrics#getCurrentCancelledJobs()
     */
    @Order(13)
    public int curCanceledJobs() {
        return metrics.getCurrentCancelledJobs();
    }

    /**
     * @return Average number of cancelled jobs.
     * @see ClusterMetrics#getAverageCancelledJobs()
     */
    @Order(14)
    public float avgCanceledJobs() {
        return metrics.getAverageCancelledJobs();
    }

    /**
     * @return Total number of cancelled jobs since node startup.
     * @see ClusterMetrics#getTotalCancelledJobs()
     */
    @Order(15)
    public int totalCanceledJobs() {
        return metrics.getTotalCancelledJobs();
    }

    /**
     * @return Maximum jobs wait time.
     * @see ClusterMetrics#getMaximumJobWaitTime()
     */
    @Order(16)
    public long maxJobsWaitTime() {
        return metrics.getMaximumJobWaitTime();
    }

    /**
     * @return Current wait time of oldest job.
     * @see ClusterMetrics#getCurrentJobWaitTime()
     */
    @Order(17)
    public long curJobsWaitTime() {
        return metrics.getCurrentJobWaitTime();
    }

    /**
     * @return Average jobs wait time.
     * @see ClusterMetrics#getAverageJobWaitTime()
     */
    @Order(18)
    public long avgJobsWaitTime() {
        return (long)metrics.getAverageJobWaitTime();
    }

    /**
     * @return Maximum jobs execute time.
     * @see ClusterMetrics#getMaximumJobExecuteTime()
     */
    @Order(19)
    public long maxJobsExecuteTime() {
        return metrics.getMaximumJobExecuteTime();
    }

    /**
     * @return Current jobs execute time.
     * @see ClusterMetrics#getCurrentJobExecuteTime()
     */
    @Order(20)
    public long curJobsExecuteTime() {
        return metrics.getCurrentJobExecuteTime();
    }

    /**
     * @return Average jobs execute time.
     * @see ClusterMetrics#getAverageJobExecuteTime()
     */
    @Order(21)
    public long avgJobsExecuteTime() {
        return (long)metrics.getAverageJobExecuteTime();
    }

    /**
     * @return Total jobs execute time.
     * @see ClusterMetrics#getTotalJobsExecutionTime()
     */
    @Order(22)
    public long totalJobsExecuteTime() {
        return metrics.getTotalJobsExecutionTime();
    }

    /**
     * @return Total executed jobs.
     * @see ClusterMetrics#getTotalExecutedJobs()
     */
    @Order(23)
    public int totalExecutedJobs() {
        return metrics.getTotalExecutedJobs();
    }

    /**
     * @return Total executed tasks.
     * @see ClusterMetrics#getTotalExecutedTasks()
     */
    @Order(24)
    public int totalExecutedTasks() {
        return metrics.getTotalExecutedTasks();
    }

    /**
     * @return Total busy time.
     * @see ClusterMetrics#getTotalBusyTime()
     */
    @Order(25)
    public long totalBusyTime() {
        return metrics.getTotalBusyTime();
    }

    /**
     * @return Total idle time.
     * @see ClusterMetrics#getTotalIdleTime()
     */
    @Order(26)
    public long totalIdleTime() {
        return metrics.getTotalIdleTime();
    }

    /**
     * @return Current idle time.
     * @see ClusterMetrics#getCurrentIdleTime()
     */
    @Order(27)
    public long curIdleTime() {
        return metrics.getCurrentIdleTime();
    }

    /**
     * @return Busy time percentage.
     * @see ClusterMetrics#getBusyTimePercentage()
     */
    @Order(28)
    public float busyTimePercentage() {
        return metrics.getBusyTimePercentage();
    }

    /**
     * @return Idle time percentage.
     * @see ClusterMetrics#getIdleTimePercentage()
     */
    @Order(29)
    public float idleTimePercentage() {
        return metrics.getIdleTimePercentage();
    }

    /**
     * @return The number of processors available to the virtual machine.
     * @see ClusterMetrics#getTotalCpus()
     */
    @Order(30)
    public int totalCpu() {
        return metrics.getTotalCpus();
    }

    /**
     * @return The estimated CPU usage in {@code [0, 1]} range.
     * @see ClusterMetrics#getCurrentCpuLoad()
     */
    @Order(31)
    public double curCpuLoad() {
        return metrics.getCurrentCpuLoad();
    }

    /**
     * @return Average of CPU load value in {@code [0, 1]} range.
     * @see ClusterMetrics#getAverageCpuLoad()
     */
    @Order(32)
    public double avgCpuLoad() {
        return metrics.getAverageCpuLoad();
    }

    /**
     * @return Average time spent in CG since the last update.
     * @see ClusterMetrics#getCurrentGcCpuLoad()
     */
    @Order(33)
    public double curGcCpuLoad() {
        return metrics.getCurrentGcCpuLoad();
    }

    /**
     * @return The initial size of memory in bytes; {@code -1} if undefined.
     * @see ClusterMetrics#getHeapMemoryInitialized()
     */
    @Order(34)
    public long heapMemoryInit() {
        return metrics.getHeapMemoryInitialized();
    }

    /**
     * @return Heap memory used.
     * @see ClusterMetrics#getHeapMemoryUsed()
     */
    @Order(35)
    public long heapMemoryUsed() {
        return metrics.getHeapMemoryUsed();
    }

    /**
     * @return Heap memory commited.
     * @see ClusterMetrics#getHeapMemoryCommitted()
     */
    @Order(36)
    public long heapMemoryCommited() {
        return metrics.getHeapMemoryCommitted();
    }

    /**
     * @return The maximum amount of memory in bytes; {@code -1} if undefined.
     * @see ClusterMetrics#getHeapMemoryMaximum()
     */
    @Order(37)
    public long heapMemoryMax() {
        return metrics.getHeapMemoryMaximum();
    }

    /**
     * @return Heap memory total.
     * @see ClusterMetrics#getHeapMemoryTotal()
     */
    @Order(38)
    public long heapMemoryTotal() {
        return metrics.getHeapMemoryTotal();
    }

    /**
     * @return The initial size of memory in bytes; {@code -1} if undefined.
     * @see ClusterMetrics#getNonHeapMemoryInitialized()
     */
    @Order(39)
    public long nonheapMemoryInit() {
        return metrics.getNonHeapMemoryInitialized();
    }

    /**
     * @return Nonheap memory used.
     * @see ClusterMetrics#getNonHeapMemoryUsed()
     */
    @Order(40)
    public long nonheapMemoryUsed() {
        return metrics.getNonHeapMemoryUsed();
    }

    /**
     * @return Nonheap memory commited.
     * @see ClusterMetrics#getNonHeapMemoryCommitted()
     */
    @Order(41)
    public long nonheapMemoryCommited() {
        return metrics.getNonHeapMemoryCommitted();
    }

    /**
     * @return The maximum amount of memory in bytes; {@code -1} if undefined.
     * @see ClusterMetrics#getNonHeapMemoryMaximum()
     */
    @Order(42)
    public long nonheapMemoryMax() {
        return metrics.getNonHeapMemoryMaximum();
    }

    /**
     * @return The total amount of memory in bytes; {@code -1} if undefined.
     * @see ClusterMetrics#getNonHeapMemoryTotal()
     */
    @Order(43)
    public long nonheapMemoryTotal() {
        return metrics.getNonHeapMemoryTotal();
    }

    /**
     * @return Uptime of the JVM in milliseconds.
     * @see ClusterMetrics#getUpTime()
     */
    @Order(44)
    public long uptime() {
        return metrics.getUpTime();
    }

    /**
     * @return Start time of the JVM in milliseconds.
     * @see ClusterMetrics#getStartTime()
     */
    @Order(45)
    public Date jvmStartTime() {
        return new Date(metrics.getStartTime());
    }

    /**
     * @return Node start time.
     * @see ClusterMetrics#getNodeStartTime()
     */
    @Order(46)
    public Date nodeStartTime() {
        return new Date(metrics.getNodeStartTime());
    }

    /**
     * @return Last data version.
     * @see ClusterMetrics#getLastDataVersion()
     */
    @Order(47)
    public long lastDataVersion() {
        return metrics.getLastDataVersion();
    }

    /**
     * @return Current thread count.
     * @see ClusterMetrics#getCurrentThreadCount()
     */
    @Order(48)
    public int curThreadCount() {
        return metrics.getCurrentThreadCount();
    }

    /**
     * @return Maximum thread count.
     * @see ClusterMetrics#getMaximumThreadCount()
     */
    @Order(49)
    public int maxThreadCount() {
        return metrics.getMaximumThreadCount();
    }

    /**
     * @return Total started thread count.
     * @see ClusterMetrics#getTotalStartedThreadCount()
     */
    @Order(50)
    public long totalThreadCount() {
        return metrics.getTotalStartedThreadCount();
    }

    /**
     * @return Current daemon thread count.
     * @see ClusterMetrics#getCurrentDaemonThreadCount()
     */
    @Order(51)
    public int curDaemonThreadCount() {
        return metrics.getCurrentDaemonThreadCount();
    }

    /**
     * @return Sent messages count.
     * @see ClusterMetrics#getSentMessagesCount()
     */
    @Order(52)
    public int sentMessagesCount() {
        return metrics.getSentMessagesCount();
    }

    /**
     * @return Sent bytes count.
     * @see ClusterMetrics#getSentBytesCount()
     */
    @Order(53)
    public long sentBytesCount() {
        return metrics.getSentBytesCount();
    }

    /**
     * @return Received messages count.
     * @see ClusterMetrics#getReceivedMessagesCount()
     */
    @Order(54)
    public int receivedMessagesCount() {
        return metrics.getReceivedMessagesCount();
    }

    /**
     * @return Received bytes count.
     * @see ClusterMetrics#getReceivedBytesCount()
     */
    @Order(55)
    public long receivedBytesCount() {
        return metrics.getReceivedBytesCount();
    }

    /**
     * @return Outbound messages queue size.
     * @see ClusterMetrics#getOutboundMessagesQueueSize()
     */
    @Order(56)
    public int outboundMessagesQueue() {
        return metrics.getOutboundMessagesQueueSize();
    }
}

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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.min;

/**
 * Implementation for {@link ClusterMetrics} interface.
 * <p>
 * Note that whenever adding or removing metric parameters, care
 * must be taken to update serialize/deserialize logic as well.
 */
public class ClusterMetricsSnapshot implements ClusterMetrics {
    /** Size of serialized node metrics. */
    public static final int METRICS_SIZE =
        4/*max active jobs*/ +
        4/*current active jobs*/ +
        4/*average active jobs*/ +
        4/*max waiting jobs*/ +
        4/*current waiting jobs*/ +
        4/*average waiting jobs*/ +
        4/*max cancelled jobs*/ +
        4/*current cancelled jobs*/ +
        4/*average cancelled jobs*/ +
        4/*max rejected jobs*/ +
        4/*current rejected jobs*/ +
        4/*average rejected jobs*/ +
        4/*total executed jobs*/ +
        4/*total rejected jobs*/ +
        4/*total cancelled jobs*/ +
        8/*max job wait time*/ +
        8/*current job wait time*/ +
        8/*average job wait time*/ +
        8/*max job execute time*/ +
        8/*current job execute time*/ +
        8/*average job execute time*/ +
        4/*total executed tasks*/ +
        8/*current idle time*/ +
        8/*total idle time*/ +
        4/*available processors*/ +
        8/*current CPU load*/ +
        8/*average CPU load*/ +
        8/*current GC CPU load*/ +
        8/*heap memory init*/ +
        8/*heap memory used*/ +
        8/*heap memory committed*/ +
        8/*heap memory max*/ +
        8/*heap memory total*/ +
        8/*non-heap memory init*/ +
        8/*non-heap memory used*/ +
        8/*non-heap memory committed*/ +
        8/*non-heap memory max*/ +
        8/*non-heap memory total*/ +
        8/*uptime*/ +
        8/*start time*/ +
        8/*node start time*/ +
        4/*thread count*/ +
        4/*peak thread count*/ +
        8/*total started thread count*/ +
        4/*daemon thread count*/ +
        8/*last data version.*/ +
        4/*sent messages count*/ +
        8/*sent bytes count*/ +
        4/*received messages count*/ +
        8/*received bytes count*/ +
        4/*outbound messages queue size*/ +
        4/*total nodes*/ +
        8/*total jobs execution time*/ +
        8/*current PME time*/;

    /** */
    private NodeMetricsMessage m;

    /**
     * Creates empty snapshot.
     */
    public ClusterMetricsSnapshot() {
        m = new NodeMetricsMessage();
    }

    /**
     * Creates snapshot based on the handled message.
     */
    public ClusterMetricsSnapshot(NodeMetricsMessage m) {
        this.m = m;
    }

    /**
     * Create metrics for given cluster group.
     *
     * @param p Projection to get metrics for.
     */
    public ClusterMetricsSnapshot(ClusterGroup p) {
        assert p != null;

        m = new NodeMetricsMessage(p.nodes());
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return m.heapMemoryTotal();
    }

    /** {@inheritDoc} */
    @Override public long getLastUpdateTime() {
        return m.lastUpdateTime();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveJobs() {
        return m.maximumActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobs() {
        return m.currentActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageActiveJobs() {
        return m.averageActiveJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumWaitingJobs() {
        return m.maximumWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitingJobs() {
        return m.currentWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageWaitingJobs() {
        return m.averageWaitingJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return m.maximumRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return m.currentRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return m.averageRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return m.totalRejectedJobs();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCancelledJobs() {
        return m.maximumCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentCancelledJobs() {
        return m.currentCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public float getAverageCancelledJobs() {
        return m.averageCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedJobs() {
        return m.totalExecutedJobs();
    }

    /** {@inheritDoc} */
    @Override public long getTotalJobsExecutionTime() {
        return m.totalJobsExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalCancelledJobs() {
        return m.totalCancelledJobs();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobWaitTime() {
        return m.maximumJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobWaitTime() {
        return m.currentJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobWaitTime() {
        return m.averageJobWaitTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobExecuteTime() {
        return m.maximumJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobExecuteTime() {
        return m.currentJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobExecuteTime() {
        return m.averageJobExecuteTime();
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedTasks() {
        return m.totalExecutedTasks();
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return getUpTime() - getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getTotalIdleTime() {
        return m.totalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentIdleTime() {
        return m.currentIdleTime();
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
        return m.totalCpus();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return m.currentCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return m.averageCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return m.currentGcCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return m.heapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return m.heapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return m.heapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return m.heapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return m.nonHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        return m.nonHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return m.nonHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return m.nonHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        return m.nonHeapMemoryTotal();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return m.upTime();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return m.startTime();
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return m.nodeStartTime();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return m.currentThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return m.maximumThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return m.totalStartedThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return m.currentDaemonThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return m.lastDataVersion();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return m.sentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return m.sentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return m.receivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return m.receivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return m.outboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return m.totalNodes();
    }

    /** {@inheritDoc} */
    @Override public long getCurrentPmeDuration() {
        return m.currentPmeDuration();
    }

    /**
     * @param neighborhood Cluster neighborhood.
     * @return CPU count.
     */
    private static int cpuCnt(Map<String, Collection<ClusterNode>> neighborhood) {
        int cpus = 0;

        for (Collection<ClusterNode> nodes : neighborhood.values()) {
            ClusterNode first = F.first(nodes);

            // Projection can be empty if all nodes in it failed.
            if (first != null)
                cpus += first.metrics().getTotalCpus();
        }

        return cpus;
    }

    /**
     * @param neighborhood Cluster neighborhood.
     * @return CPU load.
     */
    private static int cpus(Map<String, Collection<ClusterNode>> neighborhood) {
        int cpus = 0;

        for (Collection<ClusterNode> nodes : neighborhood.values()) {
            ClusterNode first = F.first(nodes);

            // Projection can be empty if all nodes in it failed.
            if (first != null)
                cpus += first.metrics().getCurrentCpuLoad();
        }

        return cpus;
    }

    /**
     * @param neighborhood Cluster neighborhood.
     * @return GC CPU load.
     */
    private static int gcCpus(Map<String, Collection<ClusterNode>> neighborhood) {
        int cpus = 0;

        for (Collection<ClusterNode> nodes : neighborhood.values()) {
            ClusterNode first = F.first(nodes);

            // Projection can be empty if all nodes in it failed.
            if (first != null)
                cpus += first.metrics().getCurrentGcCpuLoad();
        }

        return cpus;
    }

    /**
     * Gets the oldest node in given collection.
     *
     * @param nodes Nodes.
     * @return Oldest node or {@code null} if collection is empty.
     */
    @Nullable private static ClusterNode oldest(Collection<ClusterNode> nodes) {
        long min = Long.MAX_VALUE;

        ClusterNode oldest = null;

        for (ClusterNode n : nodes)
            if (n.order() < min) {
                min = n.order();
                oldest = n;
            }

        return oldest;
    }

    /**
     * Serializes node metrics into byte array.
     *
     * @param metrics Node metrics to serialize.
     * @return New offset.
     */
    public static byte[] serialize(ClusterMetrics metrics) {
        byte[] buf = new byte[METRICS_SIZE];

        serialize(buf, 0, metrics);

        return buf;
    }

    /**
     * Serializes node metrics into byte array.
     *
     * @param data Byte array.
     * @param off Offset into byte array.
     * @param metrics Node metrics to serialize.
     * @return New offset.
     */
    public static int serialize(byte[] data, int off, ClusterMetrics metrics) {
        ByteBuffer buf = ByteBuffer.wrap(data, off, METRICS_SIZE);

        buf.putInt(metrics.getMaximumActiveJobs());
        buf.putInt(metrics.getCurrentActiveJobs());
        buf.putFloat(metrics.getAverageActiveJobs());
        buf.putInt(metrics.getMaximumWaitingJobs());
        buf.putInt(metrics.getCurrentWaitingJobs());
        buf.putFloat(metrics.getAverageWaitingJobs());
        buf.putInt(metrics.getMaximumRejectedJobs());
        buf.putInt(metrics.getCurrentRejectedJobs());
        buf.putFloat(metrics.getAverageRejectedJobs());
        buf.putInt(metrics.getMaximumCancelledJobs());
        buf.putInt(metrics.getCurrentCancelledJobs());
        buf.putFloat(metrics.getAverageCancelledJobs());
        buf.putInt(metrics.getTotalRejectedJobs());
        buf.putInt(metrics.getTotalCancelledJobs());
        buf.putInt(metrics.getTotalExecutedJobs());
        buf.putLong(metrics.getMaximumJobWaitTime());
        buf.putLong(metrics.getCurrentJobWaitTime());
        buf.putDouble(metrics.getAverageJobWaitTime());
        buf.putLong(metrics.getMaximumJobExecuteTime());
        buf.putLong(metrics.getCurrentJobExecuteTime());
        buf.putDouble(metrics.getAverageJobExecuteTime());
        buf.putInt(metrics.getTotalExecutedTasks());
        buf.putLong(metrics.getCurrentIdleTime());
        buf.putLong(metrics.getTotalIdleTime());
        buf.putInt(metrics.getTotalCpus());
        buf.putDouble(metrics.getCurrentCpuLoad());
        buf.putDouble(metrics.getAverageCpuLoad());
        buf.putDouble(metrics.getCurrentGcCpuLoad());
        buf.putLong(metrics.getHeapMemoryInitialized());
        buf.putLong(metrics.getHeapMemoryUsed());
        buf.putLong(metrics.getHeapMemoryCommitted());
        buf.putLong(metrics.getHeapMemoryMaximum());
        buf.putLong(metrics.getHeapMemoryTotal());
        buf.putLong(metrics.getNonHeapMemoryInitialized());
        buf.putLong(metrics.getNonHeapMemoryUsed());
        buf.putLong(metrics.getNonHeapMemoryCommitted());
        buf.putLong(metrics.getNonHeapMemoryMaximum());
        buf.putLong(metrics.getNonHeapMemoryTotal());
        buf.putLong(metrics.getStartTime());
        buf.putLong(metrics.getNodeStartTime());
        buf.putLong(metrics.getUpTime());
        buf.putInt(metrics.getCurrentThreadCount());
        buf.putInt(metrics.getMaximumThreadCount());
        buf.putLong(metrics.getTotalStartedThreadCount());
        buf.putInt(metrics.getCurrentDaemonThreadCount());
        buf.putLong(metrics.getLastDataVersion());
        buf.putInt(metrics.getSentMessagesCount());
        buf.putLong(metrics.getSentBytesCount());
        buf.putInt(metrics.getReceivedMessagesCount());
        buf.putLong(metrics.getReceivedBytesCount());
        buf.putInt(metrics.getOutboundMessagesQueueSize());
        buf.putInt(metrics.getTotalNodes());
        buf.putLong(metrics.getTotalJobsExecutionTime());
        buf.putLong(metrics.getCurrentPmeDuration());

        assert !buf.hasRemaining() : "Invalid metrics size [expected=" + METRICS_SIZE + ", actual="
            + (buf.position() - off) + ']';

        return buf.position();
    }

    /**
     * De-serializes node metrics.
     *
     * @param data Byte array.
     * @param off Offset into byte array.
     * @return Deserialized node metrics.
     */
    public static ClusterMetrics deserialize(byte[] data, int off) {
        NodeMetricsMessage msg = new NodeMetricsMessage();

        int bufSize = min(METRICS_SIZE, data.length - off);

        ByteBuffer buf = ByteBuffer.wrap(data, off, bufSize);

        msg.lastUpdateTime(U.currentTimeMillis());

        msg.maximumActiveJobs(buf.getInt());
        msg.currentActiveJobs(buf.getInt());
        msg.averageActiveJobs(buf.getFloat());
        msg.maximumWaitingJobs(buf.getInt());
        msg.currentWaitingJobs(buf.getInt());
        msg.averageWaitingJobs(buf.getFloat());
        msg.maximumRejectedJobs(buf.getInt());
        msg.currentRejectedJobs(buf.getInt());
        msg.averageRejectedJobs(buf.getFloat());
        msg.maximumCancelledJobs(buf.getInt());
        msg.currentCancelledJobs(buf.getInt());
        msg.averageCancelledJobs(buf.getFloat());
        msg.totalRejectedJobs(buf.getInt());
        msg.totalCancelledJobs(buf.getInt());
        msg.totalExecutedJobs(buf.getInt());
        msg.maximumJobWaitTime(buf.getLong());
        msg.currentJobWaitTime(buf.getLong());
        msg.averageJobWaitTime(buf.getDouble());
        msg.maximumJobExecuteTime(buf.getLong());
        msg.currentJobExecuteTime(buf.getLong());
        msg.averageJobExecuteTime(buf.getDouble());
        msg.totalExecutedTasks(buf.getInt());
        msg.currentIdleTime(buf.getLong());
        msg.totalIdleTime(buf.getLong());
        msg.totalCpus(buf.getInt());
        msg.currentCpuLoad(buf.getDouble());
        msg.averageCpuLoad(buf.getDouble());
        msg.currentGcCpuLoad(buf.getDouble());
        msg.heapMemoryInitialized(buf.getLong());
        msg.heapMemoryUsed(buf.getLong());
        msg.heapMemoryCommitted(buf.getLong());
        msg.heapMemoryMaximum(buf.getLong());
        msg.heapMemoryTotal(buf.getLong());
        msg.nonHeapMemoryInitialized(buf.getLong());
        msg.nonHeapMemoryUsed(buf.getLong());
        msg.nonHeapMemoryCommitted(buf.getLong());
        msg.nonHeapMemoryMaximum(buf.getLong());
        msg.nonHeapMemoryTotal(buf.getLong());
        msg.startTime(buf.getLong());
        msg.nodeStartTime(buf.getLong());
        msg.upTime(buf.getLong());
        msg.currentThreadCount(buf.getInt());
        msg.maximumThreadCount(buf.getInt());
        msg.totalStartedThreadCount(buf.getLong());
        msg.currentDaemonThreadCount(buf.getInt());
        msg.lastDataVersion(buf.getLong());
        msg.sentMessagesCount(buf.getInt());
        msg.sentBytesCount(buf.getLong());
        msg.receivedMessagesCount(buf.getInt());
        msg.receivedBytesCount(buf.getLong());
        msg.outboundMessagesQueueSize(buf.getInt());
        msg.totalNodes(buf.getInt());

        // For compatibility with metrics serialized by old ignite versions.
        if (buf.remaining() >= 8)
            msg.totalJobsExecutionTime(buf.getLong());
        else
            msg.totalJobsExecutionTime(0);

        if (buf.remaining() >= 8)
            msg.currentPmeDuration(buf.getLong());
        else
            msg.currentPmeDuration(0);

        return new ClusterMetricsSnapshot(msg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsSnapshot.class, this);
    }
}

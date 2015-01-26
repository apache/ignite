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

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static java.lang.Math.*;

/**
 * Implementation for {@link org.apache.ignite.cluster.ClusterMetrics ClusterNodeMetrics} interface.
 */
class ClusterMetricsImpl implements ClusterMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int totalExecutedTask;

    /** */
    private int maxActJobs = Integer.MIN_VALUE;

    /** */
    private int curActJobs;

    /** */
    private int totalExecutedJobs;

    /** */
    private float avgActJobs;

    /** */
    private int curCancelJobs;

    /** */
    private int totalCancelJobs;

    /** */
    private int maxCancelJobs = Integer.MIN_VALUE;

    /** */
    private float avgCancelJobs;

    /** */
    private int curRejectJobs;

    /** */
    private int maxRejectJobs = Integer.MIN_VALUE;

    /** */
    private float avgRejectJobs;

    /** */
    private int totalRejectJobs;

    /** */
    private int curWaitJobs;

    /** */
    private int maxWaitJobs = Integer.MIN_VALUE;

    /** */
    private float avgWaitJobs;

    /** */
    private long maxJobExecTime = Long.MIN_VALUE;

    /** */
    private double avgJobExecTime;

    /** */
    private long minJobWaitTime = Long.MAX_VALUE;

    /** */
    private long maxJobWaitTime = Long.MIN_VALUE;

    /** */
    private double avgJobWaitTime;

    /** */
    private int curDaemonThreadCnt;

    /** */
    private int maxThreadCnt = Integer.MIN_VALUE;

    /** */
    private int curThreadCnt;

    /** */
    private int totalStartedThreadCnt;

    /** */
    private long minIdleTime = Long.MAX_VALUE;

    /** */
    private float avgIdleTimePercent;

    /** */
    private int totalIdleTime;

    /** */
    private float avgBusyTimePerc;

    /** */
    private int totalBusyTime;

    /** */
    private double avgCpuLoad;

    /** */
    private long totalHeapMemCmt = Long.MIN_VALUE;

    /** */
    private long totalHeapMemUsed;

    /** */
    private long maxHeapMemMax = Long.MIN_VALUE;

    /** */
    private long totalHeapMemInit;

    /** */
    private long totalNonHeapMemCmt;

    /** */
    private long totalNonHeapMemUsed;

    /** */
    private long maxNonHeapMemMax = Long.MIN_VALUE;

    /** */
    private long totalNonHeapMemInit;

    /** */
    private long oldestNodeStartTime = Long.MAX_VALUE;

    /** */
    private long maxUpTime = Long.MIN_VALUE;

    /** */
    private int totalCpus;

    /** */
    private int currentGcCpus;

    /** */
    private long lastUpdateTime;

    /** */
    private long lastDataVersion = Long.MIN_VALUE;

    /** */
    private int sentMessagesCnt;

    /** */
    private long sentBytesCnt;

    /** */
    private int receivedMessagesCnt;

    /** */
    private long receivedBytesCnt;

    /** */
    private int outboundMessagesQueueSize;

    /**
     * @param p Projection to get metrics for.
     */
    ClusterMetricsImpl(ClusterGroup p) {
        assert p != null;

        Collection<ClusterNode> nodes = p.nodes();

        int size = nodes.size();

        for (ClusterNode node : nodes) {
            ClusterMetrics m = node.metrics();

            lastUpdateTime = max(lastUpdateTime, node.metrics().getLastUpdateTime());

            curActJobs += m.getCurrentActiveJobs();
            maxActJobs = max(maxActJobs, m.getCurrentActiveJobs());
            avgActJobs += m.getCurrentActiveJobs();
            totalExecutedJobs += m.getTotalExecutedJobs();

            totalExecutedTask += m.getTotalExecutedTasks();

            totalCancelJobs += m.getTotalCancelledJobs();
            curCancelJobs += m.getCurrentCancelledJobs();
            maxCancelJobs = max(maxCancelJobs, m.getCurrentCancelledJobs());
            avgCancelJobs += m.getCurrentCancelledJobs();

            totalRejectJobs += m.getTotalRejectedJobs();
            curRejectJobs += m.getCurrentRejectedJobs();
            totalRejectJobs += m.getTotalRejectedJobs();
            maxRejectJobs = max(maxRejectJobs, m.getCurrentRejectedJobs());
            avgRejectJobs += m.getCurrentRejectedJobs();

            curWaitJobs += m.getCurrentJobWaitTime();
            maxWaitJobs = max(maxWaitJobs, m.getCurrentWaitingJobs());
            avgWaitJobs += m.getCurrentWaitingJobs();

            maxJobExecTime = max(maxJobExecTime, m.getCurrentJobExecuteTime());
            avgJobExecTime += m.getCurrentJobExecuteTime();

            minJobWaitTime = min(minJobWaitTime, m.getCurrentJobWaitTime());
            maxJobWaitTime = max(maxJobWaitTime, m.getCurrentJobWaitTime());
            avgJobWaitTime += m.getCurrentJobWaitTime();

            curDaemonThreadCnt += m.getCurrentDaemonThreadCount();

            maxThreadCnt = max(maxThreadCnt, m.getCurrentThreadCount());
            curThreadCnt += m.getCurrentThreadCount();
            totalStartedThreadCnt += m.getTotalStartedThreadCount();

            minIdleTime = min(minIdleTime, m.getCurrentIdleTime());
            avgIdleTimePercent += m.getIdleTimePercentage();
            totalIdleTime += m.getTotalIdleTime();

            avgBusyTimePerc += m.getBusyTimePercentage();
            totalBusyTime += m.getTotalBusyTime();

            avgCpuLoad += m.getCurrentCpuLoad();

            totalHeapMemCmt += m.getHeapMemoryCommitted();

            totalHeapMemUsed += m.getHeapMemoryUsed();

            maxHeapMemMax = max(maxHeapMemMax, m.getHeapMemoryMaximum());

            totalHeapMemInit += m.getHeapMemoryInitialized();

            totalNonHeapMemCmt += m.getNonHeapMemoryCommitted();

            totalNonHeapMemUsed += m.getNonHeapMemoryUsed();

            maxNonHeapMemMax = max(maxNonHeapMemMax, m.getNonHeapMemoryMaximum());

            totalNonHeapMemInit += m.getNonHeapMemoryInitialized();

            maxUpTime = max(maxUpTime, m.getUpTime());

            lastDataVersion = max(lastDataVersion, m.getLastDataVersion());

            sentMessagesCnt += m.getSentMessagesCount();
            sentBytesCnt += m.getSentBytesCount();
            receivedMessagesCnt += m.getReceivedMessagesCount();
            receivedBytesCnt += m.getReceivedBytesCount();
            outboundMessagesQueueSize += m.getOutboundMessagesQueueSize();
        }

        avgActJobs /= size;
        avgCancelJobs /= size;
        avgRejectJobs /= size;
        avgWaitJobs /= size;
        avgJobExecTime /= size;
        avgJobWaitTime /= size;
        avgIdleTimePercent /= size;
        avgBusyTimePerc /= size;
        avgCpuLoad /= size;

        if (!F.isEmpty(nodes)) {
            oldestNodeStartTime = oldest(nodes).metrics().getNodeStartTime();
        }

        Map<String, Collection<ClusterNode>> neighborhood = U.neighborhood(nodes);

        currentGcCpus = gcCpus(neighborhood);
        totalCpus = cpus(neighborhood);
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveJobs() {
        return maxActJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageActiveJobs() {
        return avgActJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCancelledJobs() {
        return maxCancelJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageCancelledJobs() {
        return avgCancelJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return maxRejectJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return avgRejectJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumWaitingJobs() {
        return maxWaitJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageWaitingJobs() {
        return avgWaitJobs;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobExecuteTime() {
        return maxJobExecTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobExecuteTime() {
        return avgJobExecTime;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobWaitTime() {
        return maxJobWaitTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobWaitTime() {
        return avgJobWaitTime;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return maxThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public float getIdleTimePercentage() {
        return avgIdleTimePercent;
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return avgCpuLoad;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCpus() {
        return totalCpus;
    }

    /** {@inheritDoc} */
    @Override public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobs() {
        return curActJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitingJobs() {
        return curWaitJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return curRejectJobs;
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return totalRejectJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentCancelledJobs() {
        return curCancelJobs;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCancelledJobs() {
        return totalCancelJobs;
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedJobs() {
        return totalExecutedJobs;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobWaitTime() {
        return minJobWaitTime;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobExecuteTime() {
        return maxJobExecTime;
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedTasks() {
        return totalExecutedTask;
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return totalBusyTime;
    }

    /** {@inheritDoc} */
    @Override public long getTotalIdleTime() {
        return totalIdleTime;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentIdleTime() {
        return minIdleTime;
    }

    /** {@inheritDoc} */
    @Override public float getBusyTimePercentage() {
        return avgBusyTimePerc;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return totalCpus;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return currentGcCpus;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return totalHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return totalHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return totalHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return maxHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return totalNonHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        return totalNonHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return totalNonHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return maxNonHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return maxUpTime;
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return oldestNodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return oldestNodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return curThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return totalStartedThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return curDaemonThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return lastDataVersion;
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return sentMessagesCnt;
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return sentBytesCnt;
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return receivedMessagesCnt;
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return receivedBytesCnt;
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return outboundMessagesQueueSize;
    }

    /**
     * Gets the youngest node in given collection.
     *
     * @param nodes Nodes.
     * @return Youngest node or {@code null} if collection is empty.
     */
    @Nullable private static ClusterNode youngest(Collection<ClusterNode> nodes) {
        long max = Long.MIN_VALUE;

        ClusterNode youngest = null;

        for (ClusterNode n : nodes)
            if (n.order() > max) {
                max = n.order();
                youngest = n;
            }

        return youngest;
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

    private static int cpus(Map<String, Collection<ClusterNode>> neighborhood) {
        int cpus = 0;

        for (Collection<ClusterNode> nodes : neighborhood.values()) {
            ClusterNode first = F.first(nodes);

            // Projection can be empty if all nodes in it failed.
            if (first != null)
                cpus += first.metrics().getTotalCpus();
        }

        return cpus;
    }

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsImpl.class, this);
    }
}

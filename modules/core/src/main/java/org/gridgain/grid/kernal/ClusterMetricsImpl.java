/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static java.lang.Math.*;

/**
 * Implementation for {@link org.apache.ignite.cluster.ClusterMetrics} interface.
 */
class ClusterMetricsImpl implements ClusterMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int minActJobs = Integer.MAX_VALUE;

    /** */
    private int maxActJobs = Integer.MIN_VALUE;

    /** */
    private float avgActJobs;

    /** */
    private int minCancelJobs = Integer.MAX_VALUE;

    /** */
    private int maxCancelJobs = Integer.MIN_VALUE;

    /** */
    private float avgCancelJobs;

    /** */
    private int minRejectJobs = Integer.MAX_VALUE;

    /** */
    private int maxRejectJobs = Integer.MIN_VALUE;

    /** */
    private float avgRejectJobs;

    /** */
    private int minWaitJobs = Integer.MAX_VALUE;

    /** */
    private int maxWaitJobs = Integer.MIN_VALUE;

    /** */
    private float avgWaitJobs;

    /** */
    private long minJobExecTime = Long.MAX_VALUE;

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
    private int minDaemonThreadCnt = Integer.MAX_VALUE;

    /** */
    private int maxDaemonThreadCnt = Integer.MIN_VALUE;

    /** */
    private float avgDaemonThreadCnt;

    /** */
    private int minThreadCnt = Integer.MAX_VALUE;

    /** */
    private int maxThreadCnt = Integer.MIN_VALUE;

    /** */
    private float avgThreadCnt;

    /** */
    private long minIdleTime = Long.MAX_VALUE;

    /** */
    private long maxIdleTime = Long.MIN_VALUE;

    /** */
    private double avgIdleTime;

    /** */
    private float avgIdleTimePercent;

    /** */
    private float minBusyTimePerc = Float.POSITIVE_INFINITY;

    /** */
    private float maxBusyTimePerc = Float.NEGATIVE_INFINITY;

    /** */
    private float avgBusyTimePerc;

    /** */
    private double minCpuLoad = Double.POSITIVE_INFINITY;

    /** */
    private double maxCpuLoad = Double.NEGATIVE_INFINITY;

    /** */
    private double avgCpuLoad;

    /** */
    private long minHeapMemCmt = Long.MAX_VALUE;

    /** */
    private long maxHeapMemCmt = Long.MIN_VALUE;

    /** */
    private double avgHeapMemCmt;

    /** */
    private long minHeapMemUsed = Long.MAX_VALUE;

    /** */
    private long maxHeapMemUsed = Long.MIN_VALUE;

    /** */
    private double avgHeapMemUsed;

    /** */
    private long minHeapMemMax = Long.MAX_VALUE;

    /** */
    private long maxHeapMemMax = Long.MIN_VALUE;

    /** */
    private double avgHeapMemMax;

    /** */
    private long minHeapMemInit = Long.MAX_VALUE;

    /** */
    private long maxHeapMemInit = Long.MIN_VALUE;

    /** */
    private double avgHeapMemInit;

    /** */
    private long minNonHeapMemCmt = Long.MAX_VALUE;

    /** */
    private long maxNonHeapMemCmt = Long.MIN_VALUE;

    /** */
    private double avgNonHeapMemCmt;

    /** */
    private long minNonHeapMemUsed = Long.MAX_VALUE;

    /** */
    private long maxNonHeapMemUsed = Long.MIN_VALUE;

    /** */
    private double avgNonHeapMemUsed;

    /** */
    private long minNonHeapMemMax = Long.MAX_VALUE;

    /** */
    private long maxNonHeapMemMax = Long.MIN_VALUE;

    /** */
    private double avgNonHeapMemMax;

    /** */
    private long minNonHeapMemInit = Long.MAX_VALUE;

    /** */
    private long maxNonHeapMemInit = Long.MIN_VALUE;

    /** */
    private double avgNonHeapMemInit;

    /** */
    private long youngestNodeStartTime = Long.MIN_VALUE;

    /** */
    private long oldestNodeStartTime = Long.MAX_VALUE;

    /** */
    private long minUpTime = Long.MAX_VALUE;

    /** */
    private long maxUpTime = Long.MIN_VALUE;

    /** */
    private double avgUpTime;

    /** */
    private int minCpusPerNode = Integer.MAX_VALUE;

    /** */
    private int maxCpusPerNode = Integer.MIN_VALUE;

    /** */
    private float avgCpusPerNode;

    /** */
    private int minNodesPerHost = Integer.MAX_VALUE;

    /** */
    private int maxNodesPerHost = Integer.MIN_VALUE;

    /** */
    private float avgNodesPerHost;

    /** */
    private int totalCpus;

    /** */
    private int totalHosts;

    /** */
    private int totalNodes;

    /**
     * @param p Projection to get metrics for.
     */
    ClusterMetricsImpl(ClusterGroup p) {
        assert p != null;

        Collection<ClusterNode> nodes = p.nodes();

        int size = nodes.size();

        for (ClusterNode node : nodes) {
            ClusterNodeMetrics m = node.metrics();

            minActJobs = min(minActJobs, m.getCurrentActiveJobs());
            maxActJobs = max(maxActJobs, m.getCurrentActiveJobs());
            avgActJobs += m.getCurrentActiveJobs();

            minCancelJobs = min(minCancelJobs, m.getCurrentCancelledJobs());
            maxCancelJobs = max(maxCancelJobs, m.getCurrentCancelledJobs());
            avgCancelJobs += m.getCurrentCancelledJobs();

            minRejectJobs = min(minRejectJobs, m.getCurrentRejectedJobs());
            maxRejectJobs = max(maxRejectJobs, m.getCurrentRejectedJobs());
            avgRejectJobs += m.getCurrentRejectedJobs();

            minWaitJobs = min(minWaitJobs, m.getCurrentWaitingJobs());
            maxWaitJobs = max(maxWaitJobs, m.getCurrentWaitingJobs());
            avgWaitJobs += m.getCurrentWaitingJobs();

            minJobExecTime = min(minJobExecTime, m.getCurrentJobExecuteTime());
            maxJobExecTime = max(maxJobExecTime, m.getCurrentJobExecuteTime());
            avgJobExecTime += m.getCurrentJobExecuteTime();

            minJobWaitTime = min(minJobWaitTime, m.getCurrentJobWaitTime());
            maxJobWaitTime = max(maxJobWaitTime, m.getCurrentJobWaitTime());
            avgJobWaitTime += m.getCurrentJobWaitTime();

            minDaemonThreadCnt = min(minDaemonThreadCnt, m.getCurrentDaemonThreadCount());
            maxDaemonThreadCnt = max(maxDaemonThreadCnt, m.getCurrentDaemonThreadCount());
            avgDaemonThreadCnt += m.getCurrentDaemonThreadCount();

            minThreadCnt = min(minThreadCnt, m.getCurrentThreadCount());
            maxThreadCnt = max(maxThreadCnt, m.getCurrentThreadCount());
            avgThreadCnt += m.getCurrentThreadCount();

            minIdleTime = min(minIdleTime, m.getCurrentIdleTime());
            maxIdleTime = max(maxIdleTime, m.getCurrentIdleTime());
            avgIdleTime += m.getCurrentIdleTime();
            avgIdleTimePercent += m.getIdleTimePercentage();

            minBusyTimePerc = min(minBusyTimePerc, m.getBusyTimePercentage());
            maxBusyTimePerc = max(maxBusyTimePerc, m.getBusyTimePercentage());
            avgBusyTimePerc += m.getBusyTimePercentage();

            minCpuLoad = min(minCpuLoad, m.getCurrentCpuLoad());
            maxCpuLoad = max(maxCpuLoad, m.getCurrentCpuLoad());
            avgCpuLoad += m.getCurrentCpuLoad();

            minHeapMemCmt = min(minHeapMemCmt, m.getHeapMemoryCommitted());
            maxHeapMemCmt = max(maxHeapMemCmt, m.getHeapMemoryCommitted());
            avgHeapMemCmt += m.getHeapMemoryCommitted();

            minHeapMemUsed = min(minHeapMemUsed, m.getHeapMemoryUsed());
            maxHeapMemUsed = max(maxHeapMemUsed, m.getHeapMemoryUsed());
            avgHeapMemUsed += m.getHeapMemoryUsed();

            minHeapMemMax = min(minHeapMemMax, m.getHeapMemoryMaximum());
            maxHeapMemMax = max(maxHeapMemMax, m.getHeapMemoryMaximum());
            avgHeapMemMax += m.getHeapMemoryMaximum();

            minHeapMemInit = min(minHeapMemInit, m.getHeapMemoryInitialized());
            maxHeapMemInit = max(maxHeapMemInit, m.getHeapMemoryInitialized());
            avgHeapMemInit += m.getHeapMemoryInitialized();

            minNonHeapMemCmt = min(minNonHeapMemCmt, m.getNonHeapMemoryCommitted());
            maxNonHeapMemCmt = max(maxNonHeapMemCmt, m.getNonHeapMemoryCommitted());
            avgNonHeapMemCmt += m.getNonHeapMemoryCommitted();

            minNonHeapMemUsed = min(minNonHeapMemUsed, m.getNonHeapMemoryUsed());
            maxNonHeapMemUsed = max(maxNonHeapMemUsed, m.getNonHeapMemoryUsed());
            avgNonHeapMemUsed += m.getNonHeapMemoryUsed();

            minNonHeapMemMax = min(minNonHeapMemMax, m.getNonHeapMemoryMaximum());
            maxNonHeapMemMax = max(maxNonHeapMemMax, m.getNonHeapMemoryMaximum());
            avgNonHeapMemMax += m.getNonHeapMemoryMaximum();

            minNonHeapMemInit = min(minNonHeapMemInit, m.getNonHeapMemoryInitialized());
            maxNonHeapMemInit = max(maxNonHeapMemInit, m.getNonHeapMemoryInitialized());
            avgNonHeapMemInit += m.getNonHeapMemoryInitialized();

            minUpTime = min(minUpTime, m.getUpTime());
            maxUpTime = max(maxUpTime, m.getUpTime());
            avgUpTime += m.getUpTime();

            minCpusPerNode = min(minCpusPerNode, m.getTotalCpus());
            maxCpusPerNode = max(maxCpusPerNode, m.getTotalCpus());
            avgCpusPerNode += m.getTotalCpus();
        }

        avgActJobs /= size;
        avgCancelJobs /= size;
        avgRejectJobs /= size;
        avgWaitJobs /= size;
        avgJobExecTime /= size;
        avgJobWaitTime /= size;
        avgDaemonThreadCnt /= size;
        avgThreadCnt /= size;
        avgIdleTime /= size;
        avgIdleTimePercent /= size;
        avgBusyTimePerc /= size;
        avgCpuLoad /= size;
        avgHeapMemCmt /= size;
        avgHeapMemUsed /= size;
        avgHeapMemMax /= size;
        avgHeapMemInit /= size;
        avgNonHeapMemCmt /= size;
        avgNonHeapMemUsed /= size;
        avgNonHeapMemMax /= size;
        avgNonHeapMemInit /= size;
        avgUpTime /= size;
        avgCpusPerNode /= size;

        if (!F.isEmpty(nodes)) {
            youngestNodeStartTime = youngest(nodes).metrics().getNodeStartTime();
            oldestNodeStartTime = oldest(nodes).metrics().getNodeStartTime();
        }

        Map<String, Collection<ClusterNode>> neighborhood = U.neighborhood(nodes);

        for (Collection<ClusterNode> neighbors : neighborhood.values()) {
            minNodesPerHost = min(minNodesPerHost, neighbors.size());
            maxNodesPerHost = max(maxNodesPerHost, neighbors.size());
            avgNodesPerHost += neighbors.size();
        }

        if (!F.isEmpty(neighborhood))
            avgNodesPerHost /= neighborhood.size();

        totalCpus = cpus(neighborhood);
        totalHosts = neighborhood.size();
        totalNodes = nodes.size();
    }

    /** {@inheritDoc} */
    @Override public int getMinimumActiveJobs() {
        return minActJobs;
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
    @Override public int getMinimumCancelledJobs() {
        return minCancelJobs;
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
    @Override public int getMinimumRejectedJobs() {
        return minRejectJobs;
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
    @Override public int getMinimumWaitingJobs() {
        return minWaitJobs;
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
    @Override public long getMinimumJobExecuteTime() {
        return minJobExecTime;
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
    @Override public long getMinimumJobWaitTime() {
        return minJobWaitTime;
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
    @Override public int getMinimumDaemonThreadCount() {
        return minDaemonThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumDaemonThreadCount() {
        return maxDaemonThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public float getAverageDaemonThreadCount() {
        return avgDaemonThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumThreadCount() {
        return minThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return maxThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public float getAverageThreadCount() {
        return avgThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumIdleTime() {
        return minIdleTime;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumIdleTime() {
        return maxIdleTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageIdleTime() {
        return avgIdleTime;
    }

    /** {@inheritDoc} */
    @Override public float getIdleTimePercentage() {
        return avgIdleTimePercent;
    }

    /** {@inheritDoc} */
    @Override public float getMinimumBusyTimePercentage() {
        return minBusyTimePerc;
    }

    /** {@inheritDoc} */
    @Override public float getMaximumBusyTimePercentage() {
        return maxBusyTimePerc;
    }

    /** {@inheritDoc} */
    @Override public float getAverageBusyTimePercentage() {
        return avgBusyTimePerc;
    }

    /** {@inheritDoc} */
    @Override public double getMinimumCpuLoad() {
        return minCpuLoad;
    }

    /** {@inheritDoc} */
    @Override public double getMaximumCpuLoad() {
        return maxCpuLoad;
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return avgCpuLoad;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumHeapMemoryCommitted() {
        return minHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumHeapMemoryCommitted() {
        return maxHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public double getAverageHeapMemoryCommitted() {
        return avgHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumHeapMemoryUsed() {
        return minHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumHeapMemoryUsed() {
        return maxHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public double getAverageHeapMemoryUsed() {
        return avgHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumHeapMemoryMaximum() {
        return minHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumHeapMemoryMaximum() {
        return maxHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public double getAverageHeapMemoryMaximum() {
        return avgHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumHeapMemoryInitialized() {
        return minHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumHeapMemoryInitialized() {
        return maxHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public double getAverageHeapMemoryInitialized() {
        return avgHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumNonHeapMemoryCommitted() {
        return minNonHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumNonHeapMemoryCommitted() {
        return maxNonHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public double getAverageNonHeapMemoryCommitted() {
        return avgNonHeapMemCmt;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumNonHeapMemoryUsed() {
        return minNonHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumNonHeapMemoryUsed() {
        return maxNonHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public double getAverageNonHeapMemoryUsed() {
        return avgNonHeapMemUsed;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumNonHeapMemoryMaximum() {
        return minNonHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumNonHeapMemoryMaximum() {
        return maxNonHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public double getAverageNonHeapMemoryMaximum() {
        return avgNonHeapMemMax;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumNonHeapMemoryInitialized() {
        return minNonHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumNonHeapMemoryInitialized() {
        return maxNonHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public double getAverageNonHeapMemoryInitialized() {
        return avgNonHeapMemInit;
    }

    /** {@inheritDoc} */
    @Override public long getYoungestNodeStartTime() {
        return youngestNodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public long getOldestNodeStartTime() {
        return oldestNodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumUpTime() {
        return minUpTime;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumUpTime() {
        return maxUpTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageUpTime() {
        return avgUpTime;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumCpusPerNode() {
        return minCpusPerNode;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCpusPerNode() {
        return maxCpusPerNode;
    }

    /** {@inheritDoc} */
    @Override public float getAverageCpusPerNode() {
        return avgCpusPerNode;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumNodesPerHost() {
        return minNodesPerHost;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumNodesPerHost() {
        return maxNodesPerHost;
    }

    /** {@inheritDoc} */
    @Override public float getAverageNodesPerHost() {
        return avgNodesPerHost;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCpus() {
        return totalCpus;
    }

    /** {@inheritDoc} */
    @Override public int getTotalHosts() {
        return totalHosts;
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return totalNodes;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsImpl.class, this);
    }
}

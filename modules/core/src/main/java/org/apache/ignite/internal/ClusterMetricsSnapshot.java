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
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Implementation for {@link ClusterMetrics} interface.
 * <p>
 * Note that whenever adding or removing metric parameters, care
 * must be taken to update serialize/deserialize logic as well.
 */
public class ClusterMetricsSnapshot implements ClusterMetrics, Message {
    /** */
    public static final short TYPE_CODE = 137;

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
    @Order(value = 0, method = "lastUpdateTime", getter = true)
    private long lastUpdateTime = -1;

    /** */
    @Order(value = 1, method = "maximumActiveJobs", getter = true)
    private int maxActiveJobs = -1;

    /** */
    @Order(value = 2, method = "currentActiveJobs", getter = true)
    private int curActiveJobs = -1;

    /** */
    @Order(value = 3, method = "averageActiveJobs", getter = true)
    private float avgActiveJobs = -1;

    /** */
    @Order(value = 4, method = "maximumWaitingJobs", getter = true)
    private int maxWaitingJobs = -1;

    /** */
    @Order(value = 5, method = "currentWaitingJobs", getter = true)
    private int curWaitingJobs = -1;

    /** */
    @Order(value = 6, method = "averageWaitingJobs", getter = true)
    private float avgWaitingJobs = -1;

    /** */
    @Order(value = 7, method = "maximumRejectedJobs", getter = true)
    private int maxRejectedJobs = -1;

    /** */
    @Order(value = 8, method = "currentRejectedJobs", getter = true)
    private int curRejectedJobs = -1;

    /** */
    @Order(value = 9, method = "averageRejectedJobs", getter = true)
    private float avgRejectedJobs = -1;

    /** */
    @Order(value = 10, method = "maximumCancelledJobs", getter = true)
    private int maxCancelledJobs = -1;

    /** */
    @Order(value = 11, method = "currentCancelledJobs", getter = true)
    private int curCancelledJobs = -1;

    /** */
    @Order(value = 12, method = "averageCancelledJobs", getter = true)
    private float avgCancelledJobs = -1;

    /** */
    @Order(value = 13, getter = true)
    private int totalRejectedJobs = -1;

    /** */
    @Order(value = 14, getter = true)
    private int totalCancelledJobs = -1;

    /** */
    @Order(value = 15, getter = true)
    private int totalExecutedJobs = -1;

    /** */
    @Order(value = 16, method = "maximumJobWaitTime", getter = true)
    private long maxJobWaitTime = -1;

    /** */
    @Order(value = 17, method = "currentJobWaitTime", getter = true)
    private long curJobWaitTime = Long.MAX_VALUE;

    /** */
    @Order(value = 18, method = "averageJobWaitTime", getter = true)
    private double avgJobWaitTime = -1;

    /** */
    @Order(value = 19, method = "maximumJobExecuteTime", getter = true)
    private long maxJobExecTime = -1;

    /** */
    @Order(value = 20, method = "currentJobExecuteTime", getter = true)
    private long curJobExecTime = -1;

    /** */
    @Order(value = 21, method = "averageJobExecuteTime", getter = true)
    private double avgJobExecTime = -1;

    /** */
    @Order(value = 22, method = "totalExecutedTasks", getter = true)
    private int totalExecTasks = -1;

    /** */
    @Order(value = 23, getter = true)
    private long totalIdleTime = -1;

    /** */
    @Order(value = 24, method = "currentIdleTime", getter = true)
    private long curIdleTime = -1;

    /** */
    @Order(value = 25, method = "totalCpus", getter = true)
    private int availProcs = -1;

    /** */
    @Order(value = 26, method = "currentCpuLoad", getter = true)
    private double load = -1;

    /** */
    @Order(value = 27, method = "averageCpuLoad", getter = true)
    private double avgLoad = -1;

    /** */
    @Order(value = 28, method = "currentGcCpuLoad", getter = true)
    private double gcLoad = -1;

    /** */
    @Order(value = 29, method = "heapMemoryInitialized", getter = true)
    private long heapInit = -1;

    /** */
    @Order(value = 30, method = "heapMemoryUsed", getter = true)
    private long heapUsed = -1;

    /** */
    @Order(value = 31, method = "heapMemoryCommitted", getter = true)
    private long heapCommitted = -1;

    /** */
    @Order(value = 32, method = "heapMemoryMaximum", getter = true)
    private long heapMax = -1;

    /** */
    @Order(value = 33, method = "heapMemoryTotal", getter = true)
    private long heapTotal = -1;

    /** */
    @Order(value = 34, method = "heapMemoryInitialized", getter = true)
    private long nonHeapInit = -1;

    /** */
    @Order(value = 35, method = "heapMemoryUsed", getter = true)
    private long nonHeapUsed = -1;

    /** */
    @Order(value = 36, method = "heapMemoryCommitted", getter = true)
    private long nonHeapCommitted = -1;

    /** */
    @Order(value = 37, method = "nonHeapMemoryMaximum", getter = true)
    private long nonHeapMax = -1;

    /** */
    @Order(value = 38, method = "nonHeapMemoryTotal", getter = true)
    private long nonHeapTotal = -1;

    /** */
    @Order(value = 39, getter = true)
    private long upTime = -1;

    /** */
    @Order(value = 40, getter = true)
    private long startTime = -1;

    /** */
    @Order(value = 41, getter = true)
    private long nodeStartTime = -1;

    /** */
    @Order(value = 42, method = "currentThreadCount", getter = true)
    private int threadCnt = -1;

    /** */
    @Order(value = 43, method = "maximumThreadCount", getter = true)
    private int peakThreadCnt = -1;

    /** */
    @Order(value = 44, method = "totalStartedThreadCount", getter = true)
    private long startedThreadCnt = -1;

    /** */
    @Order(value = 45, method = "currentDaemonThreadCount", getter = true)
    private int daemonThreadCnt = -1;

    /** */
    @Order(value = 46, method = "lastDataVersion", getter = true)
    private long lastDataVer = -1;

    /** */
    @Order(value = 47, method = "sentMessagesCount", getter = true)
    private int sentMsgsCnt = -1;

    /** */
    @Order(value = 48, method = "sentBytesCount", getter = true)
    private long sentBytesCnt = -1;

    /** */
    @Order(value = 49, method = "receivedMessagesCount", getter = true)
    private int rcvdMsgsCnt = -1;

    /** */
    @Order(value = 50, method = "receivedBytesCount", getter = true)
    private long rcvdBytesCnt = -1;

    /** */
    @Order(value = 51, method = "outboundMessagesQueueSize", getter = true)
    private int outMesQueueSize = -1;

    /** */
    @Order(value = 52, getter = true)
    private int totalNodes = -1;

    /** */
    @Order(value = 53, method = "totalJobsExecutionTime", getter = true)
    private long totalJobsExecTime = -1;

    /** */
    @Order(value = 54, getter = true)
    private long currentPmeDuration = -1;

    /**
     * Create empty snapshot.
     */
    public ClusterMetricsSnapshot() {
        // No-op.
    }

    /**
     * Create metrics for given nodes.
     *
     * @param nodes Nodes.
     */
    public ClusterMetricsSnapshot(Collection<ClusterNode> nodes) {
        int size = nodes.size();

        curJobWaitTime = Long.MAX_VALUE;
        lastUpdateTime = 0;
        maxActiveJobs = 0;
        curActiveJobs = 0;
        avgActiveJobs = 0;
        maxWaitingJobs = 0;
        curWaitingJobs = 0;
        avgWaitingJobs = 0;
        maxRejectedJobs = 0;
        curRejectedJobs = 0;
        avgRejectedJobs = 0;
        maxCancelledJobs = 0;
        curCancelledJobs = 0;
        avgCancelledJobs = 0;
        totalRejectedJobs = 0;
        totalCancelledJobs = 0;
        totalExecutedJobs = 0;
        totalJobsExecTime = 0;
        maxJobWaitTime = 0;
        avgJobWaitTime = 0;
        maxJobExecTime = 0;
        curJobExecTime = 0;
        avgJobExecTime = 0;
        totalExecTasks = 0;
        totalIdleTime = 0;
        curIdleTime = 0;
        availProcs = 0;
        load = 0;
        avgLoad = 0;
        gcLoad = 0;
        heapInit = 0;
        heapUsed = 0;
        heapCommitted = 0;
        heapMax = 0;
        nonHeapInit = 0;
        nonHeapUsed = 0;
        nonHeapCommitted = 0;
        nonHeapMax = 0;
        nonHeapTotal = 0;
        upTime = 0;
        startTime = 0;
        nodeStartTime = 0;
        threadCnt = 0;
        peakThreadCnt = 0;
        startedThreadCnt = 0;
        daemonThreadCnt = 0;
        lastDataVer = 0;
        sentMsgsCnt = 0;
        sentBytesCnt = 0;
        rcvdMsgsCnt = 0;
        rcvdBytesCnt = 0;
        outMesQueueSize = 0;
        heapTotal = 0;
        totalNodes = nodes.size();
        currentPmeDuration = 0;

        for (ClusterNode node : nodes) {
            ClusterMetrics m = node.metrics();

            lastUpdateTime = max(lastUpdateTime, node.metrics().getLastUpdateTime());

            curActiveJobs += m.getCurrentActiveJobs();
            maxActiveJobs = max(maxActiveJobs, m.getCurrentActiveJobs());
            avgActiveJobs += m.getCurrentActiveJobs();
            totalExecutedJobs += m.getTotalExecutedJobs();
            totalJobsExecTime += m.getTotalJobsExecutionTime();

            totalExecTasks += m.getTotalExecutedTasks();

            totalCancelledJobs += m.getTotalCancelledJobs();
            curCancelledJobs += m.getCurrentCancelledJobs();
            maxCancelledJobs = max(maxCancelledJobs, m.getCurrentCancelledJobs());
            avgCancelledJobs += m.getCurrentCancelledJobs();

            totalRejectedJobs += m.getTotalRejectedJobs();
            curRejectedJobs += m.getCurrentRejectedJobs();
            maxRejectedJobs = max(maxRejectedJobs, m.getCurrentRejectedJobs());
            avgRejectedJobs += m.getCurrentRejectedJobs();

            curWaitingJobs += m.getCurrentWaitingJobs();
            maxWaitingJobs = max(maxWaitingJobs, m.getCurrentWaitingJobs());
            avgWaitingJobs += m.getCurrentWaitingJobs();

            maxJobExecTime = max(maxJobExecTime, m.getMaximumJobExecuteTime());
            avgJobExecTime += m.getAverageJobExecuteTime();
            curJobExecTime += m.getCurrentJobExecuteTime();

            curJobWaitTime = min(curJobWaitTime, m.getCurrentJobWaitTime());
            maxJobWaitTime = max(maxJobWaitTime, m.getCurrentJobWaitTime());
            avgJobWaitTime += m.getAverageJobWaitTime();

            daemonThreadCnt += m.getCurrentDaemonThreadCount();

            peakThreadCnt = max(peakThreadCnt, m.getCurrentThreadCount());
            threadCnt += m.getCurrentThreadCount();
            startedThreadCnt += m.getTotalStartedThreadCount();

            curIdleTime += m.getCurrentIdleTime();
            totalIdleTime += m.getTotalIdleTime();

            heapCommitted += m.getHeapMemoryCommitted();

            heapUsed += m.getHeapMemoryUsed();

            heapMax = max(heapMax, m.getHeapMemoryMaximum());

            heapTotal += m.getHeapMemoryTotal();

            heapInit += m.getHeapMemoryInitialized();

            nonHeapCommitted += m.getNonHeapMemoryCommitted();

            nonHeapUsed += m.getNonHeapMemoryUsed();

            nonHeapMax = max(nonHeapMax, m.getNonHeapMemoryMaximum());

            nonHeapTotal += m.getNonHeapMemoryTotal();

            nonHeapInit += m.getNonHeapMemoryInitialized();

            upTime = max(upTime, m.getUpTime());

            lastDataVer = max(lastDataVer, m.getLastDataVersion());

            sentMsgsCnt += m.getSentMessagesCount();
            sentBytesCnt += m.getSentBytesCount();
            rcvdMsgsCnt += m.getReceivedMessagesCount();
            rcvdBytesCnt += m.getReceivedBytesCount();
            outMesQueueSize += m.getOutboundMessagesQueueSize();

            avgLoad += m.getCurrentCpuLoad();

            currentPmeDuration = max(currentPmeDuration, m.getCurrentPmeDuration());
        }

        curJobExecTime /= size;

        avgActiveJobs /= size;
        avgCancelledJobs /= size;
        avgRejectedJobs /= size;
        avgWaitingJobs /= size;
        avgJobExecTime /= size;
        avgJobWaitTime /= size;
        avgLoad /= size;

        if (!F.isEmpty(nodes)) {
            ClusterMetrics oldestNodeMetrics = oldest(nodes).metrics();

            nodeStartTime = oldestNodeMetrics.getNodeStartTime();
            startTime = oldestNodeMetrics.getStartTime();
        }

        Map<String, Collection<ClusterNode>> neighborhood = U.neighborhood(nodes);

        gcLoad = gcCpus(neighborhood);
        load = cpus(neighborhood);
        availProcs = cpuCnt(neighborhood);
    }

    /** */
    public ClusterMetricsSnapshot(ClusterMetrics metrics) {
        maxActiveJobs = metrics.getMaximumActiveJobs();
        curActiveJobs = metrics.getCurrentActiveJobs();
        avgActiveJobs = metrics.getAverageActiveJobs();

        maxWaitingJobs = metrics.getMaximumWaitingJobs();
        curWaitingJobs = metrics.getCurrentWaitingJobs();
        avgWaitingJobs = metrics.getAverageWaitingJobs();

        maxRejectedJobs = metrics.getMaximumRejectedJobs();
        curRejectedJobs = metrics.getCurrentRejectedJobs();
        avgRejectedJobs = metrics.getAverageRejectedJobs();

        maxCancelledJobs = metrics.getMaximumCancelledJobs();
        curCancelledJobs = metrics.getCurrentCancelledJobs();
        avgCancelledJobs = metrics.getAverageCancelledJobs();

        totalRejectedJobs = metrics.getTotalRejectedJobs();
        totalCancelledJobs = metrics.getTotalCancelledJobs();
        totalExecutedJobs = metrics.getTotalExecutedJobs();

        maxJobWaitTime = metrics.getMaximumJobWaitTime();
        curJobWaitTime = metrics.getCurrentJobWaitTime();
        avgJobWaitTime = metrics.getAverageJobWaitTime();

        maxJobExecTime = metrics.getMaximumJobExecuteTime();
        curJobExecTime = metrics.getCurrentJobExecuteTime();
        avgJobExecTime = metrics.getAverageJobExecuteTime();

        totalJobsExecTime = metrics.getTotalJobsExecutionTime();
        totalExecTasks = metrics.getTotalExecutedTasks();

        curIdleTime = metrics.getCurrentIdleTime();
        totalIdleTime = metrics.getTotalIdleTime();

        availProcs = metrics.getTotalCpus();
        load = metrics.getCurrentCpuLoad();
        avgLoad = metrics.getAverageCpuLoad();
        gcLoad = metrics.getCurrentGcCpuLoad();

        heapInit = metrics.getHeapMemoryInitialized();
        heapUsed = metrics.getHeapMemoryUsed();
        heapCommitted = metrics.getHeapMemoryCommitted();
        heapMax = metrics.getHeapMemoryMaximum();
        heapTotal = metrics.getHeapMemoryTotal();

        nonHeapInit = metrics.getNonHeapMemoryInitialized();
        nonHeapUsed = metrics.getNonHeapMemoryUsed();
        nonHeapCommitted = metrics.getNonHeapMemoryCommitted();
        nonHeapMax = metrics.getNonHeapMemoryMaximum();
        nonHeapTotal = metrics.getNonHeapMemoryTotal();

        startTime = metrics.getStartTime();
        nodeStartTime = metrics.getNodeStartTime();
        upTime = metrics.getUpTime();

        lastDataVer = metrics.getLastDataVersion();

        currentPmeDuration = metrics.getCurrentPmeDuration();

        totalNodes = metrics.getTotalNodes();

        threadCnt = metrics.getCurrentThreadCount();
        peakThreadCnt = metrics.getMaximumThreadCount();
        startedThreadCnt = metrics.getTotalStartedThreadCount();
        daemonThreadCnt = metrics.getCurrentDaemonThreadCount();

        sentMsgsCnt = metrics.getSentMessagesCount();
        rcvdMsgsCnt = metrics.getReceivedMessagesCount();
        outMesQueueSize = metrics.getOutboundMessagesQueueSize();

        sentBytesCnt = metrics.getSentBytesCount();
        rcvdBytesCnt = metrics.getReceivedBytesCount();
    }

    /** */
    public static ClusterMetricsSnapshot of(ClusterMetrics metrics) {
        return metrics instanceof ClusterMetricsSnapshot ? (ClusterMetricsSnapshot)metrics : new ClusterMetricsSnapshot(metrics);
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return heapTotal;
    }

    /**
     * Sets total heap size.
     *
     * @param heapTotal Total heap.
     */
    public void heapMemoryTotal(long heapTotal) {
        this.heapTotal = heapTotal;
    }

    /**
     * Sets non-heap total heap size.
     *
     * @param nonHeapTotal Total heap.
     */
    public void nonHeapMemoryTotal(long nonHeapTotal) {
        this.nonHeapTotal = nonHeapTotal;
    }

    /** {@inheritDoc} */
    @Override public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    /**
     * Sets last update time.
     *
     * @param lastUpdateTime Last update time.
     */
    public void lastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveJobs() {
        return maxActiveJobs;
    }

    /**
     * Sets max active jobs.
     *
     * @param maxActiveJobs Max active jobs.
     */
    public void maximumActiveJobs(int maxActiveJobs) {
        this.maxActiveJobs = maxActiveJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobs() {
        return curActiveJobs;
    }

    /**
     * Sets current active jobs.
     *
     * @param curActiveJobs Current active jobs.
     */
    public void currentActiveJobs(int curActiveJobs) {
        this.curActiveJobs = curActiveJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageActiveJobs() {
        return avgActiveJobs;
    }

    /**
     * Sets average active jobs.
     *
     * @param avgActiveJobs Average active jobs.
     */
    public void averageActiveJobs(float avgActiveJobs) {
        this.avgActiveJobs = avgActiveJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumWaitingJobs() {
        return maxWaitingJobs;
    }

    /**
     * Sets maximum waiting jobs.
     *
     * @param maxWaitingJobs Maximum waiting jobs.
     */
    public void maximumWaitingJobs(int maxWaitingJobs) {
        this.maxWaitingJobs = maxWaitingJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitingJobs() {
        return curWaitingJobs;
    }

    /**
     * Sets current waiting jobs.
     *
     * @param curWaitingJobs Current waiting jobs.
     */
    public void currentWaitingJobs(int curWaitingJobs) {
        this.curWaitingJobs = curWaitingJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageWaitingJobs() {
        return avgWaitingJobs;
    }

    /**
     * Sets average waiting jobs.
     *
     * @param avgWaitingJobs Average waiting jobs.
     */
    public void averageWaitingJobs(float avgWaitingJobs) {
        this.avgWaitingJobs = avgWaitingJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return maxRejectedJobs;
    }

    /**
     * @param maxRejectedJobs Maximum number of jobs rejected during a single collision resolution event.
     */
    public void maximumRejectedJobs(int maxRejectedJobs) {
        this.maxRejectedJobs = maxRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return curRejectedJobs;
    }

    /**
     * @param curRejectedJobs Number of jobs rejected during most recent collision resolution.
     */
    public void currentRejectedJobs(int curRejectedJobs) {
        this.curRejectedJobs = curRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return avgRejectedJobs;
    }

    /**
     * @param avgRejectedJobs Average number of jobs this node rejects.
     */
    public void averageRejectedJobs(float avgRejectedJobs) {
        this.avgRejectedJobs = avgRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return totalRejectedJobs;
    }

    /**
     * @param totalRejectedJobs Total number of jobs this node ever rejected.
     */
    public void totalRejectedJobs(int totalRejectedJobs) {
        this.totalRejectedJobs = totalRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumCancelledJobs() {
        return maxCancelledJobs;
    }

    /**
     * Sets maximum cancelled jobs.
     *
     * @param maxCancelledJobs Maximum cancelled jobs.
     */
    public void maximumCancelledJobs(int maxCancelledJobs) {
        this.maxCancelledJobs = maxCancelledJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentCancelledJobs() {
        return curCancelledJobs;
    }

    /**
     * Sets current cancelled jobs.
     *
     * @param curCancelledJobs Current cancelled jobs.
     */
    public void currentCancelledJobs(int curCancelledJobs) {
        this.curCancelledJobs = curCancelledJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageCancelledJobs() {
        return avgCancelledJobs;
    }

    /**
     * Sets average cancelled jobs.
     *
     * @param avgCancelledJobs Average cancelled jobs.
     */
    public void averageCancelledJobs(float avgCancelledJobs) {
        this.avgCancelledJobs = avgCancelledJobs;
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedJobs() {
        return totalExecutedJobs;
    }

    /**
     * Sets total active jobs.
     *
     * @param totalExecutedJobs Total active jobs.
     */
    public void totalExecutedJobs(int totalExecutedJobs) {
        this.totalExecutedJobs = totalExecutedJobs;
    }

    /** {@inheritDoc} */
    @Override public long getTotalJobsExecutionTime() {
        return totalJobsExecTime;
    }

    /**
     * Sets total jobs execution time.
     *
     * @param totalJobsExecTime Total jobs execution time.
     */
    public void totalJobsExecutionTime(long totalJobsExecTime) {
        this.totalJobsExecTime = totalJobsExecTime;
    }

    /** {@inheritDoc} */
    @Override public int getTotalCancelledJobs() {
        return totalCancelledJobs;
    }

    /**
     * Sets total cancelled jobs.
     *
     * @param totalCancelledJobs Total cancelled jobs.
     */
    public void totalCancelledJobs(int totalCancelledJobs) {
        this.totalCancelledJobs = totalCancelledJobs;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobWaitTime() {
        return maxJobWaitTime;
    }

    /**
     * Sets max job wait time.
     *
     * @param maxJobWaitTime Max job wait time.
     */
    public void maximumJobWaitTime(long maxJobWaitTime) {
        this.maxJobWaitTime = maxJobWaitTime;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobWaitTime() {
        return curJobWaitTime;
    }

    /**
     * Sets current job wait time.
     *
     * @param curJobWaitTime Current job wait time.
     */
    public void currentJobWaitTime(long curJobWaitTime) {
        this.curJobWaitTime = curJobWaitTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobWaitTime() {
        return avgJobWaitTime;
    }

    /**
     * Sets average job wait time.
     *
     * @param avgJobWaitTime Average job wait time.
     */
    public void averageJobWaitTime(double avgJobWaitTime) {
        this.avgJobWaitTime = avgJobWaitTime;
    }

    /** {@inheritDoc} */
    @Override public long getMaximumJobExecuteTime() {
        return maxJobExecTime;
    }

    /**
     * Sets maximum job execution time.
     *
     * @param maxJobExecTime Maximum job execution time.
     */
    public void maximumJobExecuteTime(long maxJobExecTime) {
        this.maxJobExecTime = maxJobExecTime;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentJobExecuteTime() {
        return curJobExecTime;
    }

    /**
     * Sets current job execute time.
     *
     * @param curJobExecTime Current job execute time.
     */
    public void currentJobExecuteTime(long curJobExecTime) {
        this.curJobExecTime = curJobExecTime;
    }

    /** {@inheritDoc} */
    @Override public double getAverageJobExecuteTime() {
        return avgJobExecTime;
    }

    /**
     * Sets average job execution time.
     *
     * @param avgJobExecTime Average job execution time.
     */
    public void averageJobExecuteTime(double avgJobExecTime) {
        this.avgJobExecTime = avgJobExecTime;
    }

    /** {@inheritDoc} */
    @Override public int getTotalExecutedTasks() {
        return totalExecTasks;
    }

    /**
     * Sets total executed tasks count.
     *
     * @param totalExecTasks total executed tasks count.
     */
    public void totalExecutedTasks(int totalExecTasks) {
        this.totalExecTasks = totalExecTasks;
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return getUpTime() - getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public long getTotalIdleTime() {
        return totalIdleTime;
    }

    /**
     * Set total node idle time.
     *
     * @param totalIdleTime Total node idle time.
     */
    public void totalIdleTime(long totalIdleTime) {
        this.totalIdleTime = totalIdleTime;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentIdleTime() {
        return curIdleTime;
    }

    /**
     * Sets time elapsed since execution of last job.
     *
     * @param curIdleTime Time elapsed since execution of last job.
     */
    public void currentIdleTime(long curIdleTime) {
        this.curIdleTime = curIdleTime;
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
        return availProcs;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return load;
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return avgLoad;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return gcLoad;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return heapInit;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return heapUsed;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return heapCommitted;
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return heapMax;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return nonHeapInit;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        return nonHeapUsed;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return nonHeapCommitted;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return nonHeapMax;
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryTotal() {
        return nonHeapTotal;
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return upTime;
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long getNodeStartTime() {
        return nodeStartTime;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentThreadCount() {
        return threadCnt;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumThreadCount() {
        return peakThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return startedThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentDaemonThreadCount() {
        return daemonThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public long getLastDataVersion() {
        return lastDataVer;
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return sentMsgsCnt;
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return sentBytesCnt;
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return rcvdMsgsCnt;
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return rcvdBytesCnt;
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return outMesQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTotalNodes() {
        return totalNodes;
    }

    /** {@inheritDoc} */
    @Override public long getCurrentPmeDuration() {
        return currentPmeDuration;
    }

    /**
     * Sets available processors.
     *
     * @param availProcs Available processors.
     */
    public void totalCpus(int availProcs) {
        this.availProcs = availProcs;
    }

    /**
     * Sets current CPU load.
     *
     * @param load Current CPU load.
     */
    public void currentCpuLoad(double load) {
        this.load = load;
    }

    /**
     * Sets CPU load average over the metrics history.
     *
     * @param avgLoad CPU load average.
     */
    public void averageCpuLoad(double avgLoad) {
        this.avgLoad = avgLoad;
    }

    /**
     * Sets current GC load.
     *
     * @param gcLoad Current GC load.
     */
    public void currentGcCpuLoad(double gcLoad) {
        this.gcLoad = gcLoad;
    }

    /**
     * Sets heap initial memory.
     *
     * @param heapInit Heap initial memory.
     */
    public void heapMemoryInitialized(long heapInit) {
        this.heapInit = heapInit;
    }

    /**
     * Sets used heap memory.
     *
     * @param heapUsed Used heap memory.
     */
    public void heapMemoryUsed(long heapUsed) {
        this.heapUsed = heapUsed;
    }

    /**
     * Sets committed heap memory.
     *
     * @param heapCommitted Committed heap memory.
     */
    public void heapMemoryCommitted(long heapCommitted) {
        this.heapCommitted = heapCommitted;
    }

    /**
     * Sets maximum possible heap memory.
     *
     * @param heapMax Maximum possible heap memory.
     */
    public void heapMemoryMaximum(long heapMax) {
        this.heapMax = heapMax;
    }

    /**
     * Sets initial non-heap memory.
     *
     * @param nonHeapInit Initial non-heap memory.
     */
    public void nonHeapMemoryInitialized(long nonHeapInit) {
        this.nonHeapInit = nonHeapInit;
    }

    /**
     * Sets used non-heap memory.
     *
     * @param nonHeapUsed Used non-heap memory.
     */
    public void nonHeapMemoryUsed(long nonHeapUsed) {
        this.nonHeapUsed = nonHeapUsed;
    }

    /**
     * Sets committed non-heap memory.
     *
     * @param nonHeapCommitted Committed non-heap memory.
     */
    public void nonHeapMemoryCommitted(long nonHeapCommitted) {
        this.nonHeapCommitted = nonHeapCommitted;
    }

    /**
     * Sets maximum possible non-heap memory.
     *
     * @param nonHeapMax Maximum possible non-heap memory.
     */
    public void nonHeapMemoryMaximum(long nonHeapMax) {
        this.nonHeapMax = nonHeapMax;
    }

    /**
     * Sets VM up time.
     *
     * @param upTime VM up time.
     */
    public void upTime(long upTime) {
        this.upTime = upTime;
    }

    /**
     * Sets VM start time.
     *
     * @param startTime VM start time.
     */
    public void startTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Sets node start time.
     *
     * @param nodeStartTime node start time.
     */
    public void nodeStartTime(long nodeStartTime) {
        this.nodeStartTime = nodeStartTime;
    }

    /**
     * Sets thread count.
     *
     * @param threadCnt Thread count.
     */
    public void currentThreadCount(int threadCnt) {
        this.threadCnt = threadCnt;
    }

    /**
     * Sets peak thread count.
     *
     * @param peakThreadCnt Peak thread count.
     */
    public void maximumThreadCount(int peakThreadCnt) {
        this.peakThreadCnt = peakThreadCnt;
    }

    /**
     * Sets started thread count.
     *
     * @param startedThreadCnt Started thread count.
     */
    public void totalStartedThreadCount(long startedThreadCnt) {
        this.startedThreadCnt = startedThreadCnt;
    }

    /**
     * Sets daemon thread count.
     *
     * @param daemonThreadCnt Daemon thread count.
     */
    public void currentDaemonThreadCount(int daemonThreadCnt) {
        this.daemonThreadCnt = daemonThreadCnt;
    }

    /**
     * Sets last data version.
     *
     * @param lastDataVer Last data version.
     */
    public void lastDataVersion(long lastDataVer) {
        this.lastDataVer = lastDataVer;
    }

    /**
     * Sets sent messages count.
     *
     * @param sentMsgsCnt Sent messages count.
     */
    public void sentMessagesCount(int sentMsgsCnt) {
        this.sentMsgsCnt = sentMsgsCnt;
    }

    /**
     * Sets sent bytes count.
     *
     * @param sentBytesCnt Sent bytes count.
     */
    public void sentBytesCount(long sentBytesCnt) {
        this.sentBytesCnt = sentBytesCnt;
    }

    /**
     * Sets received messages count.
     *
     * @param rcvdMsgsCnt Received messages count.
     */
    public void receivedMessagesCount(int rcvdMsgsCnt) {
        this.rcvdMsgsCnt = rcvdMsgsCnt;
    }

    /**
     * Sets received bytes count.
     *
     * @param rcvdBytesCnt Received bytes count.
     */
    public void receivedBytesCount(long rcvdBytesCnt) {
        this.rcvdBytesCnt = rcvdBytesCnt;
    }

    /**
     * Sets outbound messages queue size.
     *
     * @param outMesQueueSize Outbound messages queue size.
     */
    public void outboundMessagesQueueSize(int outMesQueueSize) {
        this.outMesQueueSize = outMesQueueSize;
    }

    /**
     * Sets total number of nodes.
     *
     * @param totalNodes Total number of nodes.
     */
    public void totalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
    }

    /**
     * Sets execution duration for current partition map exchange.
     *
     * @param currentPmeDuration Execution duration for current partition map exchange.
     */
    public void currentPmeDuration(long currentPmeDuration) {
        this.currentPmeDuration = currentPmeDuration;
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
        ClusterMetricsSnapshot metrics = new ClusterMetricsSnapshot();

        int bufSize = min(METRICS_SIZE, data.length - off);

        ByteBuffer buf = ByteBuffer.wrap(data, off, bufSize);

        metrics.lastUpdateTime(U.currentTimeMillis());

        metrics.maximumActiveJobs(buf.getInt());
        metrics.currentActiveJobs(buf.getInt());
        metrics.averageActiveJobs(buf.getFloat());
        metrics.maximumWaitingJobs(buf.getInt());
        metrics.currentWaitingJobs(buf.getInt());
        metrics.averageWaitingJobs(buf.getFloat());
        metrics.maximumRejectedJobs(buf.getInt());
        metrics.currentRejectedJobs(buf.getInt());
        metrics.averageRejectedJobs(buf.getFloat());
        metrics.maximumCancelledJobs(buf.getInt());
        metrics.currentCancelledJobs(buf.getInt());
        metrics.averageCancelledJobs(buf.getFloat());
        metrics.totalRejectedJobs(buf.getInt());
        metrics.totalCancelledJobs(buf.getInt());
        metrics.totalExecutedJobs(buf.getInt());
        metrics.maximumJobWaitTime(buf.getLong());
        metrics.currentJobWaitTime(buf.getLong());
        metrics.averageJobWaitTime(buf.getDouble());
        metrics.maximumJobExecuteTime(buf.getLong());
        metrics.currentJobExecuteTime(buf.getLong());
        metrics.averageJobExecuteTime(buf.getDouble());
        metrics.totalExecutedTasks(buf.getInt());
        metrics.currentIdleTime(buf.getLong());
        metrics.totalIdleTime(buf.getLong());
        metrics.totalCpus(buf.getInt());
        metrics.currentCpuLoad(buf.getDouble());
        metrics.averageCpuLoad(buf.getDouble());
        metrics.currentGcCpuLoad(buf.getDouble());
        metrics.heapMemoryInitialized(buf.getLong());
        metrics.heapMemoryUsed(buf.getLong());
        metrics.heapMemoryCommitted(buf.getLong());
        metrics.heapMemoryMaximum(buf.getLong());
        metrics.heapMemoryTotal(buf.getLong());
        metrics.nonHeapMemoryInitialized(buf.getLong());
        metrics.nonHeapMemoryUsed(buf.getLong());
        metrics.nonHeapMemoryCommitted(buf.getLong());
        metrics.nonHeapMemoryMaximum(buf.getLong());
        metrics.nonHeapMemoryTotal(buf.getLong());
        metrics.startTime(buf.getLong());
        metrics.nodeStartTime(buf.getLong());
        metrics.upTime(buf.getLong());
        metrics.currentThreadCount(buf.getInt());
        metrics.maximumThreadCount(buf.getInt());
        metrics.totalStartedThreadCount(buf.getLong());
        metrics.currentDaemonThreadCount(buf.getInt());
        metrics.lastDataVersion(buf.getLong());
        metrics.sentMessagesCount(buf.getInt());
        metrics.sentBytesCount(buf.getLong());
        metrics.receivedMessagesCount(buf.getInt());
        metrics.receivedBytesCount(buf.getLong());
        metrics.outboundMessagesQueueSize(buf.getInt());
        metrics.totalNodes(buf.getInt());

        // For compatibility with metrics serialized by old ignite versions.
        if (buf.remaining() >= 8)
            metrics.totalJobsExecutionTime(buf.getLong());
        else
            metrics.totalJobsExecutionTime(0);

        if (buf.remaining() >= 8)
            metrics.currentPmeDuration(buf.getLong());
        else
            metrics.currentPmeDuration(0);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsSnapshot.class, this);
    }
}

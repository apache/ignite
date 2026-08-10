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
 * Implementation for {@link ClusterMetrics} interface which is also {@link Message}.
 * <p>
 * Note that whenever adding or removing metric parameters, care
 * must be taken to update serialize/deserialize logic as well.
 */
public class ClusterMetricsSnapshot implements ClusterMetrics, Message {
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
    @Order(0)
    public int maxActiveJobs = -1;

    /** */
    @Order(1)
    public int curActiveJobs = -1;

    /** */
    @Order(2)
    public float avgActiveJobs = -1;

    /** */
    @Order(3)
    public int maxWaitingJobs = -1;

    /** */
    @Order(4)
    public int curWaitingJobs = -1;

    /** */
    @Order(5)
    public float avgWaitingJobs = -1;

    /** */
    @Order(6)
    public int maxRejectedJobs = -1;

    /** */
    @Order(7)
    public int curRejectedJobs = -1;

    /** */
    @Order(8)
    public float avgRejectedJobs = -1;

    /** */
    @Order(9)
    public int maxCancelledJobs = -1;

    /** */
    @Order(10)
    public int curCancelledJobs = -1;

    /** */
    @Order(11)
    public float avgCancelledJobs = -1;

    /** */
    @Order(12)
    public int totalRejectedJobs = -1;

    /** */
    @Order(13)
    public int totalCancelledJobs = -1;

    /** */
    @Order(14)
    public int totalExecutedJobs = -1;

    /** */
    @Order(15)
    public long maxJobWaitTime = -1;

    /** */
    @Order(16)
    public long curJobWaitTime = Long.MAX_VALUE;

    /** */
    @Order(17)
    public double avgJobWaitTime = -1;

    /** */
    @Order(18)
    public long maxJobExecTime = -1;

    /** */
    @Order(19)
    public long curJobExecTime = -1;

    /** */
    @Order(20)
    public double avgJobExecTime = -1;

    /** */
    @Order(21)
    public int totalExecTasks = -1;

    /** */
    @Order(22)
    public long totalIdleTime = -1;

    /** */
    @Order(23)
    public long curIdleTime = -1;

    /** */
    @Order(24)
    public int totalCpus = -1;

    /** */
    @Order(25)
    public double curCpuLoad = -1;

    /** */
    @Order(26)
    public double avgCpuLoad = -1;

    /** */
    @Order(27)
    public double curGcCpuLoad = -1;

    /** */
    @Order(28)
    public long heapInit = -1;

    /** */
    @Order(29)
    public long heapUsed = -1;

    /** */
    @Order(30)
    public long heapCommitted = -1;

    /** */
    @Order(31)
    public long heapMax = -1;

    /** */
    @Order(32)
    public long heapTotal = -1;

    /** */
    @Order(33)
    public long nonHeapInit = -1;

    /** */
    @Order(34)
    public long nonHeapUsed = -1;

    /** */
    @Order(35)
    public long nonHeapCommitted = -1;

    /** */
    @Order(36)
    public long nonHeapMax = -1;

    /** */
    @Order(37)
    public long nonHeapTotal = -1;

    /** */
    @Order(38)
    public long upTime = -1;

    /** */
    @Order(39)
    public long startTime = -1;

    /** */
    @Order(40)
    public long nodeStartTime = -1;

    /** */
    @Order(41)
    public int threadCnt = -1;

    /** */
    @Order(42)
    public int peakThreadCnt = -1;

    /** */
    @Order(43)
    public long startedThreadCnt = -1;

    /** */
    @Order(44)
    public int daemonThreadCnt = -1;

    /** */
    @Order(45)
    public long lastDataVer = -1;

    /** */
    @Order(46)
    public int sentMsgsCnt = -1;

    /** */
    @Order(47)
    public long sentBytesCnt = -1;

    /** */
    @Order(48)
    public int rcvdMsgsCnt = -1;

    /** */
    @Order(49)
    public long rcvdBytesCnt = -1;

    /** */
    @Order(50)
    public int outMesQueueSize = -1;

    /** */
    @Order(51)
    public int totalNodes = -1;

    /** */
    @Order(52)
    public long totalJobsExecTime = -1;

    /** */
    @Order(53)
    public long curPmeDuration = -1;

    /** */
    public long lastUpdateTime = -1;

    /** Empty constructor for serialization purposes. */
    public ClusterMetricsSnapshot() {
        // Like in deserialize().
        lastUpdateTime = System.currentTimeMillis();
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
        totalCpus = 0;
        curCpuLoad = 0;
        avgCpuLoad = 0;
        curGcCpuLoad = 0;
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
        curPmeDuration = 0;

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

            avgCpuLoad += m.getCurrentCpuLoad();

            curPmeDuration = max(curPmeDuration, m.getCurrentPmeDuration());
        }

        curJobExecTime /= size;

        avgActiveJobs /= size;
        avgCancelledJobs /= size;
        avgRejectedJobs /= size;
        avgWaitingJobs /= size;
        avgJobExecTime /= size;
        avgJobWaitTime /= size;
        avgCpuLoad /= size;

        if (!F.isEmpty(nodes)) {
            ClusterMetrics oldestNodeMetrics = oldest(nodes).metrics();

            nodeStartTime = oldestNodeMetrics.getNodeStartTime();
            startTime = oldestNodeMetrics.getStartTime();
        }

        Map<String, Collection<ClusterNode>> neighborhood = U.neighborhood(nodes);

        curGcCpuLoad = currentGcCpuLoad(neighborhood);
        curCpuLoad = currentCpuLoad(neighborhood);
        totalCpus = cpuCnt(neighborhood);
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

        totalCpus = metrics.getTotalCpus();
        curCpuLoad = metrics.getCurrentCpuLoad();
        avgCpuLoad = metrics.getAverageCpuLoad();
        curGcCpuLoad = metrics.getCurrentGcCpuLoad();

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

        curPmeDuration = metrics.getCurrentPmeDuration();

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

        lastUpdateTime = metrics.getLastUpdateTime();
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
        return curActiveJobs;
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
    @Override public int getTotalCpus() {
        return totalCpus;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return curCpuLoad;
    }

    /** {@inheritDoc} */
    @Override public double getAverageCpuLoad() {
        return avgCpuLoad;
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return curGcCpuLoad;
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
        return curPmeDuration;
    }

    /** {@inheritDoc} */
    @Override public long getTotalBusyTime() {
        return getUpTime() - getTotalIdleTime();
    }

    /** {@inheritDoc} */
    @Override public float getBusyTimePercentage() {
        return 1 - getIdleTimePercentage();
    }

    /** {@inheritDoc} */
    @Override public float getIdleTimePercentage() {
        return getTotalIdleTime() / (float)getUpTime();
    }

    /**
     * Sets available processors.
     *
     * @param totalCpus Available processors.
     */
    public void totalCpus(int totalCpus) {
        this.totalCpus = totalCpus;
    }

    /**
     * Sets current CPU load.
     *
     * @param curCpuLoad Current CPU load.
     */
    public void currentCpuLoad(double curCpuLoad) {
        this.curCpuLoad = curCpuLoad;
    }

    /**
     * Sets CPU load average over the metrics history.
     *
     * @param avgCpuLoad CPU load average.
     */
    public void averageCpuLoad(double avgCpuLoad) {
        this.avgCpuLoad = avgCpuLoad;
    }

    /**
     * Sets current GC load.
     *
     * @param curGcCpuLoad Current GC load.
     */
    public void currentGcCpuLoad(double curGcCpuLoad) {
        this.curGcCpuLoad = curGcCpuLoad;
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
     * @param curPmeDuration Execution duration for current partition map exchange.
     */
    public void currentPmeDuration(long curPmeDuration) {
        this.curPmeDuration = curPmeDuration;
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
    private static double currentCpuLoad(Map<String, Collection<ClusterNode>> neighborhood) {
        double curCpuLoad = 0.0;

        for (Collection<ClusterNode> nodes : neighborhood.values()) {
            ClusterNode first = F.first(nodes);

            // Projection can be empty if all nodes in it failed.
            if (first != null)
                curCpuLoad += first.metrics().getCurrentCpuLoad();
        }

        return curCpuLoad;
    }

    /**
     * @param neighborhood Cluster neighborhood.
     * @return GC CPU load.
     */
    private static double currentGcCpuLoad(Map<String, Collection<ClusterNode>> neighborhood) {
        double curGcCpuLoad = 0;

        for (Collection<ClusterNode> nodes : neighborhood.values()) {
            ClusterNode first = F.first(nodes);

            // Projection can be empty if all nodes in it failed.
            if (first != null)
                curGcCpuLoad += first.metrics().getCurrentGcCpuLoad();
        }

        return curGcCpuLoad;
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
    public static ClusterMetricsSnapshot deserialize(byte[] data, int off) {
        ClusterMetricsSnapshot msg = new ClusterMetricsSnapshot();

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

        return msg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsSnapshot.class, this);
    }
}

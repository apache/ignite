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

package org.apache.ignite.internal.processors.cluster;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.max;
import static java.lang.Math.min;

/** */
public class NodeMetricsMessage implements Message {
    /** */
    public static final short TYPE_CODE = 137;

    /** */
    @Order(value = 0)
    private long lastUpdateTime = -1;

    /** */
    @Order(value = 1, method = "maximumActiveJobs")
    private int maxActiveJobs = -1;

    /** */
    @Order(value = 2, method = "currentActiveJobs")
    private int curActiveJobs = -1;

    /** */
    @Order(value = 3, method = "averageActiveJobs")
    private float avgActiveJobs = -1;

    /** */
    @Order(value = 4, method = "maximumWaitingJobs")
    private int maxWaitingJobs = -1;

    /** */
    @Order(value = 5, method = "currentWaitingJobs")
    private int curWaitingJobs = -1;

    /** */
    @Order(value = 6, method = "averageWaitingJobs")
    private float avgWaitingJobs = -1;

    /** */
    @Order(value = 7, method = "maximumRejectedJobs")
    private int maxRejectedJobs = -1;

    /** */
    @Order(value = 8, method = "currentRejectedJobs")
    private int curRejectedJobs = -1;

    /** */
    @Order(value = 9, method = "averageRejectedJobs")
    private float avgRejectedJobs = -1;

    /** */
    @Order(value = 10, method = "maximumCancelledJobs")
    private int maxCancelledJobs = -1;

    /** */
    @Order(value = 11, method = "currentCancelledJobs")
    private int curCancelledJobs = -1;

    /** */
    @Order(value = 12, method = "averageCancelledJobs")
    private float avgCancelledJobs = -1;

    /** */
    @Order(value = 13)
    private int totalRejectedJobs = -1;

    /** */
    @Order(value = 14)
    private int totalCancelledJobs = -1;

    /** */
    @Order(value = 15)
    private int totalExecutedJobs = -1;

    /** */
    @Order(value = 16, method = "maximumJobWaitTime")
    private long maxJobWaitTime = -1;

    /** */
    @Order(value = 17, method = "currentJobWaitTime")
    private long curJobWaitTime = Long.MAX_VALUE;

    /** */
    @Order(value = 18, method = "averageJobWaitTime")
    private double avgJobWaitTime = -1;

    /** */
    @Order(value = 19, method = "maximumJobExecuteTime")
    private long maxJobExecTime = -1;

    /** */
    @Order(value = 20, method = "currentJobExecuteTime")
    private long curJobExecTime = -1;

    /** */
    @Order(value = 21, method = "averageJobExecuteTime")
    private double avgJobExecTime = -1;

    /** */
    @Order(value = 22, method = "totalExecutedTasks")
    private int totalExecTasks = -1;

    /** */
    @Order(value = 23)
    private long totalIdleTime = -1;

    /** */
    @Order(value = 24, method = "currentIdleTime")
    private long curIdleTime = -1;

    /** */
    @Order(value = 25)
    private int totalCpus = -1;

    /** */
    @Order(value = 26, method = "currentCpuLoad")
    private double curCpuLoad = -1;

    /** */
    @Order(value = 27, method = "averageCpuLoad")
    private double avgCpuLoad = -1;

    /** */
    @Order(value = 28, method = "currentGcCpuLoad")
    private double curGcCpuLoad = -1;

    /** */
    @Order(value = 29, method = "heapMemoryInitialized")
    private long heapInit = -1;

    /** */
    @Order(value = 30, method = "heapMemoryUsed")
    private long heapUsed = -1;

    /** */
    @Order(value = 31, method = "heapMemoryCommitted")
    private long heapCommitted = -1;

    /** */
    @Order(value = 32, method = "heapMemoryMaximum")
    private long heapMax = -1;

    /** */
    @Order(value = 33, method = "heapMemoryTotal")
    private long heapTotal = -1;

    /** */
    @Order(value = 34, method = "nonHeapMemoryInitialized")
    private long nonHeapInit = -1;

    /** */
    @Order(value = 35, method = "nonHeapMemoryUsed")
    private long nonHeapUsed = -1;

    /** */
    @Order(value = 36, method = "nonHeapMemoryCommitted")
    private long nonHeapCommitted = -1;

    /** */
    @Order(value = 37, method = "nonHeapMemoryMaximum")
    private long nonHeapMax = -1;

    /** */
    @Order(value = 38, method = "nonHeapMemoryTotal")
    private long nonHeapTotal = -1;

    /** */
    @Order(value = 39)
    private long upTime = -1;

    /** */
    @Order(value = 40)
    private long startTime = -1;

    /** */
    @Order(value = 41)
    private long nodeStartTime = -1;

    /** */
    @Order(value = 42, method = "currentThreadCount")
    private int threadCnt = -1;

    /** */
    @Order(value = 43, method = "maximumThreadCount")
    private int peakThreadCnt = -1;

    /** */
    @Order(value = 44, method = "totalStartedThreadCount")
    private long startedThreadCnt = -1;

    /** */
    @Order(value = 45, method = "currentDaemonThreadCount")
    private int daemonThreadCnt = -1;

    /** */
    @Order(value = 46, method = "lastDataVersion")
    private long lastDataVer = -1;

    /** */
    @Order(value = 47, method = "sentMessagesCount")
    private int sentMsgsCnt = -1;

    /** */
    @Order(value = 48, method = "sentBytesCount")
    private long sentBytesCnt = -1;

    /** */
    @Order(value = 49, method = "receivedMessagesCount")
    private int rcvdMsgsCnt = -1;

    /** */
    @Order(value = 50, method = "receivedBytesCount")
    private long rcvdBytesCnt = -1;

    /** */
    @Order(value = 51, method = "outboundMessagesQueueSize")
    private int outMesQueueSize = -1;

    /** */
    @Order(value = 52)
    private int totalNodes = -1;

    /** */
    @Order(value = 53, method = "totalJobsExecutionTime")
    private long totalJobsExecTime = -1;

    /** */
    @Order(value = 54, method = "currentPmeDuration")
    private long curPmeDuration = -1;

    /** */
    public NodeMetricsMessage() {
        // No-op.
    }

    /**
     * Create metrics for given nodes.
     *
     * @param nodes Nodes.
     */
    public NodeMetricsMessage(Collection<ClusterNode> nodes) {
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
    public NodeMetricsMessage(ClusterMetrics metrics) {
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
    }

    /** */
    public long heapMemoryTotal() {
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

    /** */
    public long lastUpdateTime() {
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

    /** */
    public int maximumActiveJobs() {
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

    /** */
    public int currentActiveJobs() {
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

    /** */
    public float averageActiveJobs() {
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

    /** */
    public int maximumWaitingJobs() {
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

    /** */
    public int currentWaitingJobs() {
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

    /** */
    public float averageWaitingJobs() {
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

    /** */
    public int maximumRejectedJobs() {
        return maxRejectedJobs;
    }

    /**
     * @param maxRejectedJobs Maximum number of jobs rejected during a single collision resolution event.
     */
    public void maximumRejectedJobs(int maxRejectedJobs) {
        this.maxRejectedJobs = maxRejectedJobs;
    }

    /** */
    public int currentRejectedJobs() {
        return curRejectedJobs;
    }

    /**
     * @param curRejectedJobs Number of jobs rejected during most recent collision resolution.
     */
    public void currentRejectedJobs(int curRejectedJobs) {
        this.curRejectedJobs = curRejectedJobs;
    }

    /** */
    public float averageRejectedJobs() {
        return avgRejectedJobs;
    }

    /**
     * @param avgRejectedJobs Average number of jobs this node rejects.
     */
    public void averageRejectedJobs(float avgRejectedJobs) {
        this.avgRejectedJobs = avgRejectedJobs;
    }

    /** */
    public int totalRejectedJobs() {
        return totalRejectedJobs;
    }

    /**
     * @param totalRejectedJobs Total number of jobs this node ever rejected.
     */
    public void totalRejectedJobs(int totalRejectedJobs) {
        this.totalRejectedJobs = totalRejectedJobs;
    }

    /** */
    public int maximumCancelledJobs() {
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

    /** */
    public int currentCancelledJobs() {
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

    /** */
    public float averageCancelledJobs() {
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

    /** */
    public int totalExecutedJobs() {
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

    /** */
    public long totalJobsExecutionTime() {
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

    /** */
    public int totalCancelledJobs() {
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

    /** */
    public long maximumJobWaitTime() {
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

    /** */
    public long currentJobWaitTime() {
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

    /** */
    public double averageJobWaitTime() {
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

    /** */
    public long maximumJobExecuteTime() {
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

    /** */
    public long currentJobExecuteTime() {
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

    /** */
    public double averageJobExecuteTime() {
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

    /** */
    public int totalExecutedTasks() {
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

    /** */
    public long totalIdleTime() {
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

    /** */
    public long currentIdleTime() {
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

    /** */
    public int totalCpus() {
        return totalCpus;
    }

    /** */
    public double currentCpuLoad() {
        return curCpuLoad;
    }

    /** */
    public double averageCpuLoad() {
        return avgCpuLoad;
    }

    /** */
    public double currentGcCpuLoad() {
        return curGcCpuLoad;
    }

    /** */
    public long heapMemoryInitialized() {
        return heapInit;
    }

    /** */
    public long heapMemoryUsed() {
        return heapUsed;
    }

    /** */
    public long heapMemoryCommitted() {
        return heapCommitted;
    }

    /** */
    public long heapMemoryMaximum() {
        return heapMax;
    }

    /** */
    public long nonHeapMemoryInitialized() {
        return nonHeapInit;
    }

    /** */
    public long nonHeapMemoryUsed() {
        return nonHeapUsed;
    }

    /** */
    public long nonHeapMemoryCommitted() {
        return nonHeapCommitted;
    }

    /** */
    public long nonHeapMemoryMaximum() {
        return nonHeapMax;
    }

    /** */
    public long nonHeapMemoryTotal() {
        return nonHeapTotal;
    }

    /** */
    public long upTime() {
        return upTime;
    }

    /** */
    public long startTime() {
        return startTime;
    }

    /** */
    public long nodeStartTime() {
        return nodeStartTime;
    }

    /** */
    public int currentThreadCount() {
        return threadCnt;
    }

    /** */
    public int maximumThreadCount() {
        return peakThreadCnt;
    }

    /** */
    public long totalStartedThreadCount() {
        return startedThreadCnt;
    }

    /** */
    public int currentDaemonThreadCount() {
        return daemonThreadCnt;
    }

    /** */
    public long lastDataVersion() {
        return lastDataVer;
    }

    /** */
    public int sentMessagesCount() {
        return sentMsgsCnt;
    }

    /** */
    public long sentBytesCount() {
        return sentBytesCnt;
    }

    /** */
    public int receivedMessagesCount() {
        return rcvdMsgsCnt;
    }

    /** */
    public long receivedBytesCount() {
        return rcvdBytesCnt;
    }

    /** */
    public int outboundMessagesQueueSize() {
        return outMesQueueSize;
    }

    /** */
    public int totalNodes() {
        return totalNodes;
    }

    /** */
    public long currentPmeDuration() {
        return curPmeDuration;
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

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** */
    public String toString() {
        return S.toString(NodeMetricsMessage.class, this);
    }
}

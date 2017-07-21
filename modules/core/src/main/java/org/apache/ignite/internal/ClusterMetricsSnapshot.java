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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.max;
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
        4/*total nodes*/;

    /** */
    private long lastUpdateTime = -1;

    /** */
    private int maxActiveJobs = -1;

    /** */
    private int curActiveJobs = -1;

    /** */
    private float avgActiveJobs = -1;

    /** */
    private int maxWaitingJobs = -1;

    /** */
    private int curWaitingJobs = -1;

    /** */
    private float avgWaitingJobs = -1;

    /** */
    private int maxRejectedJobs = -1;

    /** */
    private int curRejectedJobs = -1;

    /** */
    private float avgRejectedJobs = -1;

    /** */
    private int maxCancelledJobs = -1;

    /** */
    private int curCancelledJobs = -1;

    /** */
    private float avgCancelledJobs = -1;

    /** */
    private int totalRejectedJobs = -1;

    /** */
    private int totalCancelledJobs = -1;

    /** */
    private int totalExecutedJobs = -1;

    /** */
    private long maxJobWaitTime = -1;

    /** */
    private long curJobWaitTime = -1;

    /** */
    private double avgJobWaitTime = -1;

    /** */
    private long maxJobExecTime = -1;

    /** */
    private long curJobExecTime = -1;

    /** */
    private double avgJobExecTime = -1;

    /** */
    private int totalExecTasks = -1;

    /** */
    private long totalIdleTime = -1;

    /** */
    private long curIdleTime = -1;

    /** */
    private int availProcs = -1;

    /** */
    private double load = -1;

    /** */
    private double avgLoad = -1;

    /** */
    private double gcLoad = -1;

    /** */
    private long heapInit = -1;

    /** */
    private long heapUsed = -1;

    /** */
    private long heapCommitted = -1;

    /** */
    private long heapMax = -1;

    /** */
    private long heapTotal = -1;

    /** */
    private long nonHeapInit = -1;

    /** */
    private long nonHeapUsed = -1;

    /** */
    private long nonHeapCommitted = -1;

    /** */
    private long nonHeapMax = -1;

    /** */
    private long nonHeapTotal = -1;

    /** */
    private long upTime = -1;

    /** */
    private long startTime = -1;

    /** */
    private long nodeStartTime = -1;

    /** */
    private int threadCnt = -1;

    /** */
    private int peakThreadCnt = -1;

    /** */
    private long startedThreadCnt = -1;

    /** */
    private int daemonThreadCnt = -1;

    /** */
    private long lastDataVer = -1;

    /** */
    private int sentMsgsCnt = -1;

    /** */
    private long sentBytesCnt = -1;

    /** */
    private int rcvdMsgsCnt = -1;

    /** */
    private long rcvdBytesCnt = -1;

    /** */
    private int outMesQueueSize = -1;

    /** */
    private int totalNodes = -1;

    /**
     * Create empty snapshot.
     */
    public ClusterMetricsSnapshot() {
        // No-op.
    }

    /**
     * Create metrics for given cluster group.
     *
     * @param p Projection to get metrics for.
     */
    public ClusterMetricsSnapshot(ClusterGroup p) {
        assert p != null;

        Collection<ClusterNode> nodes = p.nodes();

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

        for (ClusterNode node : nodes) {
            ClusterMetrics m = node.metrics();

            lastUpdateTime = max(lastUpdateTime, node.metrics().getLastUpdateTime());

            curActiveJobs += m.getCurrentActiveJobs();
            maxActiveJobs = max(maxActiveJobs, m.getCurrentActiveJobs());
            avgActiveJobs += m.getCurrentActiveJobs();
            totalExecutedJobs += m.getTotalExecutedJobs();

            totalExecTasks += m.getTotalExecutedTasks();

            totalCancelledJobs += m.getTotalCancelledJobs();
            curCancelledJobs += m.getCurrentCancelledJobs();
            maxCancelledJobs = max(maxCancelledJobs, m.getCurrentCancelledJobs());
            avgCancelledJobs += m.getCurrentCancelledJobs();

            totalRejectedJobs += m.getTotalRejectedJobs();
            curRejectedJobs += m.getCurrentRejectedJobs();
            maxRejectedJobs = max(maxRejectedJobs, m.getCurrentRejectedJobs());
            avgRejectedJobs += m.getCurrentRejectedJobs();

            curWaitingJobs += m.getCurrentJobWaitTime();
            maxWaitingJobs = max(maxWaitingJobs, m.getCurrentWaitingJobs());
            avgWaitingJobs += m.getCurrentWaitingJobs();

            maxJobExecTime = max(maxJobExecTime, m.getMaximumJobExecuteTime());
            avgJobExecTime += m.getAverageJobExecuteTime();
            curJobExecTime += m.getCurrentJobExecuteTime();

            curJobWaitTime = min(curJobWaitTime, m.getCurrentJobWaitTime());
            maxJobWaitTime = max(maxJobWaitTime, m.getCurrentJobWaitTime());
            avgJobWaitTime += m.getCurrentJobWaitTime();

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

    /** {@inheritDoc} */
    @Override public long getHeapMemoryTotal() {
        return heapTotal;
    }

    /**
     * Sets total heap size.
     *
     * @param heapTotal Total heap.
     */
    public void setHeapMemoryTotal(long heapTotal) {
        this.heapTotal = heapTotal;
    }

    /**
     * Sets non-heap total heap size.
     *
     * @param nonHeapTotal Total heap.
     */
    public void setNonHeapMemoryTotal(long nonHeapTotal) {
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
    public void setLastUpdateTime(long lastUpdateTime) {
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
    public void setMaximumActiveJobs(int maxActiveJobs) {
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
    public void setCurrentActiveJobs(int curActiveJobs) {
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
    public void setAverageActiveJobs(float avgActiveJobs) {
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
    public void setMaximumWaitingJobs(int maxWaitingJobs) {
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
    public void setCurrentWaitingJobs(int curWaitingJobs) {
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
    public void setAverageWaitingJobs(float avgWaitingJobs) {
        this.avgWaitingJobs = avgWaitingJobs;
    }

    /** {@inheritDoc} */
    @Override public int getMaximumRejectedJobs() {
        return maxRejectedJobs;
    }

    /**
     * @param maxRejectedJobs Maximum number of jobs rejected during a single collision resolution event.
     */
    public void setMaximumRejectedJobs(int maxRejectedJobs) {
        this.maxRejectedJobs = maxRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRejectedJobs() {
        return curRejectedJobs;
    }

    /**
     * @param curRejectedJobs Number of jobs rejected during most recent collision resolution.
     */
    public void setCurrentRejectedJobs(int curRejectedJobs) {
        this.curRejectedJobs = curRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRejectedJobs() {
        return avgRejectedJobs;
    }

    /**
     * @param avgRejectedJobs Average number of jobs this node rejects.
     */
    public void setAverageRejectedJobs(float avgRejectedJobs) {
        this.avgRejectedJobs = avgRejectedJobs;
    }

    /** {@inheritDoc} */
    @Override public int getTotalRejectedJobs() {
        return totalRejectedJobs;
    }

    /**
     * @param totalRejectedJobs Total number of jobs this node ever rejected.
     */
    public void setTotalRejectedJobs(int totalRejectedJobs) {
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
    public void setMaximumCancelledJobs(int maxCancelledJobs) {
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
    public void setCurrentCancelledJobs(int curCancelledJobs) {
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
    public void setAverageCancelledJobs(float avgCancelledJobs) {
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
    public void setTotalExecutedJobs(int totalExecutedJobs) {
        this.totalExecutedJobs = totalExecutedJobs;
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
    public void setTotalCancelledJobs(int totalCancelledJobs) {
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
    public void setMaximumJobWaitTime(long maxJobWaitTime) {
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
    public void setCurrentJobWaitTime(long curJobWaitTime) {
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
    public void setAverageJobWaitTime(double avgJobWaitTime) {
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
    public void setMaximumJobExecuteTime(long maxJobExecTime) {
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
    public void setCurrentJobExecuteTime(long curJobExecTime) {
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
    public void setAverageJobExecuteTime(double avgJobExecTime) {
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
    public void setTotalExecutedTasks(int totalExecTasks) {
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
    public void setTotalIdleTime(long totalIdleTime) {
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
    public void setCurrentIdleTime(long curIdleTime) {
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

    /**
     * Sets available processors.
     *
     * @param availProcs Available processors.
     */
    public void setAvailableProcessors(int availProcs) {
        this.availProcs = availProcs;
    }

    /**
     * Sets current CPU load.
     *
     * @param load Current CPU load.
     */
    public void setCurrentCpuLoad(double load) {
        this.load = load;
    }

    /**
     * Sets CPU load average over the metrics history.
     *
     * @param avgLoad CPU load average.
     */
    public void setAverageCpuLoad(double avgLoad) {
        this.avgLoad = avgLoad;
    }

    /**
     * Sets current GC load.
     *
     * @param gcLoad Current GC load.
     */
    public void setCurrentGcCpuLoad(double gcLoad) {
        this.gcLoad = gcLoad;
    }

    /**
     * Sets heap initial memory.
     *
     * @param heapInit Heap initial memory.
     */
    public void setHeapMemoryInitialized(long heapInit) {
        this.heapInit = heapInit;
    }

    /**
     * Sets used heap memory.
     *
     * @param heapUsed Used heap memory.
     */
    public void setHeapMemoryUsed(long heapUsed) {
        this.heapUsed = heapUsed;
    }

    /**
     * Sets committed heap memory.
     *
     * @param heapCommitted Committed heap memory.
     */
    public void setHeapMemoryCommitted(long heapCommitted) {
        this.heapCommitted = heapCommitted;
    }

    /**
     * Sets maximum possible heap memory.
     *
     * @param heapMax Maximum possible heap memory.
     */
    public void setHeapMemoryMaximum(long heapMax) {
        this.heapMax = heapMax;
    }

    /**
     * Sets initial non-heap memory.
     *
     * @param nonHeapInit Initial non-heap memory.
     */
    public void setNonHeapMemoryInitialized(long nonHeapInit) {
        this.nonHeapInit = nonHeapInit;
    }

    /**
     * Sets used non-heap memory.
     *
     * @param nonHeapUsed Used non-heap memory.
     */
    public void setNonHeapMemoryUsed(long nonHeapUsed) {
        this.nonHeapUsed = nonHeapUsed;
    }

    /**
     * Sets committed non-heap memory.
     *
     * @param nonHeapCommitted Committed non-heap memory.
     */
    public void setNonHeapMemoryCommitted(long nonHeapCommitted) {
        this.nonHeapCommitted = nonHeapCommitted;
    }

    /**
     * Sets maximum possible non-heap memory.
     *
     * @param nonHeapMax Maximum possible non-heap memory.
     */
    public void setNonHeapMemoryMaximum(long nonHeapMax) {
        this.nonHeapMax = nonHeapMax;
    }

    /**
     * Sets VM up time.
     *
     * @param upTime VM up time.
     */
    public void setUpTime(long upTime) {
        this.upTime = upTime;
    }

    /**
     * Sets VM start time.
     *
     * @param startTime VM start time.
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Sets node start time.
     *
     * @param nodeStartTime node start time.
     */
    public void setNodeStartTime(long nodeStartTime) {
        this.nodeStartTime = nodeStartTime;
    }

    /**
     * Sets thread count.
     *
     * @param threadCnt Thread count.
     */
    public void setCurrentThreadCount(int threadCnt) {
        this.threadCnt = threadCnt;
    }

    /**
     * Sets peak thread count.
     *
     * @param peakThreadCnt Peak thread count.
     */
    public void setMaximumThreadCount(int peakThreadCnt) {
        this.peakThreadCnt = peakThreadCnt;
    }

    /**
     * Sets started thread count.
     *
     * @param startedThreadCnt Started thread count.
     */
    public void setTotalStartedThreadCount(long startedThreadCnt) {
        this.startedThreadCnt = startedThreadCnt;
    }

    /**
     * Sets daemon thread count.
     *
     * @param daemonThreadCnt Daemon thread count.
     */
    public void setCurrentDaemonThreadCount(int daemonThreadCnt) {
        this.daemonThreadCnt = daemonThreadCnt;
    }

    /**
     * Sets last data version.
     *
     * @param lastDataVer Last data version.
     */
    public void setLastDataVersion(long lastDataVer) {
        this.lastDataVer = lastDataVer;
    }

    /**
     * Sets sent messages count.
     *
     * @param sentMsgsCnt Sent messages count.
     */
    public void setSentMessagesCount(int sentMsgsCnt) {
        this.sentMsgsCnt = sentMsgsCnt;
    }

    /**
     * Sets sent bytes count.
     *
     * @param sentBytesCnt Sent bytes count.
     */
    public void setSentBytesCount(long sentBytesCnt) {
        this.sentBytesCnt = sentBytesCnt;
    }

    /**
     * Sets received messages count.
     *
     * @param rcvdMsgsCnt Received messages count.
     */
    public void setReceivedMessagesCount(int rcvdMsgsCnt) {
        this.rcvdMsgsCnt = rcvdMsgsCnt;
    }

    /**
     * Sets received bytes count.
     *
     * @param rcvdBytesCnt Received bytes count.
     */
    public void setReceivedBytesCount(long rcvdBytesCnt) {
        this.rcvdBytesCnt = rcvdBytesCnt;
    }

    /**
     * Sets outbound messages queue size.
     *
     * @param outMesQueueSize Outbound messages queue size.
     */
    public void setOutboundMessagesQueueSize(int outMesQueueSize) {
        this.outMesQueueSize = outMesQueueSize;
    }

    /**
     * Sets total number of nodes.
     *
     * @param totalNodes Total number of nodes.
     */
    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
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

        ByteBuffer buf = ByteBuffer.wrap(data, off, METRICS_SIZE);

        metrics.setLastUpdateTime(U.currentTimeMillis());

        metrics.setMaximumActiveJobs(buf.getInt());
        metrics.setCurrentActiveJobs(buf.getInt());
        metrics.setAverageActiveJobs(buf.getFloat());
        metrics.setMaximumWaitingJobs(buf.getInt());
        metrics.setCurrentWaitingJobs(buf.getInt());
        metrics.setAverageWaitingJobs(buf.getFloat());
        metrics.setMaximumRejectedJobs(buf.getInt());
        metrics.setCurrentRejectedJobs(buf.getInt());
        metrics.setAverageRejectedJobs(buf.getFloat());
        metrics.setMaximumCancelledJobs(buf.getInt());
        metrics.setCurrentCancelledJobs(buf.getInt());
        metrics.setAverageCancelledJobs(buf.getFloat());
        metrics.setTotalRejectedJobs(buf.getInt());
        metrics.setTotalCancelledJobs(buf.getInt());
        metrics.setTotalExecutedJobs(buf.getInt());
        metrics.setMaximumJobWaitTime(buf.getLong());
        metrics.setCurrentJobWaitTime(buf.getLong());
        metrics.setAverageJobWaitTime(buf.getDouble());
        metrics.setMaximumJobExecuteTime(buf.getLong());
        metrics.setCurrentJobExecuteTime(buf.getLong());
        metrics.setAverageJobExecuteTime(buf.getDouble());
        metrics.setTotalExecutedTasks(buf.getInt());
        metrics.setCurrentIdleTime(buf.getLong());
        metrics.setTotalIdleTime(buf.getLong());
        metrics.setAvailableProcessors(buf.getInt());
        metrics.setCurrentCpuLoad(buf.getDouble());
        metrics.setAverageCpuLoad(buf.getDouble());
        metrics.setCurrentGcCpuLoad(buf.getDouble());
        metrics.setHeapMemoryInitialized(buf.getLong());
        metrics.setHeapMemoryUsed(buf.getLong());
        metrics.setHeapMemoryCommitted(buf.getLong());
        metrics.setHeapMemoryMaximum(buf.getLong());
        metrics.setHeapMemoryTotal(buf.getLong());
        metrics.setNonHeapMemoryInitialized(buf.getLong());
        metrics.setNonHeapMemoryUsed(buf.getLong());
        metrics.setNonHeapMemoryCommitted(buf.getLong());
        metrics.setNonHeapMemoryMaximum(buf.getLong());
        metrics.setNonHeapMemoryTotal(buf.getLong());
        metrics.setStartTime(buf.getLong());
        metrics.setNodeStartTime(buf.getLong());
        metrics.setUpTime(buf.getLong());
        metrics.setCurrentThreadCount(buf.getInt());
        metrics.setMaximumThreadCount(buf.getInt());
        metrics.setTotalStartedThreadCount(buf.getLong());
        metrics.setCurrentDaemonThreadCount(buf.getInt());
        metrics.setLastDataVersion(buf.getLong());
        metrics.setSentMessagesCount(buf.getInt());
        metrics.setSentBytesCount(buf.getLong());
        metrics.setReceivedMessagesCount(buf.getInt());
        metrics.setReceivedBytesCount(buf.getLong());
        metrics.setOutboundMessagesQueueSize(buf.getInt());
        metrics.setTotalNodes(buf.getInt());

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsSnapshot.class, this);
    }
}
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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Adapter for {@link GridLocalMetrics} interface.
 * <p>
 * Note that whenever adding or removing metric parameters, care
 * must be taken to update {@link DiscoveryMetricsHelper} as well.
 */
public class DiscoveryNodeMetricsAdapter implements ClusterNodeMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

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
    private long totalPhysicalMemory = -1;

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
    private long nonHeapInit = -1;

    /** */
    private long nonHeapUsed = -1;

    /** */
    private long nonHeapCommitted = -1;

    /** */
    private long nonHeapMax = -1;

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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(maxActiveJobs);
        out.writeInt(curActiveJobs);
        out.writeFloat(avgActiveJobs);
        out.writeInt(maxWaitingJobs);
        out.writeInt(curWaitingJobs);
        out.writeFloat(avgWaitingJobs);
        out.writeInt(maxCancelledJobs);
        out.writeInt(curCancelledJobs);
        out.writeFloat(avgCancelledJobs);
        out.writeInt(maxRejectedJobs);
        out.writeInt(curRejectedJobs);
        out.writeFloat(avgRejectedJobs);
        out.writeInt(totalExecutedJobs);
        out.writeInt(totalCancelledJobs);
        out.writeInt(totalRejectedJobs);
        out.writeLong(maxJobWaitTime);
        out.writeLong(curJobWaitTime);
        out.writeDouble(avgJobWaitTime);
        out.writeLong(maxJobExecTime);
        out.writeLong(curJobExecTime);
        out.writeDouble(avgJobExecTime);
        out.writeInt(totalExecTasks);
        out.writeLong(curIdleTime);
        out.writeLong(totalIdleTime);
        out.writeInt(availProcs);
        out.writeLong(totalPhysicalMemory);
        out.writeDouble(load);
        out.writeDouble(avgLoad);
        out.writeDouble(gcLoad);
        out.writeLong(heapInit);
        out.writeLong(heapUsed);
        out.writeLong(heapCommitted);
        out.writeLong(heapMax);
        out.writeLong(nonHeapInit);
        out.writeLong(nonHeapUsed);
        out.writeLong(nonHeapCommitted);
        out.writeLong(nonHeapMax);
        out.writeLong(upTime);
        out.writeLong(startTime);
        out.writeLong(nodeStartTime);
        out.writeInt(threadCnt);
        out.writeInt(peakThreadCnt);
        out.writeLong(startedThreadCnt);
        out.writeInt(daemonThreadCnt);
        out.writeLong(lastDataVer);
        out.writeInt(sentMsgsCnt);
        out.writeLong(sentBytesCnt);
        out.writeInt(rcvdMsgsCnt);
        out.writeLong(rcvdBytesCnt);
        out.writeInt(outMesQueueSize);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        lastUpdateTime = U.currentTimeMillis();

        maxActiveJobs = in.readInt();
        curActiveJobs = in.readInt();
        avgActiveJobs = in.readFloat();
        maxWaitingJobs = in.readInt();
        curWaitingJobs = in.readInt();
        avgWaitingJobs = in.readFloat();
        maxCancelledJobs = in.readInt();
        curCancelledJobs = in.readInt();
        avgCancelledJobs = in.readFloat();
        maxRejectedJobs = in.readInt();
        curRejectedJobs = in.readInt();
        avgRejectedJobs = in.readFloat();
        totalExecutedJobs = in.readInt();
        totalCancelledJobs = in.readInt();
        totalRejectedJobs = in.readInt();
        maxJobWaitTime = in.readLong();
        curJobWaitTime = in.readLong();
        avgJobWaitTime = in.readDouble();
        maxJobExecTime = in.readLong();
        curJobExecTime = in.readLong();
        avgJobExecTime = in.readDouble();
        totalExecTasks = in.readInt();
        curIdleTime = in.readLong();
        totalIdleTime = in.readLong();
        availProcs = in.readInt();
        totalPhysicalMemory = in.readLong();
        load = in.readDouble();
        avgLoad = in.readDouble();
        gcLoad = in.readDouble();
        heapInit = in.readLong();
        heapUsed = in.readLong();
        heapCommitted = in.readLong();
        heapMax = in.readLong();
        nonHeapInit = in.readLong();
        nonHeapUsed = in.readLong();
        nonHeapCommitted = in.readLong();
        nonHeapMax = in.readLong();
        upTime = in.readLong();
        startTime = in.readLong();
        nodeStartTime = in.readLong();
        threadCnt = in.readInt();
        peakThreadCnt = in.readInt();
        startedThreadCnt = in.readLong();
        daemonThreadCnt = in.readInt();
        lastDataVer = in.readLong();
        sentMsgsCnt = in.readInt();
        sentBytesCnt = in.readLong();
        rcvdMsgsCnt = in.readInt();
        rcvdBytesCnt = in.readLong();
        outMesQueueSize = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return System.identityHashCode(this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        DiscoveryNodeMetricsAdapter other = (DiscoveryNodeMetricsAdapter)obj;

        return
            availProcs == other.availProcs &&
            totalPhysicalMemory == other.totalPhysicalMemory &&
            curActiveJobs == other.curActiveJobs &&
            curCancelledJobs == other.curCancelledJobs &&
            curIdleTime == other.curIdleTime &&
            curJobExecTime == other.curJobExecTime &&
            curJobWaitTime == other.curJobWaitTime &&
            curRejectedJobs == other.curRejectedJobs &&
            curWaitingJobs == other.curWaitingJobs &&
            daemonThreadCnt == other.daemonThreadCnt &&
            heapCommitted == other.heapCommitted &&
            heapInit == other.heapInit &&
            heapMax == other.heapMax &&
            heapUsed == other.heapUsed &&
            maxActiveJobs == other.maxActiveJobs &&
            maxCancelledJobs == other.maxCancelledJobs &&
            maxJobExecTime == other.maxJobExecTime &&
            maxJobWaitTime == other.maxJobWaitTime &&
            maxRejectedJobs == other.maxRejectedJobs &&
            maxWaitingJobs == other.maxWaitingJobs &&
            nonHeapCommitted == other.nonHeapCommitted &&
            nonHeapInit == other.nonHeapInit &&
            nonHeapMax == other.nonHeapMax &&
            nonHeapUsed == other.nonHeapUsed &&
            peakThreadCnt == other.peakThreadCnt &&
            rcvdBytesCnt == other.rcvdBytesCnt &&
            outMesQueueSize == other.outMesQueueSize &&
            rcvdMsgsCnt == other.rcvdMsgsCnt &&
            sentBytesCnt == other.sentBytesCnt &&
            sentMsgsCnt == other.sentMsgsCnt &&
            startTime == other.startTime &&
            nodeStartTime == other.nodeStartTime &&
            startedThreadCnt == other.startedThreadCnt &&
            threadCnt == other.threadCnt &&
            totalCancelledJobs == other.totalCancelledJobs &&
            totalExecutedJobs == other.totalExecutedJobs &&
            totalExecTasks == other.totalExecTasks &&
            totalIdleTime == other.totalIdleTime &&
            totalRejectedJobs == other.totalRejectedJobs &&
            upTime == other.upTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryNodeMetricsAdapter.class, this);
    }
}

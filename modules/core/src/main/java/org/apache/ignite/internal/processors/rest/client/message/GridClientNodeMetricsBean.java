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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Node metrics bean.
 */
public class GridClientNodeMetricsBean implements Externalizable {
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
    private long fileSysFreeSpace = -1;

    /** */
    private long fileSysTotalSpace = -1;

    /** */
    private long fileSysUsableSpace = -1;

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

    /**
     * Gets last update time.
     *
     * @return Last update time.
     */
    public long getLastUpdateTime() {
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

    /**
     * Gets max active jobs.
     *
     * @return Max active jobs.
     */
    public int getMaximumActiveJobs() {
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

    /**
     * Gets current active jobs.
     *
     * @return Current active jobs.
     */
    public int getCurrentActiveJobs() {
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

    /**
     * Gets average active jobs.
     *
     * @return Average active jobs.
     */
    public float getAverageActiveJobs() {
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

    /**
     * Gets maximum waiting jobs.
     *
     * @return Maximum active jobs.
     */
    public int getMaximumWaitingJobs() {
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

    /**
     * Gets current waiting jobs.
     *
     * @return Current waiting jobs.
     */
    public int getCurrentWaitingJobs() {
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

    /**
     * Gets average waiting jobs.
     *
     * @return Average waiting jobs.
     */
    public float getAverageWaitingJobs() {
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

    /**
     * @return Maximum number of jobs rejected during a single collision resolution event.
     */
    public int getMaximumRejectedJobs() {
        return maxRejectedJobs;
    }

    /**
     * @param maxRejectedJobs Maximum number of jobs rejected during a single collision resolution event.
     */
    public void setMaximumRejectedJobs(int maxRejectedJobs) {
        this.maxRejectedJobs = maxRejectedJobs;
    }

    /**
     * @return Number of jobs rejected during most recent collision resolution.
     */
    public int getCurrentRejectedJobs() {
        return curRejectedJobs;
    }

    /**
     * @param curRejectedJobs Number of jobs rejected during most recent collision resolution.
     */
    public void setCurrentRejectedJobs(int curRejectedJobs) {
        this.curRejectedJobs = curRejectedJobs;
    }

    /**
     * @return Average number of jobs this node rejects.
     */
    public float getAverageRejectedJobs() {
        return avgRejectedJobs;
    }

    /**
     * @param avgRejectedJobs Average number of jobs this node rejects.
     */
    public void setAverageRejectedJobs(float avgRejectedJobs) {
        this.avgRejectedJobs = avgRejectedJobs;
    }

    /**
     * @return Total number of jobs this node ever rejected.
     */
    public int getTotalRejectedJobs() {
        return totalRejectedJobs;
    }

    /**
     * @param totalRejectedJobs Total number of jobs this node ever rejected.
     */
    public void setTotalRejectedJobs(int totalRejectedJobs) {
        this.totalRejectedJobs = totalRejectedJobs;
    }

    /**
     * Gets maximum cancelled jobs.
     *
     * @return Maximum cancelled jobs.
     */
    public int getMaximumCancelledJobs() {
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

    /**
     * Gets current cancelled jobs.
     *
     * @return Current cancelled jobs.
     */
    public int getCurrentCancelledJobs() {
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

    /**
     * Gets average cancelled jobs.
     *
     * @return Average cancelled jobs.
     */
    public float getAverageCancelledJobs() {
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

    /**
     * Gets total active jobs.
     *
     * @return Total active jobs.
     */
    public int getTotalExecutedJobs() {
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

    /**
     * Gets total cancelled jobs.
     *
     * @return Total cancelled jobs.
     */
    public int getTotalCancelledJobs() {
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

    /**
     * Gets max job wait time.
     *
     * @return Max job wait time.
     */
    public long getMaximumJobWaitTime() {
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

    /**
     * Gets current job wait time.
     *
     * @return Current job wait time.
     */
    public long getCurrentJobWaitTime() {
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

    /**
     * Gets average job wait time.
     *
     * @return Average job wait time.
     */
    public double getAverageJobWaitTime() {
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

    /**
     * Gets maximum job execution time.
     *
     * @return Maximum job execution time.
     */
    public long getMaximumJobExecuteTime() {
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

    /**
     * Gets current job execute time.
     *
     * @return Current job execute time.
     */
    public long getCurrentJobExecuteTime() {
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

    /**
     * Gets average job execution time.
     *
     * @return Average job execution time.
     */
    public double getAverageJobExecuteTime() {
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

    /**
     * Gets total number of tasks handled by the node.
     *
     * @return Total number of tasks handled by the node.
     */
    public int getTotalExecutedTasks() {
        return totalExecTasks;
    }

    /**
     * Sets total number of tasks handled by the node.
     *
     * @param totalExecTasks Total number of tasks handled by the node.
     */
    public void setTotalExecutedTasks(int totalExecTasks) {
        this.totalExecTasks = totalExecTasks;
    }

    /**
     * @return Total busy time.
     */
    public long getTotalBusyTime() {
        return getUpTime() - getTotalIdleTime();
    }

    /**
     * @return Total idle time.
     */
    public long getTotalIdleTime() {
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

    /**
     * @return Current idle time.
     */
    public long getCurrentIdleTime() {
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

    /**
     * Gets percentage of time this node is busy executing jobs vs. idling.
     *
     * @return Percentage of time this node is busy (value is less than
     *      or equal to {@code 1} and greater than or equal to {@code 0})
     */
    public float getBusyTimePercentage() {
        return 1 - getIdleTimePercentage();
    }

    /**
     * Gets percentage of time this node is idling vs. executing jobs.
     *
     * @return Percentage of time this node is idle (value is less than
     *      or equal to {@code 1} and greater than or equal to {@code 0})
     */
    public float getIdleTimePercentage() {
        return getTotalIdleTime() / (float)getUpTime();
    }

    /**
     * Returns the number of CPUs available to the Java Virtual Machine.
     * This method is equivalent to the {@link Runtime#availableProcessors()}
     * method.
     * <p>
     * Note that this value may change during successive invocations of the
     * virtual machine.
     *
     * @return The number of processors available to the virtual
     *      machine, never smaller than one.
     */
    public int getTotalCpus() {
        return availProcs;
    }

    /**
     * Returns the system load average for the last minute.
     * The system load average is the sum of the number of runnable entities
     * queued to the {@linkplain #getTotalCpus available processors}
     * and the number of runnable entities running on the available processors
     * averaged over a period of time.
     * The way in which the load average is calculated is operating system
     * specific but is typically a damped time-dependent average.
     * <p>
     * If the load average is not available, a negative value is returned.
     * <p>
     * This method is designed to provide a hint about the system load
     * and may be queried frequently. The load average may be unavailable on
     * some platform where it is expensive to implement this method.
     *
     * @return The system load average in {@code [0, 1]} range.
     *      Negative value if not available.
     */
    public double getCurrentCpuLoad() {
        return load;
    }

    /**
     * Gets average of CPU load values over all metrics kept in the history.
     *
     * @return Average of CPU load value in {@code [0, 1]} range over all metrics kept
     *      in the history.
     */
    public double getAverageCpuLoad() {
        return avgLoad;
    }

    /**
     * Returns average CPU spent for CG since the last update.
     *
     * @return Average CPU spent for CG since the last update.
     */
    public double getCurrentGcCpuLoad() {
        return gcLoad;
    }

    /**
     * Returns the amount of heap memory in bytes that the Java virtual machine
     * initially requests from the operating system for memory management.
     * This method returns {@code -1} if the initial memory size is undefined.
     *
     * @return The initial size of memory in bytes; {@code -1} if undefined.
     */
    public long getHeapMemoryInitialized() {
        return heapInit;
    }

    /**
     * Returns the current heap size that is used for object allocation.
     * The heap consists of one or more memory pools. This value is
     * the sum of {@code used} heap memory values of all heap memory pools.
     * <p>
     * The amount of used memory in the returned is the amount of memory
     * occupied by both live objects and garbage objects that have not
     * been collected, if any.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return Amount of heap memory used.
     */
    public long getHeapMemoryUsed() {
        return heapUsed;
    }

    /**
     * Returns the amount of heap memory in bytes that is committed for
     * the Java virtual machine to use. This amount of memory is
     * guaranteed for the Java virtual machine to use.
     * The heap consists of one or more memory pools. This value is
     * the sum of {@code committed} heap memory values of all heap memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The amount of committed memory in bytes.
     */
    public long getHeapMemoryCommitted() {
        return heapCommitted;
    }

    /**
     * Returns the maximum amount of heap memory in bytes that can be
     * used for memory management. This method returns {@code -1}
     * if the maximum memory size is undefined.
     * <p>
     * This amount of memory is not guaranteed to be available
     * for memory management if it is greater than the amount of
     * committed memory. The Java virtual machine may fail to allocate
     * memory even if the amount of used memory does not exceed this
     * maximum size.
     * <p>
     * This value represents a setting of the heap memory for Java VM and is
     * not a sum of all initial heap values for all memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The maximum amount of memory in bytes; {@code -1} if undefined.
     */
    public long getHeapMemoryMaximum() {
        return heapMax;
    }

    /**
     * Returns the amount of non-heap memory in bytes that the Java virtual machine
     * initially requests from the operating system for memory management.
     * This method returns {@code -1} if the initial memory size is undefined.
     * <p>
     * This value represents a setting of non-heap memory for Java VM and is
     * not a sum of all initial heap values for all memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The initial size of memory in bytes; {@code -1} if undefined.
     */
    public long getNonHeapMemoryInitialized() {
        return nonHeapInit;
    }

    /**
     * Returns the current non-heap memory size that is used by Java VM.
     * The non-heap memory consists of one or more memory pools. This value is
     * the sum of {@code used} non-heap memory values of all non-heap memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return Amount of none-heap memory used.
     */
    public long getNonHeapMemoryUsed() {
        return nonHeapUsed;
    }

    /**
     * Returns the amount of non-heap memory in bytes that is committed for
     * the Java virtual machine to use. This amount of memory is
     * guaranteed for the Java virtual machine to use.
     * The non-heap memory consists of one or more memory pools. This value is
     * the sum of {@code committed} non-heap memory values of all non-heap memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The amount of committed memory in bytes.
     */
    public long getNonHeapMemoryCommitted() {
        return nonHeapCommitted;
    }

    /**
     * Returns the maximum amount of non-heap memory in bytes that can be
     * used for memory management. This method returns {@code -1}
     * if the maximum memory size is undefined.
     * <p>
     * This amount of memory is not guaranteed to be available
     * for memory management if it is greater than the amount of
     * committed memory.  The Java virtual machine may fail to allocate
     * memory even if the amount of used memory does not exceed this
     * maximum size.
     * <p>
     * This value represents a setting of the non-heap memory for Java VM and is
     * not a sum of all initial non-heap values for all memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The maximum amount of memory in bytes; {@code -1} if undefined.
     */
    public long getNonHeapMemoryMaximum() {
        return nonHeapMax;
    }

    /**
     * Returns the uptime of the Java virtual machine in milliseconds.
     *
     * @return Uptime of the Java virtual machine in milliseconds.
     */
    public long getUpTime() {
        return upTime;
    }

    /**
     * Returns the start time of the Java virtual machine in milliseconds.
     * This method returns the approximate time when the Java virtual
     * machine started.
     *
     * @return Start time of the Java virtual machine in milliseconds.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the start time of grid node in milliseconds.
     * There can be several grid nodes started in one JVM, so JVM start time will be
     * the same for all of them, but node start time will be different.
     *
     * @return Start time of the grid node in milliseconds.
     */
    public long getNodeStartTime() {
        return nodeStartTime;
    }

    /**
     * Returns the current number of live threads including both
     * daemon and non-daemon threads.
     *
     * @return Current number of live threads.
     */
    public int getCurrentThreadCount() {
        return threadCnt;
    }

    /**
     * Returns the maximum live thread count since the Java virtual machine
     * started or peak was reset.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The peak live thread count.
     */
    public int getMaximumThreadCount() {
        return peakThreadCnt;
    }

    /**
     * Returns the total number of threads created and also started
     * since the Java virtual machine started.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The total number of threads started.
     */
    public long getTotalStartedThreadCount() {
        return startedThreadCnt;
    }

    /**
     * Returns the current number of live daemon threads.
     *
     * @return Current number of live daemon threads.
     */
    public int getCurrentDaemonThreadCount() {
        return daemonThreadCnt;
    }

    /**
     * Returns the number of unallocated bytes in the partition.
     *
     * @return Number of unallocated bytes in the partition.
     */
    public long getFileSystemFreeSpace() {
        return fileSysFreeSpace;
    }

    /**
     * Returns the size of the partition.
     *
     * @return Size of the partition.
     */
    public long getFileSystemTotalSpace() {
        return fileSysTotalSpace;
    }

    /**
     * Returns the number of bytes available to this virtual machine on the partition.
     *
     * @return Number of bytes available to this virtual machine on the partition.
     */
    public long getFileSystemUsableSpace() {
        return fileSysUsableSpace;
    }

    /**
     * In-memory data grid assigns incremental versions to all cache operations. This method provides
     * the latest data version on the node.
     *
     * @return Last data version.
     */
    public long getLastDataVersion() {
        return lastDataVer;
    }

    /**
     * Sets available processors.
     *
     * @param availProcs Available processors.
     */
    public void setTotalCpus(int availProcs) {
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
     * Sets current GC CPU load.
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
     * @param upTime VN up time.
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
     * Sets the number of unallocated bytes in the partition.
     *
     * @param fileSysFreeSpace The number of unallocated bytes in the partition.
     */
    public void setFileSystemFreeSpace(long fileSysFreeSpace) {
        this.fileSysFreeSpace = fileSysFreeSpace;
    }

    /**
     * Sets size of the partition.
     *
     * @param fileSysTotalSpace Size of the partition.
     */
    public void setFileSystemTotalSpace(long fileSysTotalSpace) {
        this.fileSysTotalSpace = fileSysTotalSpace;
    }

    /**
     * Sets the number of bytes available to this virtual machine on the partition.
     *
     * @param fileSysUsableSpace The number of bytes available to
     *      this virtual machine on the partition.
     */
    public void setFileSystemUsableSpace(long fileSysUsableSpace) {
        this.fileSysUsableSpace = fileSysUsableSpace;
    }

    /**
     * @param lastDataVer Last data version.
     */
    public void setLastDataVersion(long lastDataVer) {
        this.lastDataVer = lastDataVer;
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int getSentMessagesCount() {
        return sentMsgsCnt;
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
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long getSentBytesCount() {
        return sentBytesCnt;
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
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int getReceivedMessagesCount() {
        return rcvdMsgsCnt;
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
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long getReceivedBytesCount() {
        return rcvdBytesCnt;
    }

    /**
     * Sets received bytes count.
     *
     * @param rcvdBytesCnt Received bytes count.
     */
    public void setReceivedBytesCount(long rcvdBytesCnt) {
        this.rcvdBytesCnt = rcvdBytesCnt;
    }

    /** {@inheritDoc} */
    public int hashCode() {
        return System.identityHashCode(this);
    }

    /** {@inheritDoc} */
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        GridClientNodeMetricsBean other = (GridClientNodeMetricsBean)obj;

        return availProcs == other.availProcs &&
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
            startTime == other.startTime &&
            nodeStartTime == other.nodeStartTime &&
            startedThreadCnt == other.startedThreadCnt &&
            threadCnt == other.threadCnt &&
            totalCancelledJobs == other.totalCancelledJobs &&
            totalExecutedJobs == other.totalExecutedJobs &&
            totalIdleTime == other.totalIdleTime &&
            totalRejectedJobs == other.totalRejectedJobs &&
            fileSysFreeSpace == other.fileSysFreeSpace &&
            fileSysTotalSpace == other.fileSysTotalSpace &&
            fileSysUsableSpace == other.fileSysUsableSpace &&
            totalExecTasks == other.totalExecTasks &&
            sentMsgsCnt == other.sentMsgsCnt &&
            sentBytesCnt == other.sentBytesCnt &&
            rcvdMsgsCnt == other.rcvdMsgsCnt &&
            rcvdBytesCnt == other.rcvdBytesCnt &&
            upTime == other.upTime;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StringBufferReplaceableByString")
    @Override public String toString() {
        return new StringBuilder().
            append("GridClientNodeMetricsBean [lastUpdateTime=").
            append(lastUpdateTime).
            append(", maxActiveJobs=").append(maxActiveJobs).
            append(", curActiveJobs=").append(curActiveJobs).
            append(", avgActiveJobs=").append(avgActiveJobs).
            append(", maxWaitingJobs=").append(maxWaitingJobs).
            append(", curWaitingJobs=").append(curWaitingJobs).
            append(", avgWaitingJobs=").append(avgWaitingJobs).
            append(", maxRejectedJobs=").append(maxRejectedJobs).
            append(", curRejectedJobs=").append(curRejectedJobs).
            append(", avgRejectedJobs=").append(avgRejectedJobs).
            append(", maxCancelledJobs=").append(maxCancelledJobs).
            append(", curCancelledJobs=").append(curCancelledJobs).
            append(", avgCancelledJobs=").append(avgCancelledJobs).
            append(", totalRejectedJobs=").append(totalRejectedJobs).
            append(", totalCancelledJobs=").append(totalCancelledJobs).
            append(", totalExecutedJobs=").append(totalExecutedJobs).
            append(", maxJobWaitTime=").append(maxJobWaitTime).
            append(", curJobWaitTime=").append(curJobWaitTime).
            append(", avgJobWaitTime=").append(avgJobWaitTime).
            append(", maxJobExecTime=").append(maxJobExecTime).
            append(", curJobExecTime=").append(curJobExecTime).
            append(", avgJobExecTime=").append(avgJobExecTime).
            append(", totalExecTasks=").append(totalExecTasks).
            append(", totalIdleTime=").append(totalIdleTime).
            append(", curIdleTime=").append(curIdleTime).
            append(", availProcs=").append(availProcs).
            append(", load=").append(load).
            append(", avgLoad=").append(avgLoad).
            append(", gcLoad=").append(gcLoad).
            append(", heapInit=").append(heapInit).
            append(", heapUsed=").append(heapUsed).
            append(", heapCommitted=").append(heapCommitted).
            append(", heapMax=").append(heapMax).
            append(", nonHeapInit=").append(nonHeapInit).
            append(", nonHeapUsed=").append(nonHeapUsed).
            append(", nonHeapCommitted=").append(nonHeapCommitted).
            append(", nonHeapMax=").append(nonHeapMax).
            append(", upTime=").append(upTime).
            append(", startTime=").append(startTime).
            append(", nodeStartTime=").append(nodeStartTime).
            append(", threadCnt=").append(threadCnt).
            append(", peakThreadCnt=").append(peakThreadCnt).
            append(", startedThreadCnt=").append(startedThreadCnt).
            append(", daemonThreadCnt=").append(daemonThreadCnt).
            append(", fileSysFreeSpace=").append(fileSysFreeSpace).
            append(", fileSysTotalSpace=").append(fileSysTotalSpace).
            append(", fileSysUsableSpace=").append(fileSysUsableSpace).
            append(", lastDataVer=").append(lastDataVer).
            append(", sentMsgsCnt=").append(sentMsgsCnt).
            append(", sentBytesCnt=").append(sentBytesCnt).
            append(", rcvdMsgsCnt=").append(rcvdMsgsCnt).
            append(", rcvdBytesCnt=").append(rcvdBytesCnt).
            append("]").
            toString();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(lastUpdateTime);
        out.writeInt(maxActiveJobs);
        out.writeInt(curActiveJobs);
        out.writeFloat(avgActiveJobs);
        out.writeInt(maxWaitingJobs);
        out.writeInt(curWaitingJobs);
        out.writeFloat(avgWaitingJobs);
        out.writeInt(maxRejectedJobs);
        out.writeInt(curRejectedJobs);
        out.writeFloat(avgRejectedJobs);
        out.writeInt(maxCancelledJobs);
        out.writeInt(curCancelledJobs);
        out.writeFloat(avgCancelledJobs);
        out.writeInt(totalRejectedJobs);
        out.writeInt(totalCancelledJobs);
        out.writeInt(totalExecutedJobs);
        out.writeLong(maxJobWaitTime);
        out.writeLong(curJobWaitTime);
        out.writeDouble(avgJobWaitTime);
        out.writeLong(maxJobExecTime);
        out.writeLong(curJobExecTime);
        out.writeDouble(avgJobExecTime);
        out.writeInt(totalExecTasks);
        out.writeLong(totalIdleTime);
        out.writeLong(curIdleTime);
        out.writeInt(availProcs);
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
        out.writeLong(fileSysFreeSpace);
        out.writeLong(fileSysTotalSpace);
        out.writeLong(fileSysUsableSpace);
        out.writeLong(lastDataVer);
        out.writeInt(sentMsgsCnt);
        out.writeLong(sentBytesCnt);
        out.writeInt(rcvdMsgsCnt);
        out.writeLong(rcvdBytesCnt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        lastUpdateTime = in.readLong();
        maxActiveJobs = in.readInt();
        curActiveJobs = in.readInt();
        avgActiveJobs = in.readFloat();
        maxWaitingJobs = in.readInt();
        curWaitingJobs = in.readInt();
        avgWaitingJobs = in.readFloat();
        maxRejectedJobs = in.readInt();
        curRejectedJobs = in.readInt();
        avgRejectedJobs = in.readFloat();
        maxCancelledJobs = in.readInt();
        curCancelledJobs = in.readInt();
        avgCancelledJobs = in.readFloat();
        totalRejectedJobs = in.readInt();
        totalCancelledJobs = in.readInt();
        totalExecutedJobs = in.readInt();
        maxJobWaitTime = in.readLong();
        curJobWaitTime = in.readLong();
        avgJobWaitTime = in.readDouble();
        maxJobExecTime = in.readLong();
        curJobExecTime = in.readLong();
        avgJobExecTime = in.readDouble();
        totalExecTasks = in.readInt();
        totalIdleTime = in.readLong();
        curIdleTime = in.readLong();
        availProcs = in.readInt();
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
        fileSysFreeSpace = in.readLong();
        fileSysTotalSpace = in.readLong();
        fileSysUsableSpace = in.readLong();
        lastDataVer = in.readLong();
        sentMsgsCnt = in.readInt();
        sentBytesCnt = in.readLong();
        rcvdMsgsCnt = in.readInt();
        rcvdBytesCnt = in.readLong();
    }
}
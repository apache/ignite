/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLEINTNODEMETRICBEAN_HPP_INCLUDED
#define GRIDCLEINTNODEMETRICBEAN_HPP_INCLUDED

#include <cstdint>

#include <gridgain/gridconf.hpp>

/**
 * Node metrics bean.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
 class GRIDGAIN_API GridClientNodeMetricsBean {
 private:
    /** */
    int64_t lastUpdateTime;

    /** */
    int maxActiveJobs;

    /** */
    int curActiveJobs;

    /** */
    float avgActiveJobs;

    /** */
    int maxWaitingJobs;

    /** */
    int curWaitingJobs;

    /** */
    float avgWaitingJobs;

    /** */
    int maxRejectedJobs;

    /** */
    int curRejectedJobs;

    /** */
    float avgRejectedJobs;

    /** */
    int maxCancelledJobs;

    /** */
    int curCancelledJobs;

    /** */
    float avgCancelledJobs;

    /** */
    int totalRejectedJobs;

    /** */
    int totalCancelledJobs;

    /** */
    int totalExecutedJobs;

    /** */
    int64_t maxJobWaitTime;

    /** */
    int64_t curJobWaitTime;

    /** */
    double avgJobWaitTime;

    /** */
    int64_t maxJobExecTime;

    /** */
    int64_t curJobExecTime;

    /** */
    double avgJobExecTime;

    /** */
    int64_t totalIdleTime;

    /** */
    int64_t curIdleTime;

    /** */
    int availProcs;

    /** */
    double load;

    /** */
    double avgLoad;

    /** */
    int64_t heapInit;

    /** */
    int64_t heapUsed;

    /** */
    int64_t heapCommitted;

    /** */
    int64_t heapMax;

    /** */
    int64_t nonHeapInit;

    /** */
    int64_t nonHeapUsed;

    /** */
    int64_t nonHeapCommitted;

    /** */
    int64_t nonHeapMax;

    /** */
    int64_t upTime;

    /** */
    int64_t startTime;

    /** */
    int64_t nodeStartTime;

    /** */
    int threadCnt;

    /** */
    int peakThreadCnt;

    /** */
    int64_t startedThreadCnt;

    /** */
    int daemonThreadCnt;

    /** */
    int64_t fileSysFreeSpace;

    /** */
    int64_t fileSysTotalSpace;

    /** */
    int64_t fileSysUsableSpace;

    /** */
    int64_t lastDataVer;

public:
    GridClientNodeMetricsBean() {
        lastUpdateTime = -1;

        maxActiveJobs = -1;

        curActiveJobs = -1;

        avgActiveJobs = -1;

        maxWaitingJobs = -1;

        curWaitingJobs = -1;

        avgWaitingJobs = -1;

        maxRejectedJobs = -1;

        curRejectedJobs = -1;

        avgRejectedJobs = -1;

        maxCancelledJobs = -1;

        curCancelledJobs = -1;

        avgCancelledJobs = -1;

        totalRejectedJobs = -1;

        totalCancelledJobs = -1;

        totalExecutedJobs = -1;

        maxJobWaitTime = -1;

        curJobWaitTime = -1;

        avgJobWaitTime = -1;

        maxJobExecTime = -1;

        curJobExecTime = -1;

        avgJobExecTime = -1;

        totalIdleTime = -1;

        curIdleTime = -1;

        availProcs = -1;

        load = -1;

        avgLoad = -1;

        heapInit = -1;

        heapUsed = -1;

        heapCommitted = -1;

        heapMax = -1;

        nonHeapInit = -1;

        nonHeapUsed = -1;

        nonHeapCommitted = -1;

        nonHeapMax = -1;

        upTime = -1;

        startTime = -1;

        nodeStartTime = -1;

        threadCnt = -1;

        peakThreadCnt = -1;

        startedThreadCnt = -1;

        daemonThreadCnt = -1;

        fileSysFreeSpace = -1;

        fileSysTotalSpace = -1;

        fileSysUsableSpace = -1;

        lastDataVer = -1;
    }

    /**
     * Gets last update time.
     *
     * @return Last update time.
     */
    int64_t getLastUpdateTime() const {
        return lastUpdateTime;
    }

    /**
     * Sets last update time.
     *
     * @param lastUpdateTime Last update time.
     */
    void setLastUpdateTime(int64_t pLastUpdateTime) {
        lastUpdateTime = pLastUpdateTime;
    }

    /**
     * Gets max active jobs.
     *
     * @return Max active jobs.
     */
    int getMaximumActiveJobs() const {
        return maxActiveJobs;
    }

    /**
     * Sets max active jobs.
     *
     * @param maxActiveJobs Max active jobs.
     */
    void setMaximumActiveJobs(int pMaxActiveJobs) {
        maxActiveJobs = pMaxActiveJobs;
    }

    /**
     * Gets current active jobs.
     *
     * @return Current active jobs.
     */
    int getCurrentActiveJobs() const {
        return curActiveJobs;
    }

    /**
     * Sets current active jobs.
     *
     * @param curActiveJobs Current active jobs.
     */
    void setCurrentActiveJobs(int pCurActiveJobs) {
        curActiveJobs = pCurActiveJobs;
    }

    /**
     * Gets average active jobs.
     *
     * @return Average active jobs.
     */
    float getAverageActiveJobs() const {
        return avgActiveJobs;
    }

    /**
     * Sets average active jobs.
     *
     * @param avgActiveJobs Average active jobs.
     */
    void setAverageActiveJobs(float pAvgActiveJobs) {
        avgActiveJobs = pAvgActiveJobs;
    }

    /**
     * Gets maximum waiting jobs.
     *
     * @return Maximum active jobs.
     */
    int getMaximumWaitingJobs() const {
        return maxWaitingJobs;
    }

    /**
     * Sets maximum waiting jobs.
     *
     * @param maxWaitingJobs Maximum waiting jobs.
     */
    void setMaximumWaitingJobs(int pMaxWaitingJobs) {
        maxWaitingJobs = pMaxWaitingJobs;
    }

    /**
     * Gets current waiting jobs.
     *
     * @return Current waiting jobs.
     */
    int getCurrentWaitingJobs() const {
        return curWaitingJobs;
    }

    /**
     * Sets current waiting jobs.
     *
     * @param curWaitingJobs Current waiting jobs.
     */
    void setCurrentWaitingJobs(int pCurWaitingJobs) {
        curWaitingJobs = pCurWaitingJobs;
    }

    /**
     * Gets average waiting jobs.
     *
     * @return Average waiting jobs.
     */
    float getAverageWaitingJobs() const {
        return avgWaitingJobs;
    }

    /**
     * Sets average waiting jobs.
     *
     * @param avgWaitingJobs Average waiting jobs.
     */
    void setAverageWaitingJobs(float pAvgWaitingJobs) {
        avgWaitingJobs = pAvgWaitingJobs;
    }

    /**
     * @return Maximum number of jobs rejected during a single collision resolution event.
     */
    int getMaximumRejectedJobs() const {
        return maxRejectedJobs;
    }

    /**
     * @param maxRejectedJobs Maximum number of jobs rejected during a single collision resolution event.
     */
    void setMaximumRejectedJobs(int pMaxRejectedJobs) {
        maxRejectedJobs = pMaxRejectedJobs;
    }

    /**
     * @return Number of jobs rejected during most recent collision resolution.
     */
    int getCurrentRejectedJobs() const {
        return curRejectedJobs;
    }

    /**
     * @param curRejectedJobs Number of jobs rejected during most recent collision resolution.
     */
    void setCurrentRejectedJobs(int pCurRejectedJobs) {
        curRejectedJobs = pCurRejectedJobs;
    }

    /**
     * @return Average number of jobs this node rejects.
     */
    float getAverageRejectedJobs() const {
        return avgRejectedJobs;
    }

    /**
     * @param avgRejectedJobs Average number of jobs this node rejects.
     */
    void setAverageRejectedJobs(float pAvgRejectedJobs) {
        avgRejectedJobs = pAvgRejectedJobs;
    }

    /**
     * @return Total number of jobs this node ever rejected.
     */
    int getTotalRejectedJobs() const {
        return totalRejectedJobs;
    }

    /**
     * @param totalRejectedJobs Total number of jobs this node ever rejected.
     */
    void setTotalRejectedJobs(int pTotalRejectedJobs) {
        totalRejectedJobs = pTotalRejectedJobs;
    }

    /**
     * Gets maximum cancelled jobs.
     *
     * @return Maximum cancelled jobs.
     */
    int getMaximumCancelledJobs() const {
        return maxCancelledJobs;
    }

    /**
     * Sets maximum cancelled jobs.
     *
     * @param maxCancelledJobs Maximum cancelled jobs.
     */
    void setMaximumCancelledJobs(int pMaxCancelledJobs) {
        maxCancelledJobs = pMaxCancelledJobs;
    }

    /**
     * Gets current cancelled jobs.
     *
     * @return Current cancelled jobs.
     */
    int getCurrentCancelledJobs() const {
        return curCancelledJobs;
    }

    /**
     * Sets current cancelled jobs.
     *
     * @param curCancelledJobs Current cancelled jobs.
     */
    void setCurrentCancelledJobs(int pCurCancelledJobs) {
        curCancelledJobs = pCurCancelledJobs;
    }

    /**
     * Gets average cancelled jobs.
     *
     * @return Average cancelled jobs.
     */
    float getAverageCancelledJobs() const {
        return avgCancelledJobs;
    }

    /**
     * Sets average cancelled jobs.
     *
     * @param avgCancelledJobs Average cancelled jobs.
     */
    void setAverageCancelledJobs(float pAvgCancelledJobs) {
        avgCancelledJobs = pAvgCancelledJobs;
    }

    /**
     * Gets total active jobs.
     *
     * @return Total active jobs.
     */
    int getTotalExecutedJobs() const {
        return totalExecutedJobs;
    }

    /**
     * Sets total active jobs.
     *
     * @param totalExecutedJobs Total active jobs.
     */
    void setTotalExecutedJobs(int pTotalExecutedJobs) {
        totalExecutedJobs = pTotalExecutedJobs;
    }

    /**
     * Gets total cancelled jobs.
     *
     * @return Total cancelled jobs.
     */
    int getTotalCancelledJobs() const {
        return totalCancelledJobs;
    }

    /**
     * Sets total cancelled jobs.
     *
     * @param totalCancelledJobs Total cancelled jobs.
     */
    void setTotalCancelledJobs(int pTotalCancelledJobs) {
        totalCancelledJobs = pTotalCancelledJobs;
    }

    /**
     * Gets max job wait time.
     *
     * @return Max job wait time.
     */
    int64_t getMaximumJobWaitTime() const {
        return maxJobWaitTime;
    }

    /**
     * Sets max job wait time.
     *
     * @param maxJobWaitTime Max job wait time.
     */
    void setMaximumJobWaitTime(int64_t pMaxJobWaitTime) {
        maxJobWaitTime = pMaxJobWaitTime;
    }

    /**
     * Gets current job wait time.
     *
     * @return Current job wait time.
     */
    int64_t getCurrentJobWaitTime() const{
        return curJobWaitTime;
    }

    /**
     * Sets current job wait time.
     *
     * @param curJobWaitTime Current job wait time.
     */
    void setCurrentJobWaitTime(int64_t pCurJobWaitTime) {
        curJobWaitTime = pCurJobWaitTime;
    }

    /**
     * Gets average job wait time.
     *
     * @return Average job wait time.
     */
    double getAverageJobWaitTime() const {
        return avgJobWaitTime;
    }

    /**
     * Sets average job wait time.
     *
     * @param avgJobWaitTime Average job wait time.
     */
    void setAverageJobWaitTime(double pAvgJobWaitTime) {
        avgJobWaitTime = pAvgJobWaitTime;
    }

    /**
     * Gets maximum job execution time.
     *
     * @return Maximum job execution time.
     */
    int64_t getMaximumJobExecuteTime() const {
        return maxJobExecTime;
    }

    /**
     * Sets maximum job execution time.
     *
     * @param maxJobExecTime Maximum job execution time.
     */
    void setMaximumJobExecuteTime(int64_t pMaxJobExecTime) {
        maxJobExecTime = pMaxJobExecTime;
    }

    /**
     * Gets current job execute time.
     *
     * @return Current job execute time.
     */
    int64_t getCurrentJobExecuteTime() const {
        return curJobExecTime;
    }

    /**
     * Sets current job execute time.
     *
     * @param curJobExecTime Current job execute time.
     */
    void setCurrentJobExecuteTime(int64_t pCurJobExecTime) {
        curJobExecTime = pCurJobExecTime;
    }

    /**
     * Gets average job execution time.
     *
     * @return Average job execution time.
     */
    double getAverageJobExecuteTime() const {
        return avgJobExecTime;
    }

    /**
     * Sets average job execution time.
     *
     * @param avgJobExecTime Average job execution time.
     */
    void setAverageJobExecuteTime(double pAvgJobExecTime) {
        avgJobExecTime = pAvgJobExecTime;
    }

    /**
     * @return Total busy time.
     */
    int64_t getTotalBusyTime() const {
        return getUpTime() - getTotalIdleTime();
    }

    /**
     * @return Total idle time.
     */
    int64_t getTotalIdleTime() const {
        return totalIdleTime;
    }

    /**
     * Set total node idle time.
     *
     * @param totalIdleTime Total node idle time.
     */
    void setTotalIdleTime(int64_t pTotalIdleTime) {
        totalIdleTime = pTotalIdleTime;
    }

    /**
     * @return Current idle time.
     */
    int64_t getCurrentIdleTime() const {
        return curIdleTime;
    }

    /**
     * Sets time elapsed since execution of last job.
     *
     * @param curIdleTime Time elapsed since execution of last job.
     */
    void setCurrentIdleTime(int64_t pCurIdleTime) {
        curIdleTime = pCurIdleTime;
    }

    /**
     * Gets percentage of time this node is busy executing jobs vs. idling.
     *
     * @return Percentage of time this node is busy (value is less than
     *      or equal to <tt>1</tt> and greater than or equal to <tt>0</tt>)
     */
    float getBusyTimePercentage() const {
        return 1 - getIdleTimePercentage();
    }

    /**
     * Gets percentage of time this node is idling vs. executing jobs.
     *
     * @return Percentage of time this node is idle (value is less than
     *      or equal to <tt>1</tt> and greater than or equal to <tt>0</tt>)
     */
    float getIdleTimePercentage() const {
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
    int getTotalCpus() const {
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
     * @return The system load average in <code>[0, 1]</code> range.
     *      Negative value if not available.
     */
    double getCurrentCpuLoad() const {
        return load;
    }

    /**
     * Gets average of CPU load values over all metrics kept in the history.
     *
     * @return Average of CPU load value in <code>[0, 1]</code> range over all metrics kept
     *      in the history.
     */
    double getAverageCpuLoad() const {
        return avgLoad;
    }

    /**
     * Returns the amount of heap memory in bytes that the Java virtual machine
     * initially requests from the operating system for memory management.
     * This method returns <tt>-1</tt> if the initial memory size is undefined.
     *
     * @return The initial size of memory in bytes; <tt>-1</tt> if undefined.
     */
    int64_t getHeapMemoryInitialized() const {
        return heapInit;
    }

    /**
     * Returns the current heap size that is used for object allocation.
     * The heap consists of one or more memory pools. This value is
     * the sum of <tt>used</tt> heap memory values of all heap memory pools.
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
    int64_t getHeapMemoryUsed() const {
        return heapUsed;
    }

    /**
     * Returns the amount of heap memory in bytes that is committed for
     * the Java virtual machine to use. This amount of memory is
     * guaranteed for the Java virtual machine to use.
     * The heap consists of one or more memory pools. This value is
     * the sum of <tt>committed</tt> heap memory values of all heap memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The amount of committed memory in bytes.
     */
    int64_t getHeapMemoryCommitted() const {
        return heapCommitted;
    }

    /**
     * Returns the maximum amount of heap memory in bytes that can be
     * used for memory management. This method returns <tt>-1</tt>
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
     * @return The maximum amount of memory in bytes; <tt>-1</tt> if undefined.
     */
    int64_t getHeapMemoryMaximum() const {
        return heapMax;
    }

    /**
     * Returns the amount of non-heap memory in bytes that the Java virtual machine
     * initially requests from the operating system for memory management.
     * This method returns <tt>-1</tt> if the initial memory size is undefined.
     * <p>
     * This value represents a setting of non-heap memory for Java VM and is
     * not a sum of all initial heap values for all memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The initial size of memory in bytes; <tt>-1</tt> if undefined.
     */
    int64_t getNonHeapMemoryInitialized() const {
        return nonHeapInit;
    }

    /**
     * Returns the current non-heap memory size that is used by Java VM.
     * The non-heap memory consists of one or more memory pools. This value is
     * the sum of <tt>used</tt> non-heap memory values of all non-heap memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return Amount of none-heap memory used.
     */
    int64_t getNonHeapMemoryUsed() const {
        return nonHeapUsed;
    }

    /**
     * Returns the amount of non-heap memory in bytes that is committed for
     * the Java virtual machine to use. This amount of memory is
     * guaranteed for the Java virtual machine to use.
     * The non-heap memory consists of one or more memory pools. This value is
     * the sum of <tt>committed</tt> non-heap memory values of all non-heap memory pools.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The amount of committed memory in bytes.
     */
    int64_t getNonHeapMemoryCommitted() const {
        return nonHeapCommitted;
    }

    /**
     * Returns the maximum amount of non-heap memory in bytes that can be
     * used for memory management. This method returns <tt>-1</tt>
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
     * @return The maximum amount of memory in bytes; <tt>-1</tt> if undefined.
     */
    int64_t getNonHeapMemoryMaximum() const {
        return nonHeapMax;
    }

    /**
     * Returns the uptime of the Java virtual machine in milliseconds.
     *
     * @return Uptime of the Java virtual machine in milliseconds.
     */
    int64_t getUpTime() const {
        return upTime;
    }

    /**
     * Returns the start time of the Java virtual machine in milliseconds.
     * This method returns the approximate time when the Java virtual
     * machine started.
     *
     * @return Start time of the Java virtual machine in milliseconds.
     */
    int64_t getStartTime() const {
        return startTime;
    }

    /**
     * Returns the start time of grid node in milliseconds.
     * There can be several grid nodes started in one JVM, so JVM start time will be
     * the same for all of them, but node start time will be different.
     *
     * @return Start time of the grid node in milliseconds.
     */
    int64_t getNodeStartTime() const {
        return nodeStartTime;
    }

    /**
     * Returns the current number of live threads including both
     * daemon and non-daemon threads.
     *
     * @return Current number of live threads.
     */
    int getCurrentThreadCount() const {
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
    int getMaximumThreadCount() const {
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
    int64_t getTotalStartedThreadCount() const {
        return startedThreadCnt;
    }

    /**
     * Returns the current number of live daemon threads.
     *
     * @return Current number of live daemon threads.
     */
    int getCurrentDaemonThreadCount() const {
        return daemonThreadCnt;
    }

    /**
     * Returns the number of unallocated bytes in the partition.
     *
     * @return Number of unallocated bytes in the partition.
     */
    int64_t getFileSystemFreeSpace() const {
        return fileSysFreeSpace;
    }

    /**
     * Returns the size of the partition.
     *
     * @return Size of the partition.
     */
    int64_t getFileSystemTotalSpace() const {
        return fileSysTotalSpace;
    }

    /**
     * Returns the number of bytes available to this virtual machine on the partition.
     *
     * @return Number of bytes available to this virtual machine on the partition.
     */
    int64_t getFileSystemUsableSpace() const {
        return fileSysUsableSpace;
    }

    /**
     * In-memory data grid assigns incremental versions to all cache operations. This method provides
     * the latest data version on the node.
     *
     * @return Last data version.
     */
    int64_t getLastDataVersion() const {
        return lastDataVer;
    }

    /**
     * Sets available processors.
     *
     * @param availProcs Available processors.
     */
    void setTotalCpus(int pAvailProcs) {
        availProcs = pAvailProcs;
    }

    /**
     * Sets current CPU load.
     *
     * @param load Current CPU load.
     */
    void setCurrentCpuLoad(double pLoad) {
        load = pLoad;
    }

    /**
     * Sets CPU load average over the metrics history.
     *
     * @param avgLoad CPU load average.
     */
    void setAverageCpuLoad(double pAvgLoad) {
        avgLoad = pAvgLoad;
    }

    /**
     * Sets heap initial memory.
     *
     * @param heapInit Heap initial memory.
     */
    void setHeapMemoryInitialized(int64_t pHeapInit) {
        heapInit = pHeapInit;
    }

    /**
     * Sets used heap memory.
     *
     * @param heapUsed Used heap memory.
     */
    void setHeapMemoryUsed(int64_t pHeapUsed) {
        heapUsed = pHeapUsed;
    }

    /**
     * Sets committed heap memory.
     *
     * @param heapCommitted Committed heap memory.
     */
    void setHeapMemoryCommitted(int64_t pHeapCommitted) {
        heapCommitted = pHeapCommitted;
    }

    /**
     * Sets maximum possible heap memory.
     *
     * @param heapMax Maximum possible heap memory.
     */
    void setHeapMemoryMaximum(int64_t pHeapMax) {
        heapMax = pHeapMax;
    }

    /**
     * Sets initial non-heap memory.
     *
     * @param nonHeapInit Initial non-heap memory.
     */
    void setNonHeapMemoryInitialized(int64_t pNonHeapInit) {
        nonHeapInit = pNonHeapInit;
    }

    /**
     * Sets used non-heap memory.
     *
     * @param nonHeapUsed Used non-heap memory.
     */
    void setNonHeapMemoryUsed(int64_t pNonHeapUsed) {
        nonHeapUsed = pNonHeapUsed;
    }

    /**
     * Sets committed non-heap memory.
     *
     * @param nonHeapCommitted Committed non-heap memory.
     */
    void setNonHeapMemoryCommitted(int64_t pNonHeapCommitted) {
        nonHeapCommitted = pNonHeapCommitted;
    }

    /**
     * Sets maximum possible non-heap memory.
     *
     * @param nonHeapMax Maximum possible non-heap memory.
     */
    void setNonHeapMemoryMaximum(int64_t pNonHeapMax) {
        nonHeapMax = pNonHeapMax;
    }

    /**
     * Sets VM up time.
     *
     * @param upTime VN up time.
     */
    void setUpTime(int64_t pUpTime) {
        upTime = pUpTime;
    }

    /**
     * Sets VM start time.
     *
     * @param startTime VM start time.
     */
    void setStartTime(int64_t pStartTime) {
        startTime = pStartTime;
    }

    /**
     * Sets node start time.
     *
     * @param nodeStartTime node start time.
     */
    void setNodeStartTime(int64_t pNodeStartTime) {
        nodeStartTime = pNodeStartTime;
    }

    /**
     * Sets thread count.
     *
     * @param threadCnt Thread count.
     */
    void setCurrentThreadCount(int pThreadCnt) {
        threadCnt = pThreadCnt;
    }

    /**
     * Sets peak thread count.
     *
     * @param peakThreadCnt Peak thread count.
     */
    void setMaximumThreadCount(int pPeakThreadCnt) {
        peakThreadCnt = pPeakThreadCnt;
    }

    /**
     * Sets started thread count.
     *
     * @param startedThreadCnt Started thread count.
     */
    void setTotalStartedThreadCount(int64_t pStartedThreadCnt) {
        startedThreadCnt = pStartedThreadCnt;
    }

    /**
     * Sets daemon thread count.
     *
     * @param daemonThreadCnt Daemon thread count.
     */
    void setCurrentDaemonThreadCount(int pDaemonThreadCnt) {
        daemonThreadCnt = pDaemonThreadCnt;
    }

    /**
     * Sets the number of unallocated bytes in the partition.
     *
     * @param fileSysFreeSpace The number of unallocated bytes in the partition.
     */
    void setFileSystemFreeSpace(int64_t pFileSysFreeSpace) {
        fileSysFreeSpace = pFileSysFreeSpace;
    }

    /**
     * Sets size of the partition.
     *
     * @param fileSysTotalSpace Size of the partition.
     */
    void setFileSystemTotalSpace(int64_t pFileSysTotalSpace) {
        fileSysTotalSpace = pFileSysTotalSpace;
    }

    /**
     * Sets the number of bytes available to this virtual machine on the partition.
     *
     * @param fileSysUsableSpace The number of bytes available to
     *      this virtual machine on the partition.
     */
    void setFileSystemUsableSpace(int64_t pFileSysUsableSpace) {
        fileSysUsableSpace = pFileSysUsableSpace;
    }

    /**
     * @param lastDataVer Last data version.
     */
    void setLastDataVersion(int64_t pLastDataVer) {
        lastDataVer = pLastDataVer;
    }
};

#endif

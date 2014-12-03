/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.cluster;

import org.gridgain.grid.*;

import java.io.*;

/**
 * This interface defines cumulative metrics for the projection. Projection metrics are
 * defined as combined total, min, max, and average measurements from participating nodes'
 * metrics. Projection metrics are obtained by calling {@link GridProjection#metrics()}
 * method.
 * <p>
 * Note that these metrics already represent the current snapshot and can change from call
 * to call. If projection is dynamic the metrics snapshot will also change with changes
 * in participating nodes.
 * @see GridNodeMetrics
 * @see GridProjection#metrics()
 */
public interface ClusterMetrics extends Serializable {
    /**
     * Gets minimum number of active jobs that concurrently run on nodes in the projection.
     *
     * @return Minimum number of active jobs.
     */
    public int getMinimumActiveJobs();

    /**
     * Gets maximum number of active jobs that concurrently run on nodes in the projection.
     *
     * @return Maximum number of active jobs.
     */
    public int getMaximumActiveJobs();

    /**
     * Gets average number of active jobs that concurrently run nodes in the projection.
     *
     * @return Average number of active jobs.
     */
    public float getAverageActiveJobs();

    /**
     * Gets minimum number of cancelled jobs that concurrently run on nodes in the projection.
     *
     * @return Minimum number of cancelled jobs.
     */
    public int getMinimumCancelledJobs();

    /**
     * Gets maximum number of cancelled jobs that concurrently run on nodes in the projection.
     *
     * @return Maximum number of cancelled jobs.
     */
    public int getMaximumCancelledJobs();

    /**
     * Gets average number of cancelled jobs that concurrently run nodes in the projection.
     *
     * @return Average number of cancelled jobs.
     */
    public float getAverageCancelledJobs();

    /**
     * Gets minimum number of rejected jobs that concurrently run on nodes in the projection.
     *
     * @return Minimum number of rejected jobs.
     */
    public int getMinimumRejectedJobs();

    /**
     * Gets maximum number of rejected jobs that concurrently run on nodes in the projection.
     *
     * @return Maximum number of rejected jobs.
     */
    public int getMaximumRejectedJobs();

    /**
     * Gets average number of rejected jobs that concurrently run nodes in the projection.
     *
     * @return Average number of rejected jobs.
     */
    public float getAverageRejectedJobs();

    /**
     * Gets minimum number of waiting jobs that concurrently run on nodes in the projection.
     *
     * @return Minimum number of waiting jobs.
     */
    public int getMinimumWaitingJobs();

    /**
     * Gets maximum number of waiting jobs that concurrently run on nodes in the projection.
     *
     * @return Maximum number of waiting jobs.
     */
    public int getMaximumWaitingJobs();

    /**
     * Gets average number of waiting jobs that concurrently run nodes in the projection.
     *
     * @return Average number of waiting jobs.
     */
    public float getAverageWaitingJobs();

    /**
     * Gets minimum job execution time for nodes in the projection.
     *
     * @return Minimum job execution time.
     */
    public long getMinimumJobExecuteTime();

    /**
     * Gets maximum job execution time for nodes in the projection.
     *
     * @return Maximum job execution time.
     */
    public long getMaximumJobExecuteTime();

    /**
     * Gets average job execution time for nodes in the projection.
     *
     * @return Average job execution time.
     */
    public double getAverageJobExecuteTime();

    /**
     * Gets minimum job wait time for nodes in the projection.
     *
     * @return Minimum job wait time.
     */
    public long getMinimumJobWaitTime();

    /**
     * Gets maximum job wait time for nodes in the projection.
     *
     * @return Maximum job wait time.
     */
    public long getMaximumJobWaitTime();

    /**
     * Gets average job wait time for nodes in the projection.
     *
     * @return Average job wait time.
     */
    public double getAverageJobWaitTime();

    /**
     * Gets minimum number of daemon threads for nodes in the projection.
     *
     * @return Minimum daemon thread count.
     */
    public int getMinimumDaemonThreadCount();

    /**
     * Gets maximum number of daemon threads for nodes in the projection.
     *
     * @return Maximum daemon thread count.
     */
    public int getMaximumDaemonThreadCount();

    /**
     * Gets average number of daemon threads for nodes in the projection.
     *
     * @return Average daemon thread count.
     */
    public float getAverageDaemonThreadCount();

    /**
     * Gets minimum number of threads for nodes in the projection.
     *
     * @return Minimum thread count.
     */
    public int getMinimumThreadCount();

    /**
     * Gets maximum number of threads for nodes in the projection.
     *
     * @return Maximum thread count.
     */
    public int getMaximumThreadCount();

    /**
     * Gets average number of threads for nodes in the projection.
     *
     * @return Average thread count.
     */
    public float getAverageThreadCount();

    /**
     * Gets minimum idle time for nodes in the projection.
     *
     * @return Minimum idle time.
     */
    public long getMinimumIdleTime();

    /**
     * Gets maximum idle time for nodes in the projection.
     *
     * @return Maximum idle time.
     */
    public long getMaximumIdleTime();

    /**
     * Gets average idle time for nodes in the projection.
     *
     * @return Average idle time.
     */
    public double getAverageIdleTime();

    /**
     * Gets percentage of time nodes in this projection are idling vs. executing jobs.
     *
     * @return Percentage of time nodes in this projection are idle (value is less than
     *      or equal to {@code 1} and greater than or equal to {@code 0}).
     */
    public float getIdleTimePercentage();

    /**
     * Gets minimum busy time for nodes in the projection.
     *
     * @return Minimum busy time.
     */
    public float getMinimumBusyTimePercentage();

    /**
     * Gets maximum busy time for nodes in the projection.
     *
     * @return Maximum busy time.
     */
    public float getMaximumBusyTimePercentage();

    /**
     * Gets average busy time for nodes in the projection.
     *
     * @return Average busy time.
     */
    public float getAverageBusyTimePercentage();

    /**
     * Gets minimum CPU load for nodes in the projection.
     *
     * @return Minimum CPU load.
     */
    public double getMinimumCpuLoad();

    /**
     * Gets maximum CPU load for nodes in the projection.
     *
     * @return Maximum CPU load.
     */
    public double getMaximumCpuLoad();

    /**
     * Gets average CPU load for nodes in the projection.
     *
     * @return Average CPU load in <code>[0, 1]</code> range.
     */
    public double getAverageCpuLoad();

    /**
     * Gets minimum amount of committed heap memory for nodes in the projection.
     *
     * @return Minimum amount of committed heap memory.
     */
    public long getMinimumHeapMemoryCommitted();

    /**
     * Gets maximum amount of committed heap memory for nodes in the projection.
     *
     * @return Maximum amount of committed heap memory.
     */
    public long getMaximumHeapMemoryCommitted();

    /**
     * Gets average amount of committed heap memory for nodes in the projection.
     *
     * @return Average amount of committed heap memory.
     */
    public double getAverageHeapMemoryCommitted();

    /**
     * Gets minimum amount of used heap memory for nodes in the projection.
     *
     * @return Minimum amount of used heap memory.
     */
    public long getMinimumHeapMemoryUsed();

    /**
     * Gets maximum amount of used heap memory for nodes in the projection.
     *
     * @return Maximum amount of used heap memory.
     */
    public long getMaximumHeapMemoryUsed();

    /**
     * Gets average amount of used heap memory for nodes in the projection.
     *
     * @return Average amount of used heap memory.
     */
    public double getAverageHeapMemoryUsed();

    /**
     * Gets minimum amount of maximum heap memory for nodes in the projection.
     *
     * @return Minimum amount of maximum heap memory.
     */
    public long getMinimumHeapMemoryMaximum();

    /**
     * Gets maximum amount of maximum heap memory for nodes in the projection.
     *
     * @return Maximum amount of used heap memory.
     */
    public long getMaximumHeapMemoryMaximum();

    /**
     * Gets minimum amount of maximum heap memory for nodes in the projection.
     *
     * @return Minimum amount of maximum heap memory.
     */
    public double getAverageHeapMemoryMaximum();

    /**
     * Gets minimum amount of initialized heap memory for nodes in the projection.
     *
     * @return Minimum amount of initialized heap memory.
     */
    public long getMinimumHeapMemoryInitialized();

    /**
     * Gets maximum amount of initialized heap memory for nodes in the projection.
     *
     * @return Maximum amount of initialized heap memory.
     */
    public long getMaximumHeapMemoryInitialized();

    /**
     * Gets average amount of initialized heap memory for nodes in the projection.
     *
     * @return Average amount of initialized heap memory.
     */
    public double getAverageHeapMemoryInitialized();

    /**
     * Gets minimum amount of committed non-heap memory for nodes in the projection.
     *
     * @return Minimum amount of committed non-heap memory.
     */
    public long getMinimumNonHeapMemoryCommitted();

    /**
     * Gets maximum amount of committed non-heap memory for nodes in the projection.
     *
     * @return Maximum amount of committed non-heap memory.
     */
    public long getMaximumNonHeapMemoryCommitted();

    /**
     * Gets average amount of committed non-heap memory for nodes in the projection.
     *
     * @return Average amount of committed non-heap memory.
     */
    public double getAverageNonHeapMemoryCommitted();

    /**
     * Gets minimum amount of used non-heap memory for nodes in the projection.
     *
     * @return Minimum amount of used non-heap memory.
     */
    public long getMinimumNonHeapMemoryUsed();

    /**
     * Gets maximum amount of used non-heap memory for nodes in the projection.
     *
     * @return Maximum amount of used non-heap memory.
     */
    public long getMaximumNonHeapMemoryUsed();

    /**
     * Gets average amount of used non-heap memory for nodes in the projection.
     *
     * @return Average amount of used non-heap memory.
     */
    public double getAverageNonHeapMemoryUsed();

    /**
     * Gets minimum amount of maximum non-heap memory for nodes in the projection.
     *
     * @return Minimum amount of maximum non-heap memory.
     */
    public long getMinimumNonHeapMemoryMaximum();

    /**
     * Gets maximum amount of maximum non-heap memory for nodes in the projection.
     *
     * @return Maximum amount of maximum non-heap memory.
     */
    public long getMaximumNonHeapMemoryMaximum();

    /**
     * Gets average amount of maximum non-heap memory for nodes in the projection.
     *
     * @return Average amount of maximum non-heap memory.
     */
    public double getAverageNonHeapMemoryMaximum();

    /**
     * Gets minimum amount of initialized non-heap memory for nodes in the projection.
     *
     * @return Minimum amount of initialized non-heap memory.
     */
    public long getMinimumNonHeapMemoryInitialized();

    /**
     * Gets maximum amount of initialized non-heap memory for nodes in the projection.
     *
     * @return Maximum amount of initialized non-heap memory.
     */
    public long getMaximumNonHeapMemoryInitialized();

    /**
     * Gets average amount of initialized non-heap memory for nodes in the projection.
     *
     * @return Average amount of initialized non-heap memory.
     */
    public double getAverageNonHeapMemoryInitialized();

    /**
     * Gets start time of the youngest node in the projection.
     *
     * @return Youngest node start time.
     */
    public long getYoungestNodeStartTime();

    /**
     * Gets start time of the oldest node in the projection.
     *
     * @return Oldest node start time.
     */
    public long getOldestNodeStartTime();

    /**
     * Gets minimum up time for nodes in the projection.
     *
     * @return Minimum up time.
     */
    public long getMinimumUpTime();

    /**
     * Gets maximum up time for nodes in the projection.
     *
     * @return Maximum up time.
     */
    public long getMaximumUpTime();

    /**
     * Gets average up time for nodes in the projection.
     *
     * @return Average up time.
     */
    public double getAverageUpTime();

    /**
     * Gets minimum number of available processors for nodes in the projection.
     *
     * @return Minimum number of available processors.
     */
    public int getMinimumCpusPerNode();

    /**
     * Gets maximum number of available processors for nodes in the projection.
     *
     * @return Maximum number of available processors.
     */
    public int getMaximumCpusPerNode();

    /**
     * Gets average number of available processors for nodes in the projection.
     *
     * @return Average number of available processors.
     */
    public float getAverageCpusPerNode();

    /**
     * Gets minimum number of nodes per host.
     *
     * @return Minimum number of nodes per host.
     */
    public int getMinimumNodesPerHost();

    /**
     * Gets maximum number of nodes per host.
     *
     * @return Maximum number of nodes per host.
     */
    public int getMaximumNodesPerHost();

    /**
     * Gets average number of nodes per host.
     *
     * @return Average number of nodes per host.
     */
    public float getAverageNodesPerHost();

    /**
     * Gets total number of available processors.
     *
     * @return Total number of available processors.
     */
    public int getTotalCpus();

    /**
     * Gets total number of hosts.
     *
     * @return Total number of hosts.
     */
    public int getTotalHosts();

    /**
     * Gets total number of nodes.
     *
     * @return Total number of nodes.
     */
    public int getTotalNodes();
}

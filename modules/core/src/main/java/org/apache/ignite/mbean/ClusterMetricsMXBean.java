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

package org.apache.ignite.mbean;

import org.apache.ignite.cluster.*;

/**
 * MBean for local node metrics.
 */
@IgniteMBeanDescription("MBean that provides access to all local node metrics.")
public interface ClusterMetricsMXBean extends ClusterMetrics {
    /** {@inheritDoc} */
    @IgniteMBeanDescription("Last update time of this node metrics.")
    public long getLastUpdateTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum number of jobs that ever ran concurrently on this node.")
    public int getMaximumActiveJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of currently active jobs concurrently executing on the node.")
    public int getCurrentActiveJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average number of active jobs concurrently executing on the node.")
    public float getAverageActiveJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum number of waiting jobs this node had.")
    public int getMaximumWaitingJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of queued jobs currently waiting to be executed.")
    public int getCurrentWaitingJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average number of waiting jobs this node had queued.")
    public float getAverageWaitingJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum number of jobs rejected at once during a single collision resolution operation.")
    public int getMaximumRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of jobs rejected after more recent collision resolution operation.")
    public int getCurrentRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average number of jobs this node rejects during collision resolution operations.")
    public float getAverageRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription(
        "Total number of jobs this node rejects during collision resolution operations since node startup.")
    public int getTotalRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum number of cancelled jobs this node ever had running concurrently.")
    public int getMaximumCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Number of cancelled jobs that are still running.")
    public int getCurrentCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average number of cancelled jobs this node ever had running concurrently.")
    public float getAverageCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Total number of cancelled jobs since node startup.")
    public int getTotalCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Total number of jobs handled by the node.")
    public int getTotalExecutedJobs();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum time a job ever spent waiting in a queue to be executed.")
    public long getMaximumJobWaitTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Current wait time of oldest job.")
    public long getCurrentJobWaitTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time jobs spend waiting in the queue to be executed.")
    public double getAverageJobWaitTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Time it took to execute the longest job on the node.")
    public long getMaximumJobExecuteTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Longest time a current job has been executing for.")
    public long getCurrentJobExecuteTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time a job takes to execute on the node.")
    public double getAverageJobExecuteTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Total number of tasks handled by the node.")
    public int getTotalExecutedTasks();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Total time this node spent executing jobs.")
    public long getTotalBusyTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Total time this node spent idling (not executing any jobs).")
    public long getTotalIdleTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Time this node spend idling since executing last job.")
    public long getCurrentIdleTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Percentage of time this node is busy executing jobs vs. idling.")
    public float getBusyTimePercentage();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Percentage of time this node is idling vs. executing jobs.")
    public float getIdleTimePercentage();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The number of CPUs available to the Java Virtual Machine.")
    public int getTotalCpus();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The system load average; or a negative value if not available.")
    public double getCurrentCpuLoad();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average of CPU load values over all metrics kept in the history.")
    public double getAverageCpuLoad();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Average time spent in CG since the last update.")
    public double getCurrentGcCpuLoad();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The initial size of memory in bytes; -1 if undefined.")
    public long getHeapMemoryInitialized();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Current heap size that is used for object allocation.")
    public long getHeapMemoryUsed();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The amount of committed memory in bytes.")
    public long getHeapMemoryCommitted();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The maximum amount of memory in bytes; -1 if undefined.")
    public long getHeapMemoryMaximum();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The initial size of memory in bytes; -1 if undefined.")
    public long getNonHeapMemoryInitialized();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Current non-heap memory size that is used by Java VM.")
    public long getNonHeapMemoryUsed();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Amount of non-heap memory in bytes that is committed for the JVM to use.")
    public long getNonHeapMemoryCommitted();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Maximum amount of non-heap memory in bytes that can " +
            "be used for memory management. -1 if undefined.")
    public long getNonHeapMemoryMaximum();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Uptime of the JVM in milliseconds.")
    public long getUpTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Start time of the JVM in milliseconds.")
    public long getStartTime();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Current number of live threads.")
    public int getCurrentThreadCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The peak live thread count.")
    public int getMaximumThreadCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("The total number of threads started.")
    public long getTotalStartedThreadCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Current number of live daemon threads.")
    public int getCurrentDaemonThreadCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Last data version.")
    public long getLastDataVersion();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Sent messages count.")
    public int getSentMessagesCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Sent bytes count.")
    public long getSentBytesCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Received messages count.")
    public int getReceivedMessagesCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Received bytes count.")
    public long getReceivedBytesCount();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Outbound messages queue size.")
    public int getOutboundMessagesQueueSize();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Start time of the grid node in milliseconds.")
    @Override long getNodeStartTime();
}

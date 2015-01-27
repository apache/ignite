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

package org.apache.ignite.mxbean;

import org.apache.ignite.cluster.*;

/**
 * MBean for local node metrics.
 */
@IgniteMXBeanDescription("MBean that provides access to all local node metrics.")
public interface ClusterLocalNodeMetricsMXBean extends ClusterMetrics {
    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Last update time of this node metrics.")
    public long getLastUpdateTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum number of jobs that ever ran concurrently on this node.")
    public int getMaximumActiveJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of currently active jobs concurrently executing on the node.")
    public int getCurrentActiveJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average number of active jobs concurrently executing on the node.")
    public float getAverageActiveJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum number of waiting jobs this node had.")
    public int getMaximumWaitingJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of queued jobs currently waiting to be executed.")
    public int getCurrentWaitingJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average number of waiting jobs this node had queued.")
    public float getAverageWaitingJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum number of jobs rejected at once during a single collision resolution operation.")
    public int getMaximumRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of jobs rejected after more recent collision resolution operation.")
    public int getCurrentRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average number of jobs this node rejects during collision resolution operations.")
    public float getAverageRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription(
        "Total number of jobs this node rejects during collision resolution operations since node startup.")
    public int getTotalRejectedJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum number of cancelled jobs this node ever had running concurrently.")
    public int getMaximumCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Number of cancelled jobs that are still running.")
    public int getCurrentCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average number of cancelled jobs this node ever had running concurrently.")
    public float getAverageCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Total number of cancelled jobs since node startup.")
    public int getTotalCancelledJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Total number of jobs handled by the node.")
    public int getTotalExecutedJobs();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum time a job ever spent waiting in a queue to be executed.")
    public long getMaximumJobWaitTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Current wait time of oldest job.")
    public long getCurrentJobWaitTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time jobs spend waiting in the queue to be executed.")
    public double getAverageJobWaitTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Time it took to execute the longest job on the node.")
    public long getMaximumJobExecuteTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Longest time a current job has been executing for.")
    public long getCurrentJobExecuteTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time a job takes to execute on the node.")
    public double getAverageJobExecuteTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Total number of tasks handled by the node.")
    public int getTotalExecutedTasks();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Total time this node spent executing jobs.")
    public long getTotalBusyTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Total time this node spent idling (not executing any jobs).")
    public long getTotalIdleTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Time this node spend idling since executing last job.")
    public long getCurrentIdleTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Percentage of time this node is busy executing jobs vs. idling.")
    public float getBusyTimePercentage();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Percentage of time this node is idling vs. executing jobs.")
    public float getIdleTimePercentage();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The number of CPUs available to the Java Virtual Machine.")
    public int getTotalCpus();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The system load average; or a negative value if not available.")
    public double getCurrentCpuLoad();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average of CPU load values over all metrics kept in the history.")
    public double getAverageCpuLoad();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Average time spent in CG since the last update.")
    public double getCurrentGcCpuLoad();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The initial size of memory in bytes; -1 if undefined.")
    public long getHeapMemoryInitialized();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Current heap size that is used for object allocation.")
    public long getHeapMemoryUsed();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The amount of committed memory in bytes.")
    public long getHeapMemoryCommitted();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The maximum amount of memory in bytes; -1 if undefined.")
    public long getHeapMemoryMaximum();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The initial size of memory in bytes; -1 if undefined.")
    public long getNonHeapMemoryInitialized();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Current non-heap memory size that is used by Java VM.")
    public long getNonHeapMemoryUsed();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Amount of non-heap memory in bytes that is committed for the JVM to use.")
    public long getNonHeapMemoryCommitted();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Maximum amount of non-heap memory in bytes that can " +
        "be used for memory management. -1 if undefined.")
    public long getNonHeapMemoryMaximum();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Uptime of the JVM in milliseconds.")
    public long getUpTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Start time of the JVM in milliseconds.")
    public long getStartTime();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Current number of live threads.")
    public int getCurrentThreadCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The peak live thread count.")
    public int getMaximumThreadCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("The total number of threads started.")
    public long getTotalStartedThreadCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Current number of live daemon threads.")
    public int getCurrentDaemonThreadCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Last data version.")
    public long getLastDataVersion();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Sent messages count.")
    public int getSentMessagesCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Sent bytes count.")
    public long getSentBytesCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Received messages count.")
    public int getReceivedMessagesCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Received bytes count.")
    public long getReceivedBytesCount();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Outbound messages queue size.")
    public int getOutboundMessagesQueueSize();

    /** {@inheritDoc} */
    @IgniteMXBeanDescription("Start time of the grid node in milliseconds.")
    public long getNodeStartTime();
}

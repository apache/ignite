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

import org.apache.ignite.cluster.ClusterMetrics;

/**
 * MBean for local node metrics.
 */
@MXBeanDescription("MBean that provides access to all local node metrics.")
public interface ClusterLocalNodeMetricsMXBean extends ClusterMetrics {
    /** {@inheritDoc} */
    @MXBeanDescription("Last update time of this node metrics.")
    public long getLastUpdateTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum number of jobs that ever ran concurrently on this node.")
    public int getMaximumActiveJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of currently active jobs concurrently executing on the node.")
    public int getCurrentActiveJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Average number of active jobs concurrently executing on the node.")
    public float getAverageActiveJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum number of waiting jobs this node had.")
    public int getMaximumWaitingJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of queued jobs currently waiting to be executed.")
    public int getCurrentWaitingJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Average number of waiting jobs this node had queued.")
    public float getAverageWaitingJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum number of jobs rejected at once during a single collision resolution operation.")
    public int getMaximumRejectedJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of jobs rejected after more recent collision resolution operation.")
    public int getCurrentRejectedJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Average number of jobs this node rejects during collision resolution operations.")
    public float getAverageRejectedJobs();

    /** {@inheritDoc} */
    @MXBeanDescription(
        "Total number of jobs this node rejects during collision resolution operations since node startup.")
    public int getTotalRejectedJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum number of cancelled jobs this node ever had running concurrently.")
    public int getMaximumCancelledJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of cancelled jobs that are still running.")
    public int getCurrentCancelledJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Average number of cancelled jobs this node ever had running concurrently.")
    public float getAverageCancelledJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of cancelled jobs since node startup.")
    public int getTotalCancelledJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of jobs handled by the node.")
    public int getTotalExecutedJobs();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum time a job ever spent waiting in a queue to be executed.")
    public long getMaximumJobWaitTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Current wait time of oldest job.")
    public long getCurrentJobWaitTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time jobs spend waiting in the queue to be executed.")
    public double getAverageJobWaitTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Time it took to execute the longest job on the node.")
    public long getMaximumJobExecuteTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Longest time a current job has been executing for.")
    public long getCurrentJobExecuteTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time a job takes to execute on the node.")
    public double getAverageJobExecuteTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of tasks handled by the node.")
    public int getTotalExecutedTasks();

    /** {@inheritDoc} */
    @MXBeanDescription("Total time this node spent executing jobs.")
    public long getTotalBusyTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Total time this node spent idling (not executing any jobs).")
    public long getTotalIdleTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Time this node spend idling since executing last job.")
    public long getCurrentIdleTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of time this node is busy executing jobs vs. idling.")
    public float getBusyTimePercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of time this node is idling vs. executing jobs.")
    public float getIdleTimePercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("The number of CPUs available to the Java Virtual Machine.")
    public int getTotalCpus();

    /** {@inheritDoc} */
    @MXBeanDescription("The system load average; or a negative value if not available.")
    public double getCurrentCpuLoad();

    /** {@inheritDoc} */
    @MXBeanDescription("Average of CPU load values over all metrics kept in the history.")
    public double getAverageCpuLoad();

    /** {@inheritDoc} */
    @MXBeanDescription("Average time spent in CG since the last update.")
    public double getCurrentGcCpuLoad();

    /** {@inheritDoc} */
    @MXBeanDescription("The initial size of memory in bytes; -1 if undefined.")
    public long getHeapMemoryInitialized();

    /** {@inheritDoc} */
    @MXBeanDescription("Current heap size that is used for object allocation.")
    public long getHeapMemoryUsed();

    /** {@inheritDoc} */
    @MXBeanDescription("The amount of committed memory in bytes.")
    public long getHeapMemoryCommitted();

    /** {@inheritDoc} */
    @MXBeanDescription("The maximum amount of memory in bytes; -1 if undefined.")
    public long getHeapMemoryMaximum();

    /** {@inheritDoc} */
    @MXBeanDescription("The total amount of memory in bytes; -1 if undefined.")
    public long getHeapMemoryTotal();

    /** {@inheritDoc} */
    @MXBeanDescription("The initial size of memory in bytes; -1 if undefined.")
    public long getNonHeapMemoryInitialized();

    /** {@inheritDoc} */
    @MXBeanDescription("Current non-heap memory size that is used by Java VM.")
    public long getNonHeapMemoryUsed();

    /** {@inheritDoc} */
    @MXBeanDescription("Amount of non-heap memory in bytes that is committed for the JVM to use.")
    public long getNonHeapMemoryCommitted();

    /** {@inheritDoc} */
    @MXBeanDescription("Maximum amount of non-heap memory in bytes that can " +
        "be used for memory management. -1 if undefined.")
    public long getNonHeapMemoryMaximum();

    /** {@inheritDoc} */
    @MXBeanDescription("Total amount of non-heap memory in bytes that can " +
        "be used for memory management. -1 if undefined.")
    public long getNonHeapMemoryTotal();

    /** {@inheritDoc} */
    @MXBeanDescription("Uptime of the JVM in milliseconds.")
    public long getUpTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Start time of the JVM in milliseconds.")
    public long getStartTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Current number of live threads.")
    public int getCurrentThreadCount();

    /** {@inheritDoc} */
    @MXBeanDescription("The peak live thread count.")
    public int getMaximumThreadCount();

    /** {@inheritDoc} */
    @MXBeanDescription("The total number of threads started.")
    public long getTotalStartedThreadCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Current number of live daemon threads.")
    public int getCurrentDaemonThreadCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Last data version.")
    public long getLastDataVersion();

    /** {@inheritDoc} */
    @MXBeanDescription("Sent messages count.")
    public int getSentMessagesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Sent bytes count.")
    public long getSentBytesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Received messages count.")
    public int getReceivedMessagesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Received bytes count.")
    public long getReceivedBytesCount();

    /** {@inheritDoc} */
    @MXBeanDescription("Outbound messages queue size.")
    public int getOutboundMessagesQueueSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Start time of the grid node in milliseconds.")
    public long getNodeStartTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of nodes.")
    public int getTotalNodes();
}
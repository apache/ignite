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

namespace Apache.Ignite.Core.Cluster
{
    using System;

    /// <summary>
    /// Represents runtime information of a cluster. Apart from obvious
    /// statistical value, this information is used for implementation of
    /// load balancing, failover, and collision SPIs. For example, collision SPI
    /// in combination with fail-over SPI could check if other nodes don't have
    /// any active or waiting jobs and fail-over some jobs to those nodes.
    /// <para />
    /// Node metrics for any node can be accessed via <see cref="IClusterNode.Metrics()"/> 
    /// method. Keep in mind that there will be a certain network delay (usually
    /// equal to heartbeat delay) for the accuracy of node metrics. However, when accessing
    /// metrics on local node the metrics are always accurate and up to date.
    /// </summary>
    public interface IClusterMetrics
    {
        /// <summary>
        /// Last update time of this node metrics.
        /// </summary>
        DateTime LastUpdateTime
        {
            get;
        }

        /// <summary>
        /// Maximum number of jobs that ever ran concurrently on this node.
        /// </summary>
        int MaximumActiveJobs
        {
            get;
        }

        /// <summary>
        /// Number of currently active jobs concurrently executing on the node.
        /// </summary>
        int CurrentActiveJobs
        {
            get;
        }

        /// <summary>
        /// Average number of active jobs. 
        /// </summary>
        float AverageActiveJobs
        {
            get;
        }

        /// <summary>
        /// Maximum number of waiting jobs.
        /// </summary>
        int MaximumWaitingJobs
        {
            get;
        }
        
        /// <summary>
        /// Number of queued jobs currently waiting to be executed.
        /// </summary>
        int CurrentWaitingJobs
        {
            get;
        }

        /// <summary>
        /// Average number of waiting jobs.
        /// </summary>
        float AverageWaitingJobs
        {
            get;
        }

        /// <summary>
        /// Maximum number of jobs rejected at once.
        /// </summary>
        int MaximumRejectedJobs
        {
            get;
        }

        /// <summary>
        /// Number of jobs rejected after more recent collision resolution operation.
        /// </summary>
        int CurrentRejectedJobs
        {
            get;
        }

        /// <summary>
        /// Average number of jobs this node rejects during collision resolution operations.
        /// </summary>
        float AverageRejectedJobs
        {
            get;
        }

        /// <summary>
        /// Total number of jobs this node rejects during collision resolution operations since node startup.
        /// </summary>
        int TotalRejectedJobs
        {
            get;
        }

        /// <summary>
        /// Maximum number of cancelled jobs ever had running concurrently.
        /// </summary>
        int MaximumCancelledJobs
        {
            get;
        }

        /// <summary>
        /// Number of cancelled jobs that are still running.
        /// </summary>
        int CurrentCancelledJobs
        {
            get;
        }

        /// <summary>
        /// Average number of cancelled jobs.
        /// </summary>
        float AverageCancelledJobs
        {
            get;
        }

        /// <summary>
        /// Total number of cancelled jobs since node startup.
        /// </summary>
        int TotalCancelledJobs
        {
            get;
        }

        /// <summary>
        /// Total number of jobs handled by the node since node startup.
        /// </summary>
        int TotalExecutedJobs
        {
            get;
        }

        /// <summary>
        /// Maximum time a job ever spent waiting in a queue to be executed.
        /// </summary>
        long MaximumJobWaitTime
        {
            get;
        }

        /// <summary>
        /// Current time an oldest jobs has spent waiting to be executed.
        /// </summary>
        long CurrentJobWaitTime
        {
            get;
        }

        /// <summary>
        /// Average time jobs spend waiting in the queue to be executed.
        /// </summary>
        double AverageJobWaitTime
        {
            get;
        }

        /// <summary>
        /// Time it took to execute the longest job on the node.
        /// </summary>
        long MaximumJobExecuteTime
        {
            get;
        }

        /// <summary>
        /// Longest time a current job has been executing for.
        /// </summary>
        long CurrentJobExecuteTime
        {
            get;
        }

        /// <summary>
        /// Average job execution time.
        /// </summary>
        double AverageJobExecuteTime
        {
            get;
        }

        /// <summary>
        /// Total number of jobs handled by the node. 
        /// </summary>
        int TotalExecutedTasks
        {
            get;
        }

        /// <summary>
        /// Total time this node spent executing jobs.
        /// </summary>
        long TotalBusyTime
        {
            get;
        }

        /// <summary>
        /// Total time this node spent idling.
        /// </summary>
        long TotalIdleTime
        {
            get;
        }

        /// <summary>
        /// Time this node spend idling since executing last job.
        /// </summary>
        long CurrentIdleTime
        {
            get;
        }

        /// <summary>
        /// Percentage of time this node is busy.
        /// </summary>
        float BusyTimePercentage
        {
            get;
        }

        /// <summary>
        /// Percentage of time this node is idle
        /// </summary>
        float IdleTimePercentage
        {
            get;
        }

        /// <summary>
        /// Returns the number of CPUs available to the Java Virtual Machine.
        /// </summary>
        int TotalCpus
        {
            get;
        }

        /// <summary>
        /// Returns the CPU usage usage in [0, 1] range.
        /// </summary>
        double CurrentCpuLoad
        {
            get;
        }

        /// <summary>
        /// Average of CPU load values in [0, 1] range over all metrics kept in the history.
        /// </summary>
        double AverageCpuLoad
        {
            get;
        }

        /// <summary>
        /// Average time spent in CG since the last update.
        /// </summary>
        double CurrentGcCpuLoad
        {
            get;
        }
        
        /// <summary>
        /// Amount of heap memory in bytes that the JVM
        /// initially requests from the operating system for memory management.
        /// This method returns <code>-1</code> if the initial memory size is undefined.
        /// <para />
        /// This value represents a setting of the heap memory for Java VM and is
        /// not a sum of all initial heap values for all memory pools.
        /// </summary>
        long HeapMemoryInitialized
        {
            get;
        }

        /// <summary>
        /// Current heap size that is used for object allocation.
        /// The heap consists of one or more memory pools. This value is
        /// the sum of used heap memory values of all heap memory pools.
        /// <para />
        /// The amount of used memory in the returned is the amount of memory
        /// occupied by both live objects and garbage objects that have not
        /// been collected, if any.
        /// </summary>
        long HeapMemoryUsed
        {
            get;
        }

        /// <summary>
        /// Amount of heap memory in bytes that is committed for the JVM to use. This amount of memory is
        /// guaranteed for the JVM to use. The heap consists of one or more memory pools. This value is
        /// the sum of committed heap memory values of all heap memory pools.
        /// </summary>
        long HeapMemoryCommitted
        {
            get;
        }

        /// <summary>
        /// Mmaximum amount of heap memory in bytes that can be used for memory management.
        /// This method returns <code>-1</code> if the maximum memory size is undefined.
        /// <para />
        /// This amount of memory is not guaranteed to be available for memory management if 
        /// it is greater than the amount of committed memory. The JVM may fail to allocate
        /// memory even if the amount of used memory does not exceed this maximum size.
        /// <para />
        /// This value represents a setting of the heap memory for Java VM and is
        /// not a sum of all initial heap values for all memory pools.
        /// </summary>
        long HeapMemoryMaximum
        {
            get;
        }

        /// <summary>
        /// Total amount of heap memory in bytes. This method returns <code>-1</code>
        /// if the total memory size is undefined.
        /// <para />
        /// This amount of memory is not guaranteed to be available for memory management if it is 
        /// greater than the amount of committed memory. The JVM may fail to allocate memory even 
        /// if the amount of used memory does not exceed this maximum size.
        /// <para />
        /// This value represents a setting of the heap memory for Java VM and is
        /// not a sum of all initial heap values for all memory pools.
        /// </summary>
        long HeapMemoryTotal
        {
            get;
        }

        /// <summary>
        /// Amount of non-heap memory in bytes that the JVM initially requests from the operating 
        /// system for memory management.
        /// </summary>
        long NonHeapMemoryInitialized
        {
            get;
        }

        /// <summary>
        /// Current non-heap memory size that is used by Java VM.
        /// </summary>
        long NonHeapMemoryUsed
        {
            get;
        }

        /// <summary>
        /// Amount of non-heap memory in bytes that is committed for the JVM to use. 
        /// </summary>
        long NonHeapMemoryCommitted
        {
            get;
        }
        
        /// <summary>
        /// Maximum amount of non-heap memory in bytes that can be used for memory management.
        /// </summary>
        long NonHeapMemoryMaximum
        {
            get;
        }

        /// <summary>
        /// Total amount of non-heap memory in bytes that can be used for memory management. 
        /// </summary>
        long NonHeapMemoryTotal
        {
            get;
        }

        /// <summary>
        /// Uptime of the JVM in milliseconds.
        /// </summary>
        long UpTime
        {
            get;
        }

        /// <summary>
        /// Start time of the JVM in milliseconds.
        /// </summary>
        DateTime StartTime
        {
            get;
        }

        /// <summary>
        /// Start time of the Ignite node in milliseconds.
        /// </summary>
        DateTime NodeStartTime
        {
            get;
        }

        /// <summary>
        /// Current number of live threads.
        /// </summary>
        int CurrentThreadCount
        {
            get;
        }

        /// <summary>
        /// The peak live thread count.
        /// </summary>
        int MaximumThreadCount
        {
            get;
        }

        /// <summary>
        /// The total number of threads started.
        /// </summary>
        long TotalStartedThreadCount
        {
            get;
        }

        /// <summary>
        /// Current number of live daemon threads.
        /// </summary>
        int CurrentDaemonThreadCount
        {
            get;
        }

        /// <summary>
        /// Ignite assigns incremental versions to all cache operations. This property provides
        /// the latest data version on the node.
        /// </summary>
        long LastDataVersion
        {
            get;
        }

        /// <summary>
        /// Sent messages count 
        /// </summary>
        int SentMessagesCount
        {
            get;
        }

        /// <summary>
        /// Sent bytes count.
        /// </summary>
        long SentBytesCount
        {
            get;
        }

        /// <summary>
        /// Received messages count.
        /// </summary>
        int ReceivedMessagesCount
        {
            get;
        }

        /// <summary>
        /// Received bytes count.
        /// </summary>
        long ReceivedBytesCount
        {
            get;
        }

        /// <summary>
        /// Outbound messages queue size.
        /// </summary>
        int OutboundMessagesQueueSize
        {
            get;
        }

        /// <summary>
        /// Gets total number of nodes.
        /// </summary>
        int TotalNodes
        {
            get;
        }
    }
}

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

namespace Apache.Ignite.Core.Impl.Cluster
{
    using System;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Cluster metrics implementation.
    /// </summary>
    internal class ClusterMetricsImpl : IClusterMetrics
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterMetricsImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ClusterMetricsImpl(IPortableRawReader reader)
        {
            LastUpdateTimeRaw = reader.ReadLong();

            DateTime? lastUpdateTime0 = reader.ReadDate();

            LastUpdateTime = lastUpdateTime0 ?? default(DateTime);
            MaximumActiveJobs = reader.ReadInt();
            CurrentActiveJobs = reader.ReadInt();
            AverageActiveJobs = reader.ReadFloat();
            MaximumWaitingJobs = reader.ReadInt();

            CurrentWaitingJobs = reader.ReadInt();
            AverageWaitingJobs = reader.ReadFloat();
            MaximumRejectedJobs = reader.ReadInt();
            CurrentRejectedJobs = reader.ReadInt();
            AverageRejectedJobs = reader.ReadFloat();

            TotalRejectedJobs = reader.ReadInt();
            MaximumCancelledJobs = reader.ReadInt();
            CurrentCancelledJobs = reader.ReadInt();
            AverageCancelledJobs = reader.ReadFloat();
            TotalCancelledJobs = reader.ReadInt();

            TotalExecutedJobs = reader.ReadInt();
            MaximumJobWaitTime = reader.ReadLong();
            CurrentJobWaitTime = reader.ReadLong();
            AverageJobWaitTime = reader.ReadDouble();
            MaximumJobExecuteTime = reader.ReadLong();

            CurrentJobExecuteTime = reader.ReadLong();
            AverageJobExecuteTime = reader.ReadDouble();
            TotalExecutedTasks = reader.ReadInt();
            TotalIdleTime = reader.ReadLong();
            CurrentIdleTime = reader.ReadLong();

            TotalCpus = reader.ReadInt();
            CurrentCpuLoad = reader.ReadDouble();
            AverageCpuLoad = reader.ReadDouble();
            CurrentGcCpuLoad = reader.ReadDouble();
            HeapMemoryInitialized = reader.ReadLong();

            HeapMemoryUsed = reader.ReadLong();
            HeapMemoryCommitted = reader.ReadLong();
            HeapMemoryMaximum = reader.ReadLong();
            HeapMemoryTotal = reader.ReadLong();
            NonHeapMemoryInitialized = reader.ReadLong();

            NonHeapMemoryUsed = reader.ReadLong();
            NonHeapMemoryCommitted = reader.ReadLong();
            NonHeapMemoryMaximum = reader.ReadLong();
            NonHeapMemoryTotal = reader.ReadLong();
            UpTime = reader.ReadLong();

            DateTime? startTime0 = reader.ReadDate();

            StartTime = startTime0 ?? default(DateTime);

            DateTime? nodeStartTime0 = reader.ReadDate();

            NodeStartTime = nodeStartTime0 ?? default(DateTime);

            CurrentThreadCount = reader.ReadInt();
            MaximumThreadCount = reader.ReadInt();
            TotalStartedThreadCount = reader.ReadLong();
            CurrentDaemonThreadCount = reader.ReadInt();
            LastDataVersion = reader.ReadLong();

            SentMessagesCount = reader.ReadInt();
            SentBytesCount = reader.ReadLong();
            ReceivedMessagesCount = reader.ReadInt();
            ReceivedBytesCount = reader.ReadLong();
            OutboundMessagesQueueSize = reader.ReadInt();

            TotalNodes = reader.ReadInt();
        }

        /// <summary>
        /// Last update time in raw format.
        /// </summary>
        internal long LastUpdateTimeRaw { get; set; }

        /** <inheritDoc /> */
        public DateTime LastUpdateTime { get; private set; }

        /** <inheritDoc /> */
        public int MaximumActiveJobs { get; private set; }

        /** <inheritDoc /> */
        public int CurrentActiveJobs { get; private set; }

        /** <inheritDoc /> */
        public float AverageActiveJobs { get; private set; }

        /** <inheritDoc /> */
        public int MaximumWaitingJobs { get; private set; }

        /** <inheritDoc /> */
        public int CurrentWaitingJobs { get; private set; }

        /** <inheritDoc /> */
        public float AverageWaitingJobs { get; private set; }

        /** <inheritDoc /> */
        public int MaximumRejectedJobs { get; private set; }

        /** <inheritDoc /> */
        public int CurrentRejectedJobs { get; private set; }

        /** <inheritDoc /> */
        public float AverageRejectedJobs { get; private set; }

        /** <inheritDoc /> */
        public int TotalRejectedJobs { get; private set; }

        /** <inheritDoc /> */
        public int MaximumCancelledJobs { get; private set; }

        /** <inheritDoc /> */
        public int CurrentCancelledJobs { get; private set; }

        /** <inheritDoc /> */
        public float AverageCancelledJobs { get; private set; }

        /** <inheritDoc /> */
        public int TotalCancelledJobs { get; private set; }

        /** <inheritDoc /> */
        public int TotalExecutedJobs { get; private set; }

        /** <inheritDoc /> */
        public long MaximumJobWaitTime { get; private set; }

        /** <inheritDoc /> */
        public long CurrentJobWaitTime { get; private set; }

        /** <inheritDoc /> */
        public double AverageJobWaitTime { get; private set; }

        /** <inheritDoc /> */
        public long MaximumJobExecuteTime { get; private set; }

        /** <inheritDoc /> */
        public long CurrentJobExecuteTime { get; private set; }

        /** <inheritDoc /> */
        public double AverageJobExecuteTime { get; private set; }

        /** <inheritDoc /> */
        public int TotalExecutedTasks { get; private set; }

        /** <inheritDoc /> */
        public long TotalBusyTime
        {
            get { return UpTime - TotalIdleTime; }
        }

        /** <inheritDoc /> */
        public long TotalIdleTime { get; private set; }

        /** <inheritDoc /> */
        public long CurrentIdleTime { get; private set; }

        /** <inheritDoc /> */
        public float BusyTimePercentage
        {
            get { return 1 - IdleTimePercentage; }
        }

        /** <inheritDoc /> */
        public float IdleTimePercentage
        {
            get { return TotalIdleTime / (float) UpTime; }
        }

        /** <inheritDoc /> */
        public int TotalCpus { get; private set; }

        /** <inheritDoc /> */
        public double CurrentCpuLoad { get; private set; }

        /** <inheritDoc /> */
        public double AverageCpuLoad { get; private set; }

        /** <inheritDoc /> */
        public double CurrentGcCpuLoad { get; private set; }

        /** <inheritDoc /> */
        public long HeapMemoryInitialized { get; private set; }

        /** <inheritDoc /> */
        public long HeapMemoryUsed { get; private set; }

        /** <inheritDoc /> */
        public long HeapMemoryCommitted { get; private set; }

        /** <inheritDoc /> */
        public long HeapMemoryMaximum { get; private set; }

        /** <inheritDoc /> */
        public long HeapMemoryTotal { get; private set; }

        /** <inheritDoc /> */
        public long NonHeapMemoryInitialized { get; private set; }

        /** <inheritDoc /> */
        public long NonHeapMemoryUsed { get; private set; }

        /** <inheritDoc /> */
        public long NonHeapMemoryCommitted { get; private set; }

        /** <inheritDoc /> */
        public long NonHeapMemoryMaximum { get; private set; }

        /** <inheritDoc /> */
        public long NonHeapMemoryTotal { get; private set; }

        /** <inheritDoc /> */
        public long UpTime { get; private set; }

        /** <inheritDoc /> */
        public DateTime StartTime { get; private set; }

        /** <inheritDoc /> */
        public DateTime NodeStartTime { get; private set; }

        /** <inheritDoc /> */
        public int CurrentThreadCount { get; private set; }

        /** <inheritDoc /> */
        public int MaximumThreadCount { get; private set; }

        /** <inheritDoc /> */
        public long TotalStartedThreadCount { get; private set; }

        /** <inheritDoc /> */
        public int CurrentDaemonThreadCount { get; private set; }

        /** <inheritDoc /> */
        public long LastDataVersion { get; private set; }

        /** <inheritDoc /> */
        public int SentMessagesCount { get; private set; }

        /** <inheritDoc /> */
        public long SentBytesCount { get; private set; }

        /** <inheritDoc /> */
        public int ReceivedMessagesCount { get; private set; }

        /** <inheritDoc /> */
        public long ReceivedBytesCount { get; private set; }

        /** <inheritDoc /> */
        public int OutboundMessagesQueueSize { get; private set; }

        /** <inheritDoc /> */
        public int TotalNodes { get; private set; }
    }
}

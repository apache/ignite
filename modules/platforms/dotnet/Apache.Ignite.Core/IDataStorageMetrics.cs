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

namespace Apache.Ignite.Core
{
    using System;

    /// <summary>
    /// Persistent store metrics.
    /// </summary>
    public interface IDataStorageMetrics
    {
        /// <summary>
        /// Gets the average number of WAL records per second written during the last time interval. 
        /// </summary>
        float WalLoggingRate { get; }

        /// <summary>
        /// Gets the average number of bytes per second written during the last time interval.
        /// </summary>
        float WalWritingRate { get; }

        /// <summary>
        /// Gets the current number of WAL segments in the WAL archive.
        /// </summary>
        int WalArchiveSegments { get; }

        /// <summary>
        /// Gets the average WAL fsync duration in microseconds over the last time interval.
        /// </summary>
        float WalFsyncTimeAverage { get; }

        /// <summary>
        /// Gets the duration of the last checkpoint.
        /// </summary>
        TimeSpan LastCheckpointDuration { get; }

        /// <summary>
        /// Gets the duration of last checkpoint lock wait.
        /// </summary>
        TimeSpan LastCheckpointLockWaitDuration { get; }

        /// <summary>
        /// Gets the duration of last checkpoint mark phase.
        /// </summary>
        TimeSpan LastCheckpointMarkDuration { get; }

        /// <summary>
        /// Gets the duration of last checkpoint pages write phase.
        /// </summary>
        TimeSpan LastCheckpointPagesWriteDuration { get; }

        /// <summary>
        /// Gets the duration of the sync phase of the last checkpoint.
        /// </summary>
        TimeSpan LastCheckpointFsyncDuration { get; }

        /// <summary>
        /// Gets the total number of pages written during the last checkpoint.
        /// </summary>
        long LastCheckpointTotalPagesNumber { get; }

        /// <summary>
        /// Gets the number of data pages written during the last checkpoint.
        /// </summary>
        long LastCheckpointDataPagesNumber { get; }

        /// <summary>
        /// Gets the number of pages copied to a temporary checkpoint buffer during the last checkpoint.
        /// </summary>
        long LastCheckpointCopiedOnWritePagesNumber { get; }
    }
}

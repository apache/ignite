/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.PersistentStore
{
    using System;

    /// <summary>
    /// Persistent store metrics.
    /// Obsolete, see <see cref="IDataStorageMetrics"/>.
    /// </summary>
    [Obsolete("Use IDataStorageMetrics")]
    public interface IPersistentStoreMetrics
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
        TimeSpan LastCheckpointingDuration { get; }

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

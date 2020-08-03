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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Data storage metrics.
    /// </summary>
    internal class DataStorageMetrics : IDataStorageMetrics
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataStorageMetrics"/> class.
        /// </summary>
        public DataStorageMetrics(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            WalLoggingRate = reader.ReadFloat();
            WalWritingRate = reader.ReadFloat();
            WalArchiveSegments = reader.ReadInt();
            WalFsyncTimeAverage = reader.ReadFloat();
            LastCheckpointDuration = reader.ReadLongAsTimespan();
            LastCheckpointLockWaitDuration = reader.ReadLongAsTimespan();
            LastCheckpointMarkDuration = reader.ReadLongAsTimespan();
            LastCheckpointPagesWriteDuration = reader.ReadLongAsTimespan();
            LastCheckpointFsyncDuration = reader.ReadLongAsTimespan();
            LastCheckpointTotalPagesNumber = reader.ReadLong();
            LastCheckpointDataPagesNumber = reader.ReadLong();
            LastCheckpointCopiedOnWritePagesNumber = reader.ReadLong();
        }

        /** <inheritdoc /> */
        public float WalLoggingRate { get; private set; }

        /** <inheritdoc /> */
        public float WalWritingRate { get; private set; }

        /** <inheritdoc /> */
        public int WalArchiveSegments { get; private set; }

        /** <inheritdoc /> */
        public float WalFsyncTimeAverage { get; private set; }

        /** <inheritdoc /> */
        public TimeSpan LastCheckpointDuration { get; private set; }

        /** <inheritdoc /> */
        public TimeSpan LastCheckpointLockWaitDuration { get; private set; }

        /** <inheritdoc /> */
        public TimeSpan LastCheckpointMarkDuration { get; private set; }

        /** <inheritdoc /> */
        public TimeSpan LastCheckpointPagesWriteDuration { get; private set; }

        /** <inheritdoc /> */
        public TimeSpan LastCheckpointFsyncDuration { get; private set; }

        /** <inheritdoc /> */
        public long LastCheckpointTotalPagesNumber { get; private set; }

        /** <inheritdoc /> */
        public long LastCheckpointDataPagesNumber { get; private set; }

        /** <inheritdoc /> */
        public long LastCheckpointCopiedOnWritePagesNumber { get; private set; }
    }
}
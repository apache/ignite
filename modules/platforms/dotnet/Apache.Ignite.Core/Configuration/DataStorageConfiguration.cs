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

namespace Apache.Ignite.Core.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Data storage configuration for Ignite page memory.
    /// <para />
    /// The page memory is a manageable off-heap based memory architecture that divides all expandable data
    /// regions into pages of fixed size. An individual page can store one or many cache key-value entries
    /// that allows reusing the memory in the most efficient way and avoid memory fragmentation issues.
    /// <para />
    /// By default, the page memory allocates a single expandable data region. All the caches that will be
    /// configured in an application will be mapped to this data region by default, thus, all the cache data
    /// will reside in that data region.
    /// </summary>
    public class DataStorageConfiguration
    {
        /// <summary>
        /// Default value for <see cref="CheckpointThreads"/>.
        /// </summary>
        public const int DefaultCheckpointThreads = 4;

        /// <summary>
        /// Default name is assigned to default data region if no user-defined
        /// <see cref="DefaultDataRegionConfiguration"/> is specified.
        /// </summary>
        public const string DefaultDataRegionName = "default";

        /// <summary>
        /// Default value for <see cref="CheckpointFrequency"/>.
        /// </summary>
        public static readonly TimeSpan DefaultCheckpointFrequency = TimeSpan.FromSeconds(180);

        /// <summary>
        /// Default value for <see cref="LockWaitTime"/>.
        /// </summary>
        public static readonly TimeSpan DefaultLockWaitTime = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Default value for <see cref="WalHistorySize"/>.
        /// </summary>
        public const int DefaultWalHistorySize = 20;

        /// <summary>
        /// Default value for <see cref="WalSegments"/>.
        /// </summary>
        public const int DefaultWalSegments = 10;

        /// <summary>
        /// Default value for <see cref="WalSegmentSize"/>.
        /// </summary>
        public const int DefaultWalSegmentSize = 64 * 1024 * 1024;

        /// <summary>
        /// Default value for <see cref="WalThreadLocalBufferSize"/>.
        /// </summary>
        public const int DefaultTlbSize = 128 * 1024;

        /// <summary>
        /// Default value for <see cref="WalFlushFrequency"/>.
        /// </summary>
        public static readonly TimeSpan DefaultWalFlushFrequency = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Default value for <see cref="WalRecordIteratorBufferSize"/>.
        /// </summary>
        public const int DefaultWalRecordIteratorBufferSize = 64 * 1024 * 1024;

        /// <summary>
        /// Default value for <see cref="WalFsyncDelayNanos"/>.
        /// </summary>
        public const long DefaultWalFsyncDelayNanos = 1000;

        /// <summary>
        /// The default sub intervals.
        /// </summary>
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with Java config")]
        public const int DefaultMetricsSubIntervalCount = 5;

        /// <summary>
        /// Default value for <see cref="WalFlushFrequency"/>.
        /// </summary>
        public static readonly TimeSpan DefaultWalAutoArchiveAfterInactivity = TimeSpan.FromMilliseconds(-1);

        /// <summary>
        /// The default rate time interval.
        /// </summary>
        public static readonly TimeSpan DefaultMetricsRateTimeInterval = TimeSpan.FromSeconds(60);

        /// <summary>
        /// Default value for <see cref="WalPath"/>.
        /// </summary>
        public const string DefaultWalPath = "db/wal";

        /// <summary>
        /// Default value for <see cref="WalArchivePath"/>.
        /// </summary>
        public const string DefaultWalArchivePath = "db/wal/archive";

        /// <summary>
        /// Default value for <see cref="WalMode"/>.
        /// </summary>
        public const WalMode DefaultWalMode = WalMode.LogOnly;

        /// <summary>
        /// Default value for <see cref="CheckpointWriteOrder"/>.
        /// </summary>
        public const CheckpointWriteOrder DefaultCheckpointWriteOrder = CheckpointWriteOrder.Sequential;

        /// <summary>
        /// Default value for <see cref="WriteThrottlingEnabled"/>.
        /// </summary>
        public const bool DefaultWriteThrottlingEnabled = false;

        /// <summary>
        /// Default value for <see cref="WalCompactionEnabled"/>.
        /// </summary>
        public const bool DefaultWalCompactionEnabled = false;

        /// <summary>
        /// Default size of a memory chunk reserved for system cache initially.
        /// </summary>
        public const long DefaultSystemRegionInitialSize = 40 * 1024 * 1024;

        /// <summary>
        /// Default max size of a memory chunk for the system cache.
        /// </summary>
        public const long DefaultSystemRegionMaxSize = 100 * 1024 * 1024;

        /// <summary>
        /// The default page size.
        /// </summary>
        public const int DefaultPageSize = 4 * 1024;

        /// <summary>
        /// The default concurrency level.
        /// </summary>
        public static readonly int DefaultConcurrencyLevel = Environment.ProcessorCount;

        /// <summary>
        /// Default value for <see cref="MaxWalArchiveSize"/>.
        /// </summary>
        public const long DefaultMaxWalArchiveSize = 1024 * 1024 * 1024;

        /// <summary>
        /// Default value for <see cref="WalPageCompression"/>.
        /// </summary>
        public const DiskPageCompression DefaultWalPageCompression = DiskPageCompression.Disabled;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataStorageConfiguration"/> class.
        /// </summary>
        public DataStorageConfiguration()
        {
            CheckpointThreads = DefaultCheckpointThreads;
            CheckpointFrequency = DefaultCheckpointFrequency;
            LockWaitTime = DefaultLockWaitTime;
            WalHistorySize = DefaultWalHistorySize;
            WalSegments = DefaultWalSegments;
            WalSegmentSize = DefaultWalSegmentSize;
            WalThreadLocalBufferSize = DefaultTlbSize;
            WalFlushFrequency = DefaultWalFlushFrequency;
            WalRecordIteratorBufferSize = DefaultWalRecordIteratorBufferSize;
            WalFsyncDelayNanos = DefaultWalFsyncDelayNanos;
            MetricsRateTimeInterval = DefaultMetricsRateTimeInterval;
            MetricsSubIntervalCount = DefaultMetricsSubIntervalCount;
            WalArchivePath = DefaultWalArchivePath;
            WalPath = DefaultWalPath;
            WalMode = DefaultWalMode;
            CheckpointWriteOrder = DefaultCheckpointWriteOrder;
            WriteThrottlingEnabled = DefaultWriteThrottlingEnabled;
            WalCompactionEnabled = DefaultWalCompactionEnabled;
            SystemRegionInitialSize = DefaultSystemRegionInitialSize;
            SystemRegionMaxSize = DefaultSystemRegionMaxSize;
            PageSize = DefaultPageSize;
            WalAutoArchiveAfterInactivity = DefaultWalAutoArchiveAfterInactivity;
            MaxWalArchiveSize = DefaultMaxWalArchiveSize;
            WalPageCompression = DefaultWalPageCompression;
            ConcurrencyLevel = DefaultConcurrencyLevel;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataStorageConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal DataStorageConfiguration(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            StoragePath = reader.ReadString();
            CheckpointFrequency = reader.ReadLongAsTimespan();
            CheckpointThreads = reader.ReadInt();
            LockWaitTime = reader.ReadLongAsTimespan();
            WalHistorySize = reader.ReadInt();
            WalSegments = reader.ReadInt();
            WalSegmentSize = reader.ReadInt();
            WalPath = reader.ReadString();
            WalArchivePath = reader.ReadString();
            WalMode = (WalMode)reader.ReadInt();
            WalThreadLocalBufferSize = reader.ReadInt();
            WalFlushFrequency = reader.ReadLongAsTimespan();
            WalFsyncDelayNanos = reader.ReadLong();
            WalRecordIteratorBufferSize = reader.ReadInt();
            AlwaysWriteFullPages = reader.ReadBoolean();
            MetricsEnabled = reader.ReadBoolean();
            MetricsSubIntervalCount = reader.ReadInt();
            MetricsRateTimeInterval = reader.ReadLongAsTimespan();
            CheckpointWriteOrder = (CheckpointWriteOrder)reader.ReadInt();
            WriteThrottlingEnabled = reader.ReadBoolean();
            WalCompactionEnabled = reader.ReadBoolean();
            MaxWalArchiveSize = reader.ReadLong();

            SystemRegionInitialSize = reader.ReadLong();
            SystemRegionMaxSize = reader.ReadLong();
            PageSize = reader.ReadInt();
            ConcurrencyLevel = reader.ReadInt();
            WalAutoArchiveAfterInactivity = reader.ReadLongAsTimespan();
            CheckpointReadLockTimeout = reader.ReadTimeSpanNullable();
            WalPageCompression = (DiskPageCompression)reader.ReadInt();
            WalPageCompressionLevel = reader.ReadIntNullable();

            var count = reader.ReadInt();

            if (count > 0)
            {
                DataRegionConfigurations = Enumerable.Range(0, count)
                    .Select(x => new DataRegionConfiguration(reader))
                    .ToArray();
            }

            if (reader.ReadBoolean())
            {
                DefaultDataRegionConfiguration = new DataRegionConfiguration(reader);
            }
        }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            Debug.Assert(writer != null);

            writer.WriteString(StoragePath);
            writer.WriteTimeSpanAsLong(CheckpointFrequency);
            writer.WriteInt(CheckpointThreads);
            writer.WriteTimeSpanAsLong(LockWaitTime);
            writer.WriteInt(WalHistorySize);
            writer.WriteInt(WalSegments);
            writer.WriteInt(WalSegmentSize);
            writer.WriteString(WalPath);
            writer.WriteString(WalArchivePath);
            writer.WriteInt((int)WalMode);
            writer.WriteInt(WalThreadLocalBufferSize);
            writer.WriteTimeSpanAsLong(WalFlushFrequency);
            writer.WriteLong(WalFsyncDelayNanos);
            writer.WriteInt(WalRecordIteratorBufferSize);
            writer.WriteBoolean(AlwaysWriteFullPages);
            writer.WriteBoolean(MetricsEnabled);
            writer.WriteInt(MetricsSubIntervalCount);
            writer.WriteTimeSpanAsLong(MetricsRateTimeInterval);
            writer.WriteInt((int)CheckpointWriteOrder);
            writer.WriteBoolean(WriteThrottlingEnabled);
            writer.WriteBoolean(WalCompactionEnabled);
            writer.WriteLong(MaxWalArchiveSize);

            writer.WriteLong(SystemRegionInitialSize);
            writer.WriteLong(SystemRegionMaxSize);
            writer.WriteInt(PageSize);
            writer.WriteInt(ConcurrencyLevel);
            writer.WriteTimeSpanAsLong(WalAutoArchiveAfterInactivity);
            writer.WriteTimeSpanAsLongNullable(CheckpointReadLockTimeout);
            writer.WriteInt((int)WalPageCompression);
            writer.WriteIntNullable(WalPageCompressionLevel);

            if (DataRegionConfigurations != null)
            {
                writer.WriteInt(DataRegionConfigurations.Count);

                foreach (var region in DataRegionConfigurations)
                {
                    if (region == null)
                    {
                        throw new IgniteException(
                            "DataStorageConfiguration.DataRegionConfigurations must not contain null items.");
                    }

                    region.Write(writer);
                }
            }
            else
            {
                writer.WriteInt(0);
            }

            if (DefaultDataRegionConfiguration != null)
            {
                writer.WriteBoolean(true);
                DefaultDataRegionConfiguration.Write(writer);
            }
            else
            {
                writer.WriteBoolean(false);
            }
        }

        /// <summary>
        /// Gets or sets the path where data and indexes will be persisted.
        /// </summary>
        public string StoragePath { get; set; }

        /// <summary>
        /// Gets or sets the checkpointing frequency which is a minimal interval when the dirty pages will be written
        /// to the Persistent Store.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:03:00")]
        public TimeSpan CheckpointFrequency { get; set; }

        /// <summary>
        /// Gets or sets the number of threads for checkpointing.
        /// </summary>
        [DefaultValue(DefaultCheckpointThreads)]
        public int CheckpointThreads { get; set; }

        /// <summary>
        /// Gets or sets the persistent manager file lock wait time.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:10")]
        public TimeSpan LockWaitTime { get; set; }

        /// <summary>
        /// Gets or sets the number of checkpoints to store in WAL (Write Ahead Log) history.
        /// </summary>
        [DefaultValue(DefaultWalHistorySize)]
        public int WalHistorySize { get; set; }

        /// <summary>
        /// Gets or sets a number of WAL (Write Ahead Log) segments to work with.
        /// For performance reasons, the whole WAL is split into files of fixed length called segments.
        /// </summary>
        [DefaultValue(DefaultWalSegments)]
        public int WalSegments { get; set; }

        /// <summary>
        /// Gets or sets the size of the WAL (Write Ahead Log) segment.
        /// For performance reasons, the whole WAL is split into files of fixed length called segments.
        /// </summary>
        [DefaultValue(DefaultWalSegmentSize)]
        public int WalSegmentSize { get; set; }

        /// <summary>
        /// Gets or sets the path to the directory where WAL (Write Ahead Log) is stored.
        /// </summary>
        [DefaultValue(DefaultWalPath)]
        public string WalPath { get; set; }

        /// <summary>
        /// Gets or sets the path to the directory where WAL (Write Ahead Log) archive is stored.
        /// Every WAL segment will be fully copied to this directory before it can be reused for WAL purposes.
        /// </summary>
        [DefaultValue(DefaultWalArchivePath)]
        public string WalArchivePath { get; set; }

        /// <summary>
        /// Gets or sets the WAL (Write Ahead Log) mode.
        /// </summary>
        [DefaultValue(DefaultWalMode)]
        public WalMode WalMode { get; set; }

        /// <summary>
        /// Gets or sets the size of the TLB (Thread-Local Buffer), in bytes.
        /// </summary>
        [DefaultValue(DefaultTlbSize)]
        public int WalThreadLocalBufferSize { get; set; }

        /// <summary>
        /// Gets or sets the WAL (Write Ahead Log) flush frequency.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:02")]
        public TimeSpan WalFlushFrequency { get; set; }

        /// <summary>
        /// Gets or sets the WAL (Write Ahead Log) fsync (disk sync) delay, in nanoseconds
        /// </summary>
        [DefaultValue(DefaultWalFsyncDelayNanos)]
        public long WalFsyncDelayNanos { get; set; }

        /// <summary>
        /// Gets or sets the size of the WAL (Write Ahead Log) record iterator buffer, in bytes.
        /// </summary>
        [DefaultValue(DefaultWalRecordIteratorBufferSize)]
        public int WalRecordIteratorBufferSize { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether full pages should always be written.
        /// </summary>
        public bool AlwaysWriteFullPages { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to enable data storage metrics.
        /// See <see cref="IIgnite.GetDataStorageMetrics"/>.
        /// </summary>
        public bool MetricsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the length of the time interval for rate-based metrics.
        /// This interval defines a window over which hits will be tracked.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:01:00")]
        public TimeSpan MetricsRateTimeInterval { get; set; }

        /// <summary>
        /// Number of sub-intervals to split the <see cref="MetricsRateTimeInterval"/> into to track the update history.
        /// </summary>
        [DefaultValue(DefaultMetricsSubIntervalCount)]
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with Java config")]
        public int MetricsSubIntervalCount { get; set; }

        /// <summary>
        /// Gets or sets the checkpoint page write order on disk.
        /// </summary>
        [DefaultValue(DefaultCheckpointWriteOrder)]
        public CheckpointWriteOrder CheckpointWriteOrder { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether threads that generate dirty
        /// pages too fast during ongoing checkpoint will be throttled.
        /// </summary>
        [DefaultValue(DefaultWriteThrottlingEnabled)]
        public bool WriteThrottlingEnabled { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether WAL compaction is enabled.
        /// If true, system filters and compresses WAL archive in background.
        /// Compressed WAL archive gets automatically decompressed on demand.
        /// </summary>
        [DefaultValue(DefaultWalCompactionEnabled)]
        public bool WalCompactionEnabled { get; set; }

        /// <summary>
        /// Gets or sets maximum size of wal archive folder, in bytes.
        /// </summary>
        [DefaultValue(DefaultMaxWalArchiveSize)]
        public long MaxWalArchiveSize { get; set; }

        /// <summary>
        /// Gets or sets the size of a memory chunk reserved for system needs.
        /// </summary>
        [DefaultValue(DefaultSystemRegionInitialSize)]
        public long SystemRegionInitialSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum memory region size reserved for system needs.
        /// </summary>
        [DefaultValue(DefaultSystemRegionMaxSize)]
        public long SystemRegionMaxSize { get; set; }

        /// <summary>
        /// Gets or sets the size of the memory page.
        /// </summary>
        [DefaultValue(DefaultPageSize)]
        public int PageSize { get; set; }

        /// <summary>
        /// Gets or sets the number of concurrent segments in Ignite internal page mapping tables.
        /// </summary>
        public int ConcurrencyLevel { get; set; }

        /// <summary>
        /// Gets or sets the inactivity time after which to run WAL segment auto archiving.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "-00:00:00.001")]
        public TimeSpan WalAutoArchiveAfterInactivity { get; set; }

        /// <summary>
        /// Gets or sets the timeout for checkpoint read lock acquisition.
        /// </summary>
        public TimeSpan? CheckpointReadLockTimeout { get; set; }

        /// <summary>
        /// Gets or sets the compression algorithm for WAL page snapshot records.
        /// </summary>
        public DiskPageCompression WalPageCompression { get; set; }

        /// <summary>
        /// Gets or sets the compression level for WAL page snapshot records.
        /// </summary>
        public int? WalPageCompressionLevel { get; set; }

        /// <summary>
        /// Gets or sets the data region configurations.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<DataRegionConfiguration> DataRegionConfigurations { get; set; }

        /// <summary>
        /// Gets or sets the default region configuration.
        /// </summary>
        public DataRegionConfiguration DefaultDataRegionConfiguration { get; set; }
    }
}

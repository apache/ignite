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
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Defines custom data region configuration for Apache Ignite page memory
    /// (see <see cref="DataStorageConfiguration"/>). 
    /// <para />
    /// For each configured data region Apache Ignite instantiates respective memory regions with different
    /// parameters like maximum size, eviction policy, swapping options, etc.
    /// An Apache Ignite cache can be mapped to a particular region using
    /// <see cref="CacheConfiguration.DataRegionName"/> method.
    /// </summary>
    public class DataRegionConfiguration
    {
        /// <summary>
        /// Default value for <see cref="PersistenceEnabled"/>.
        /// </summary>
        public const bool DefaultPersistenceEnabled = false;

        /// <summary>
        /// The default eviction threshold.
        /// </summary>
        public const double DefaultEvictionThreshold = 0.9;

        /// <summary>
        /// The default empty pages pool size.
        /// </summary>
        public const int DefaultEmptyPagesPoolSize = 100;

        /// <summary>
        /// The default initial size.
        /// </summary>
        public const long DefaultInitialSize = 256 * 1024 * 1024;

        /// <summary>
        /// The default maximum size, equals to 20% of total RAM.
        /// </summary>
        public static readonly long DefaultMaxSize =
            (long)((long)MemoryInfo.GetTotalPhysicalMemory(2048L * 1024 * 1024) * 0.2);

        /// <summary>
        /// The default sub intervals.
        /// </summary>
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with Java config")]
        public const int DefaultMetricsSubIntervalCount = 5;

        /// <summary>
        /// The default rate time interval.
        /// </summary>
        public static readonly TimeSpan DefaultMetricsRateTimeInterval = TimeSpan.FromSeconds(60);

        /// <summary>
        /// Default value for <see cref="LazyMemoryAllocation"/>.
        /// </summary>
        public const bool DefaultLazyMemoryAllocation = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataRegionConfiguration"/> class.
        /// </summary>
        public DataRegionConfiguration()
        {
            PersistenceEnabled = DefaultPersistenceEnabled;
            EvictionThreshold = DefaultEvictionThreshold;
            EmptyPagesPoolSize = DefaultEmptyPagesPoolSize;
            InitialSize = DefaultInitialSize;
            MaxSize = DefaultMaxSize;
            MetricsSubIntervalCount = DefaultMetricsSubIntervalCount;
            MetricsRateTimeInterval = DefaultMetricsRateTimeInterval;
            LazyMemoryAllocation = DefaultLazyMemoryAllocation;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataRegionConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal DataRegionConfiguration(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
            PersistenceEnabled = reader.ReadBoolean();
            InitialSize = reader.ReadLong();
            MaxSize = reader.ReadLong();
            SwapPath = reader.ReadString();
            PageEvictionMode = (DataPageEvictionMode)reader.ReadInt();
            EvictionThreshold = reader.ReadDouble();
            EmptyPagesPoolSize = reader.ReadInt();
            MetricsEnabled = reader.ReadBoolean();
            MetricsSubIntervalCount = reader.ReadInt();
            MetricsRateTimeInterval = reader.ReadLongAsTimespan();
            CheckpointPageBufferSize = reader.ReadLong();

            LazyMemoryAllocation = reader.ReadBoolean();
        }

        /// <summary>
        /// Writes this instance to a writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(Name);
            writer.WriteBoolean(PersistenceEnabled);
            writer.WriteLong(InitialSize);
            writer.WriteLong(MaxSize);
            writer.WriteString(SwapPath);
            writer.WriteInt((int)PageEvictionMode);
            writer.WriteDouble(EvictionThreshold);
            writer.WriteInt(EmptyPagesPoolSize);
            writer.WriteBoolean(MetricsEnabled);
            writer.WriteInt(MetricsSubIntervalCount);
            writer.WriteTimeSpanAsLong(MetricsRateTimeInterval);
            writer.WriteLong(CheckpointPageBufferSize);
            writer.WriteBoolean(LazyMemoryAllocation);
        }

        /// <summary>
        /// Gets or sets the data region name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether disk persistence is enabled for this region.
        /// Default is <see cref="DefaultPersistenceEnabled"/>.
        /// </summary>
        [DefaultValue(DefaultPersistenceEnabled)]
        public bool PersistenceEnabled { get; set; }

        /// <summary>
        /// Gets or sets initial memory region size.
        /// When the used memory size exceeds this value, new chunks of memory will be allocated.
        /// </summary>
        [DefaultValue(DefaultInitialSize)]
        public long InitialSize { get; set; }

        /// <summary>
        /// Sets maximum memory region size. The total size should not be less
        /// than 10 MB due to internal data structures overhead.
        /// </summary>
        public long MaxSize { get; set; }

        /// <summary>
        /// Gets or sets the the path to the directory for memory-mapped files.
        /// <para />
        /// Null for no swap.
        /// </summary>
        public string SwapPath { get; set; }

        /// <summary>
        /// Gets or sets the page eviction mode. If <see cref="DataPageEvictionMode.Disabled"/> is used (default)
        /// then an out of memory exception will be thrown if the memory region usage 
        /// goes beyond <see cref="MaxSize"/>.
        /// </summary>
        public DataPageEvictionMode PageEvictionMode { get; set; }

        /// <summary>
        /// Gets or sets the threshold for memory pages eviction initiation. For instance, if the threshold is 0.9
        /// it means that the page memory will start the eviction only after 90% of the memory region is occupied.
        /// </summary>
        [DefaultValue(DefaultEvictionThreshold)]
        public double EvictionThreshold { get; set; }

        /// <summary>
        /// Gets or sets the minimal number of empty pages to be present in reuse lists for this data region.
        /// This parameter ensures that Ignite will be able to successfully evict old data entries when the size of
        /// (key, value) pair is slightly larger than page size / 2.
        /// Increase this parameter if cache can contain very big entries (total size of pages in this pool
        /// should be enough to contain largest cache entry).
        /// </summary>
        [DefaultValue(DefaultEmptyPagesPoolSize)]
        public int EmptyPagesPoolSize { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether memory metrics should be enabled.
        /// <para />
        /// Metrics can be retrieved with <see cref="IIgnite.GetDataRegionMetrics()"/> method.
        /// </summary>
        public bool MetricsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the rate time interval for <see cref="IDataRegionMetrics.AllocationRate"/>
        /// and <see cref="IDataRegionMetrics.EvictionRate"/> monitoring purposes.
        /// <para />
        /// For instance, after setting the interval to 60 seconds, subsequent calls
        /// to <see cref="IDataRegionMetrics.AllocationRate"/> will return average allocation
        /// rate (pages per second) for the last minute.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:01:00")]
        public TimeSpan MetricsRateTimeInterval { get; set; }

        /// <summary>
        /// Gets or sets the number of sub intervals to split <see cref="MetricsRateTimeInterval"/> into to calculate 
        /// <see cref="IDataRegionMetrics.AllocationRate"/> and <see cref="IDataRegionMetrics.EvictionRate"/>.
        /// <para />
        /// Bigger value results in more accurate metrics.
        /// </summary>
        [DefaultValue(DefaultMetricsSubIntervalCount)]
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with Java config")]
        public int MetricsSubIntervalCount { get; set; }

        /// <summary>
        /// Gets or sets the size of the checkpointing page buffer.
        /// <para />
        /// Default is <c>0</c>: Ignite will choose buffer size automatically.
        /// </summary>
        public long CheckpointPageBufferSize { get; set; }
        
        /// <summary>
        /// Gets or sets the lazy memory allocation flag.
        /// </summary>
        [DefaultValue(DefaultLazyMemoryAllocation)]
        public bool LazyMemoryAllocation { get; set; }
    }
}

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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Defines page memory policy configuration. See <see cref="MemoryConfiguration.MemoryPolicies"/>.
    /// Obsolete, use <see cref="DataRegionConfiguration"/>.
    /// </summary>
    [Obsolete("Use DataRegionConfiguration.")]
    public class MemoryPolicyConfiguration
    {
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
            (long) ((long) MemoryInfo.GetTotalPhysicalMemory(2048L * 1024 * 1024) * 0.2);

        /// <summary>
        /// The default sub intervals.
        /// </summary>
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with Java config")]
        public const int DefaultSubIntervals = 5;

        /// <summary>
        /// The default rate time interval.
        /// </summary>
        public static readonly TimeSpan DefaultRateTimeInterval = TimeSpan.FromSeconds(60);

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPolicyConfiguration"/> class.
        /// </summary>
        public MemoryPolicyConfiguration()
        {
            EvictionThreshold = DefaultEvictionThreshold;
            EmptyPagesPoolSize = DefaultEmptyPagesPoolSize;
            Name = MemoryConfiguration.DefaultDefaultMemoryPolicyName;
            InitialSize = DefaultInitialSize;
            MaxSize = DefaultMaxSize;
            SubIntervals = DefaultSubIntervals;
            RateTimeInterval = DefaultRateTimeInterval;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPolicyConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal MemoryPolicyConfiguration(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
            InitialSize = reader.ReadLong();
            MaxSize = reader.ReadLong();
            SwapFilePath = reader.ReadString();
            PageEvictionMode = (DataPageEvictionMode) reader.ReadInt();
            EvictionThreshold = reader.ReadDouble();
            EmptyPagesPoolSize = reader.ReadInt();
            MetricsEnabled = reader.ReadBoolean();
            SubIntervals = reader.ReadInt();
            RateTimeInterval = reader.ReadLongAsTimespan();
        }

        /// <summary>
        /// Writes this instance to a writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(Name);
            writer.WriteLong(InitialSize);
            writer.WriteLong(MaxSize);
            writer.WriteString(SwapFilePath);
            writer.WriteInt((int) PageEvictionMode);
            writer.WriteDouble(EvictionThreshold);
            writer.WriteInt(EmptyPagesPoolSize);
            writer.WriteBoolean(MetricsEnabled);
            writer.WriteInt(SubIntervals);
            writer.WriteTimeSpanAsLong(RateTimeInterval);
        }

        /// <summary>
        /// Gets or sets the memory policy name.
        /// Defaults to <see cref="MemoryConfiguration.DefaultDefaultMemoryPolicyName"/>.
        /// </summary>
        [DefaultValue(MemoryConfiguration.DefaultDefaultMemoryPolicyName)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets initial memory region size defined by this memory policy.
        /// When the used memory size exceeds this value, new chunks of memory will be allocated.
        /// </summary>
        [DefaultValue(DefaultInitialSize)]
        public long InitialSize { get; set; }

        /// <summary>
        /// Sets maximum memory region size defined by this memory policy. The total size should not be less
        /// than 10 MB due to internal data structures overhead.
        /// </summary>
        public long MaxSize { get; set; }

        /// <summary>
        /// Gets or sets the the path to the memory-mapped file the memory region defined by this memory policy
        /// will be mapped to. Having the path set, allows relying on swapping capabilities of an underlying
        /// operating system for the memory region.
        /// <para />
        /// Null for no swap.
        /// </summary>
        public string SwapFilePath { get; set; }

        /// <summary>
        /// Gets or sets the page eviction mode. If <see cref="DataPageEvictionMode.Disabled"/> is used (default)
        /// then an out of memory exception will be thrown if the memory region usage,
        /// defined by this memory policy, goes beyond <see cref="MaxSize"/>.
        /// </summary>
        public DataPageEvictionMode PageEvictionMode { get; set; }

        /// <summary>
        /// Gets or sets the threshold for memory pages eviction initiation. For instance, if the threshold is 0.9
        /// it means that the page memory will start the eviction only after 90% of the memory region
        /// (defined by this policy) is occupied.
        /// </summary>
        [DefaultValue(DefaultEvictionThreshold)]
        public double EvictionThreshold { get; set; }

        /// <summary>
        /// Gets or sets the minimal number of empty pages to be present in reuse lists for this memory policy.
        /// This parameter ensures that Ignite will be able to successfully evict old data entries when the size of
        /// (key, value) pair is slightly larger than page size / 2.
        /// Increase this parameter if cache can contain very big entries (total size of pages in this pool
        /// should be enough to contain largest cache entry).
        /// </summary>
        [DefaultValue(DefaultEmptyPagesPoolSize)]
        public int EmptyPagesPoolSize { get;set; }

        /// <summary>
        /// Gets or sets a value indicating whether memory metrics should be enabled.
        /// <para />
        /// Metrics can be retrieved with <see cref="IIgnite.GetMemoryMetrics()"/> method.
        /// </summary>
        public bool MetricsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the rate time interval for <see cref="IMemoryMetrics.AllocationRate"/>
        /// and <see cref="IMemoryMetrics.EvictionRate"/> monitoring purposes.
        /// <para />
        /// For instance, after setting the interval to 60 seconds, subsequent calls
        /// to <see cref="IMemoryMetrics.AllocationRate"/> will return average allocation
        /// rate (pages per second) for the last minute.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:01:00")]
        public TimeSpan RateTimeInterval { get; set; }

        /// <summary>
        /// Gets or sets the number of sub intervals to split <see cref="RateTimeInterval"/> into to calculate 
        /// <see cref="IMemoryMetrics.AllocationRate"/> and <see cref="IMemoryMetrics.EvictionRate"/>.
        /// <para />
        /// Bigger value results in more accurate metrics.
        /// </summary>
        [DefaultValue(DefaultSubIntervals)]
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", 
            Justification = "Consistency with Java config")]
        public int SubIntervals { get; set; }
    }
}
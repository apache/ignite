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
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Defines page memory policy configuration. See <see cref="MemoryConfiguration.MemoryPolicies"/>.
    /// </summary>
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
        /// Initializes a new instance of the <see cref="MemoryPolicyConfiguration"/> class.
        /// </summary>
        public MemoryPolicyConfiguration()
        {
            EvictionThreshold = DefaultEvictionThreshold;
            EmptyPagesPoolSize = DefaultEmptyPagesPoolSize;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryPolicyConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal MemoryPolicyConfiguration(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
            Size = reader.ReadLong();
            SwapFilePath = reader.ReadString();
            PageEvictionMode = (DataPageEvictionMode) reader.ReadInt();
            EvictionThreshold = reader.ReadDouble();
            EmptyPagesPoolSize = reader.ReadInt();
        }

        /// <summary>
        /// Writes this instance to a writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(Name);
            writer.WriteLong(Size);
            writer.WriteString(SwapFilePath);
            writer.WriteInt((int) PageEvictionMode);
            writer.WriteDouble(EvictionThreshold);
            writer.WriteInt(EmptyPagesPoolSize);
        }

        /// <summary>
        /// Gets or sets the memory policy name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the maximum memory region size defined by this memory policy.
        /// If the whole data can not fit into the memory region an out of memory exception will be thrown.
        /// </summary>
        public long Size { get; set; }

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
        /// defined by this memory policy, goes beyond <see cref="Size"/>.
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
    }
}
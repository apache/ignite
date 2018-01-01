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
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;

    /// <summary>
    /// A page memory configuration for an Apache Ignite node. The page memory is a manageable off-heap based
    /// memory architecture that divides all continuously allocated memory regions into pages of fixed size.
    /// An individual page can store one or many cache key-value entries that allows reusing the memory
    /// in the most efficient way and avoid memory fragmentation issues. 
    /// <para />
    /// By default, the page memory allocates a single continuous memory region. All the caches that
    /// will be configured in an application will be mapped to this memory region by default,
    /// thus, all the cache data will reside in that memory region.
    /// <para />
    /// If initial size of the default memory region doesn't satisfy requirements or it's
    /// required to have multiple memory regions with different properties
    /// then <see cref="MemoryPolicyConfiguration" /> can be used for both scenarios.
    /// For instance, using memory policies you can define memory regions of different maximum size,
    /// eviction policies, swapping options, etc. Once you define a new memory region you can bind
    /// particular Ignite caches to it. <para />
    /// To learn more about memory policies refer to <see cref="MemoryPolicyConfiguration" /> documentation.
    /// <para />
    /// Obsolete, use <see cref="DataStorageConfiguration"/>.
    /// </summary>
    [Obsolete("Use DataStorageConfiguration.")]
    public class MemoryConfiguration
    {
        /// <summary>
        /// Default size of a memory chunk reserved for system cache initially.
        /// </summary>
        public const long DefaultSystemCacheInitialSize = 40 * 1024 * 1024;

        /// <summary>
        /// Default max size of a memory chunk for the system cache.
        /// </summary>
        public const long DefaultSystemCacheMaxSize = 100 * 1024 * 1024;

        /// <summary>
        /// The default page size.
        /// </summary>
        public const int DefaultPageSize = 4 * 1024;

        /// <summary>
        /// The default value for <see cref="DefaultMemoryPolicyName"/>.
        /// </summary>
        public const string DefaultDefaultMemoryPolicyName = "default";

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryConfiguration"/> class.
        /// </summary>
        public MemoryConfiguration()
        {
            SystemCacheInitialSize = DefaultSystemCacheInitialSize;
            SystemCacheMaxSize = DefaultSystemCacheMaxSize;
            PageSize = DefaultPageSize;
            DefaultMemoryPolicyName = DefaultDefaultMemoryPolicyName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public MemoryConfiguration(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            SystemCacheInitialSize = reader.ReadLong();
            SystemCacheMaxSize = reader.ReadLong();
            PageSize = reader.ReadInt();
            ConcurrencyLevel = reader.ReadInt();
            DefaultMemoryPolicyName = reader.ReadString();

            var count = reader.ReadInt();

            if (count > 0)
            {
                MemoryPolicies = Enumerable.Range(0, count)
                    .Select(x => new MemoryPolicyConfiguration(reader))
                    .ToArray();
            }
        }

        /// <summary>
        /// Writes this instance to a writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            Debug.Assert(writer != null);

            writer.WriteLong(SystemCacheInitialSize);
            writer.WriteLong(SystemCacheMaxSize);
            writer.WriteInt(PageSize);
            writer.WriteInt(ConcurrencyLevel);
            writer.WriteString(DefaultMemoryPolicyName);

            if (MemoryPolicies != null)
            {
                writer.WriteInt(MemoryPolicies.Count);

                foreach (var policy in MemoryPolicies)
                {
                    if (policy == null)
                    {
                        throw new IgniteException("MemoryConfiguration.MemoryPolicies must not contain null items.");
                    }

                    policy.Write(writer);
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }

        /// <summary>
        /// Gets or sets the size of a memory chunk reserved for system cache needs.
        /// </summary>
        [DefaultValue(DefaultSystemCacheInitialSize)]
        public long SystemCacheInitialSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum memory region size reserved for system cache.
        /// </summary>
        [DefaultValue(DefaultSystemCacheMaxSize)]
        public long SystemCacheMaxSize { get; set; }

        /// <summary>
        /// Gets or sets the size of the memory page.
        /// </summary>
        [DefaultValue(DefaultPageSize)]
        public int PageSize { get; set; }

        /// <summary>
        /// Gets or sets the number of concurrent segments in Ignite internal page mapping tables.
        /// </summary>
        public int ConcurrencyLevel { get;set; }

        /// <summary>
        /// Gets or sets the name of the default memory policy in <see cref="MemoryPolicies"/>.
        /// </summary>
        [DefaultValue(DefaultDefaultMemoryPolicyName)]
        public string DefaultMemoryPolicyName { get; set; }

        /// <summary>
        /// Gets or sets the memory policies.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<MemoryPolicyConfiguration> MemoryPolicies { get; set; }
    }
}

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

namespace Apache.Ignite.Core.Client.DataStructures
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Configuration for Ignite collections.
    /// </summary>
    public sealed class CollectionClientConfiguration
    {
        /// <summary>
        /// Default value for <see cref="CacheMode"/>.
        /// </summary>
        public const CacheMode DefaultCacheMode = CacheMode.Partitioned;

        /// <summary>
        /// Default value for <see cref="AtomicityMode"/>.
        /// </summary>
        public const CacheAtomicityMode DefaultAtomicityMode = CacheAtomicityMode.Atomic;

        /// <summary>
        /// Gets or sets the number of backup nodes for the underlying cache.
        /// </summary>
        public int Backups { get; set; }

        /// <summary>
        /// Gets or sets the cache mode for the underlying cache.
        /// </summary>
        [DefaultValue(DefaultCacheMode)]
        public CacheMode CacheMode { get; set; } = DefaultCacheMode;

        /// <summary>
        /// Gets or sets the cache mode for the underlying cache.
        /// </summary>
        [DefaultValue(DefaultAtomicityMode)]
        public CacheAtomicityMode AtomicityMode { get; set; } = DefaultAtomicityMode;

        /// <summary>
        /// Gets or sets a value indicating whether all set items should be stored on a single node.
        /// </summary>
        public bool Colocated { get; set; }
    }
}

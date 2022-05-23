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
    /// Configuration for atomic data structures. See <see cref="IAtomicLongClient"/>.
    /// </summary>
    public class AtomicClientConfiguration
    {
        /// <summary>
        /// Default value for <see cref="CacheMode"/>.
        /// </summary>
        public const CacheMode DefaultCacheMode = CacheMode.Partitioned;

        /// <summary>
        /// Default value for <see cref="AtomicSequenceReserveSize"/>.
        /// </summary>
        public const int DefaultAtomicSequenceReserveSize = 1000;

        /// <summary>
        /// Default value for <see cref="Backups"/>.
        /// </summary>
        public const int DefaultBackups = 1;

        /// <summary>
        /// Gets or sets the number of backup nodes for the underlying cache.
        /// </summary>
        [DefaultValue(DefaultBackups)]
        public int Backups { get; set; } = DefaultBackups;

        /// <summary>
        /// Gets or sets the cache mode for the underlying cache.
        /// </summary>
        [DefaultValue(DefaultCacheMode)]
        public CacheMode CacheMode { get; set; } = DefaultCacheMode;

        /// <summary>
        /// Gets or sets the default number of sequence values reserved for atomic sequence instances. After
        /// a certain number has been reserved, consequent increments of the sequence will happen locally,
        /// without communication with other nodes, until the next reservation has to be made.
        /// <para />
        /// Default is <see cref="DefaultAtomicSequenceReserveSize"/>
        /// </summary>
        [DefaultValue(DefaultAtomicSequenceReserveSize)]
        public int AtomicSequenceReserveSize { get; set; } = DefaultAtomicSequenceReserveSize;

        /// <summary>
        /// Gets or sets the group name for the underlying cache.
        /// </summary>
        public string GroupName { get; set; }
    }
}

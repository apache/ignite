/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Cache
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Enumeration of all supported cache peek modes.
    /// </summary>
    [Flags]
    [SuppressMessage("Microsoft.Naming", "CA1714:FlagsEnumsShouldHavePluralNames")]
    public enum CachePeekMode
    {
        /// <summary>
        /// Peeks into all available cache storages.
        /// </summary>
        All = 0x01,

        /// <summary>
        /// Peek into near cache only (don't peek into partitioned cache).
        /// In case of LOCAL cache, behaves as <see cref="All"/> mode.
        /// </summary>
        Near = 0x02,

        /// <summary>
        /// Peek value from primary copy of partitioned cache only (skip near cache).
        /// In case of LOCAL cache, behaves as <see cref="All"/> mode.
        /// </summary>
        Primary = 0x04,

        /// <summary>
        /// Peek value from backup copies of partitioned cache only (skip near cache).
        /// In case of LOCAL cache, behaves as <see cref="All"/> mode.
        /// </summary>
        Backup = 0x08,

        /// <summary>
        /// Peeks value from the on-heap storage only.
        /// </summary>
        Onheap = 0x10,

        /// <summary>
        /// Peeks value from the off-heap storage only, without loading off-heap value into cache.
        /// </summary>
        Offheap = 0x20
    }
}

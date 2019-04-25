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

    /// <summary>
    /// Memory usage metrics.
    /// Obsolete, use <see cref="IDataRegionMetrics"/>.
    /// </summary>
    [Obsolete("See IDataRegionMetrics.")]
    public interface IMemoryMetrics
    {
        /// <summary>
        /// Gets the memory policy name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the count of allocated pages.
        /// </summary>
        long TotalAllocatedPages { get; }

        /// <summary>
        /// Gets the allocation rate, in pages per second.
        /// </summary>
        float AllocationRate { get; }

        /// <summary>
        /// Gets the eviction rate, in pages per second.
        /// </summary>
        float EvictionRate { get; }

        /// <summary>
        /// Gets the percentage of pages fully occupied by entries that are larger than page.
        /// </summary>
        float LargeEntriesPagesPercentage { get; }

        /// <summary>
        /// Gets the page fill factor: the percentage of used space.
        /// </summary>
        float PageFillFactor { get; }
    }
}

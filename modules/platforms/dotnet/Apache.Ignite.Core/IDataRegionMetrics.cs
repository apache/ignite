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

namespace Apache.Ignite.Core
{
    using System;

    /// <summary>
    /// Memory usage metrics.
    /// </summary>
    public interface IDataRegionMetrics
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
        /// Gets a total number of pages used for storing the data.
        /// </summary>
        long TotalUsedPages { get; }

        /// <summary>
        /// Gets the size of allocated pages in bytes.
        /// </summary>
        long TotalAllocatedSize { get; }

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
        /// Gets the page fill factor: free space to overall size ratio across all pages.
        /// </summary>
        float PageFillFactor { get; }

        /// <summary>
        /// Gets the number of dirty RAM pages.
        /// </summary>
        long DirtyPages { get; }

        /// <summary>
        /// Gets the rate (pages per second) at which pages get replaced with other pages from persistent storage.
        /// </summary>
        float PageReplaceRate { get; }

        /// <summary>
        /// Gets the average age (in milliseconds) for pages being replaced from persistent storage.
        /// </summary>
        float PageReplaceAge { get; }

        /// <summary>
        /// Gets the count of pages loaded to RAM.
        /// </summary>
        long PhysicalMemoryPages { get; }

        /// <summary>
        /// Gets the size of pages loaded to RAM in bytes.
        /// </summary>
        long PhysicalMemorySize { get; }

        /// <summary>
        /// Gets checkpoint buffer size in pages.
        /// Deprecated, always returns 0. Use <see cref="UsedCheckpointBufferPages"/> instead.
        /// </summary>
        [Obsolete("Deprecated, always returns 0. Use UsedCheckpointBufferPages instead.")]
        long CheckpointBufferPages { get; }

        /// <summary>
        /// Gets checkpoint buffer size in bytes.
        /// </summary>
        long CheckpointBufferSize { get; }

        /// <summary>
        /// Gets used checkpoint buffer size in pages.
        /// </summary>
        long UsedCheckpointBufferPages { get; }

        /// <summary>
        /// Gets used checkpoint buffer size in bytes.
        /// </summary>
        long UsedCheckpointBufferSize { get; }

        /// <summary>
        /// Gets memory page size in bytes.
        /// </summary>
        int PageSize { get; }

        /// <summary>
        /// Gets the number of read pages from last restart.
        /// </summary>
        long PagesRead { get; }

        /// <summary>
        ///  Gets the number of written pages from last restart.
        /// </summary>
        long PagesWritten  { get; }

        /// <summary>
        /// Gets the number of replaced pages from last restart.
        /// </summary>
        long PagesReplaced  { get; }

        /// <summary>
        /// Gets total offheap size in bytes.
        /// </summary>
        long OffHeapSize { get; }

        /// <summary>
        /// Gets total used offheap size in bytes.
        /// </summary>
        long OffheapUsedSize  { get; }
    }
}

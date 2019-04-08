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

namespace Apache.Ignite.Core.Impl
{
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Data region metrics.
    /// </summary>
    internal class DataRegionMetrics : IDataRegionMetrics
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataRegionMetrics"/> class.
        /// </summary>
        public DataRegionMetrics(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            Name = reader.ReadString();
            TotalAllocatedPages = reader.ReadLong();
            TotalUsedPages = reader.ReadLong();
            TotalAllocatedSize = reader.ReadLong();
            AllocationRate = reader.ReadFloat();
            EvictionRate = reader.ReadFloat();
            LargeEntriesPagesPercentage = reader.ReadFloat();
            PageFillFactor = reader.ReadFloat();
            DirtyPages = reader.ReadLong();
            PageReplaceRate = reader.ReadFloat();
            PageReplaceAge = reader.ReadFloat();
            PhysicalMemoryPages = reader.ReadLong();
            PhysicalMemorySize = reader.ReadLong();
            UsedCheckpointBufferPages = reader.ReadLong();
            UsedCheckpointBufferSize = reader.ReadLong();
            PageSize = reader.ReadInt();
            CheckpointBufferSize = reader.ReadLong();
            PagesRead = reader.ReadLong();
            PagesWritten = reader.ReadLong();
            PagesReplaced = reader.ReadLong();
            OffHeapSize = reader.ReadLong();
            OffheapUsedSize = reader.ReadLong();
        }

        /** <inheritdoc /> */
        public string Name { get; private set; }

        /** <inheritdoc /> */
        public long TotalAllocatedPages { get; private set; }

        /** <inheritdoc /> */
        public long TotalUsedPages { get; private set; }

        /** <inheritdoc /> */
        public long TotalAllocatedSize { get; private set; }

        /** <inheritdoc /> */
        public float AllocationRate { get; private set; }

        /** <inheritdoc /> */
        public float EvictionRate { get; private set; }

        /** <inheritdoc /> */
        public long DirtyPages { get; private set; }

        /** <inheritdoc /> */
        public float PageReplaceRate { get; private set; }

        /** <inheritdoc /> */
        public float PageReplaceAge { get; private set; }

        /** <inheritdoc /> */
        public float LargeEntriesPagesPercentage { get; private set; }

        /** <inheritdoc /> */
        public float PageFillFactor { get; private set; }

        /** <inheritdoc /> */
        public long PhysicalMemoryPages { get; private set; }

        /** <inheritdoc /> */
        public long PhysicalMemorySize { get; private set; }

        /** <inheritdoc /> */
        public long CheckpointBufferPages
        {
            get
            {
                return 0L;
            }
        }

        /** <inheritdoc /> */
        public long CheckpointBufferSize { get; private set; }

        /** <inheritdoc /> */
        public long UsedCheckpointBufferPages { get; private set; }

        /** <inheritdoc /> */
        public long UsedCheckpointBufferSize { get; private set; }

        /** <inheritdoc /> */
        public int PageSize { get; private set; }

        /** <inheritdoc /> */
        public long PagesRead { get; private set; }

        /** <inheritdoc /> */
        public long PagesWritten { get; private set; }

        /** <inheritdoc /> */
        public long PagesReplaced { get; private set; }

        /** <inheritdoc /> */
        public long OffHeapSize { get; private set; }

        /** <inheritdoc /> */
        public long OffheapUsedSize { get; private set; }
    }
}

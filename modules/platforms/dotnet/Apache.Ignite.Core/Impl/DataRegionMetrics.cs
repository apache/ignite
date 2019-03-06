/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

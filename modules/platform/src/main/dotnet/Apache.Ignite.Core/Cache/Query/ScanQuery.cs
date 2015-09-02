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

namespace Apache.Ignite.Core.Cache.Query
{
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Scan query over cache entries. Will accept all the entries if no predicate was set.
    /// </summary>
    public class ScanQuery<TK, TV> : QueryBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQuery{K, V}"/> class.
        /// </summary>
        /// <param name="filter">The filter.</param>
        public ScanQuery(ICacheEntryFilter<TK, TV> filter = null)
        {
            Filter = filter;
        }

        /// <summary>
        /// Gets or sets the predicate.
        /// </summary>
        public ICacheEntryFilter<TK, TV> Filter { get; set; }

        /// <summary>
        /// Gets or sets partition number over which this query should iterate. If null, query will iterate 
        /// over all partitions in the cache. Must be in the range [0, N) where N is partition number in the cache.
        /// </summary>
        public int? Partition { get; set; }

        /** <inheritDoc /> */
        internal override void Write(PortableWriterImpl writer, bool keepPortable)
        {
            writer.WriteBoolean(Local);
            writer.WriteInt(PageSize);
            
            writer.WriteBoolean(Partition.HasValue);
            
            if (Partition.HasValue)
                writer.WriteInt(Partition.Value);

            if (Filter == null)
                writer.WriteObject<CacheEntryFilterHolder>(null);
            else
            {
                var holder = new CacheEntryFilterHolder(Filter, (key, val) => Filter.Invoke(
                    new CacheEntry<TK, TV>((TK) key, (TV) val)), writer.Marshaller, keepPortable);
                
                writer.WriteObject(holder);
                writer.WriteLong(holder.Handle);
            }
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QryScan; }
        }
    }
}

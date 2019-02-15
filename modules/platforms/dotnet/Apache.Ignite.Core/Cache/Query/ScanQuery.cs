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

namespace Apache.Ignite.Core.Cache.Query
{
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;

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
        internal override void Write(BinaryWriter writer, bool keepBinary)
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
                    new CacheEntry<TK, TV>((TK) key, (TV) val)), writer.Marshaller, keepBinary);

                writer.WriteObject(holder);
            }
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QryScan; }
        }
    }
}

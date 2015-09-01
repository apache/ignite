/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache.Query
{
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Scan query over cache entries. Will accept all the entries if no predicate was set.
    /// </summary>
    public class ScanQuery<K, V> : QueryBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQuery{K, V}"/> class.
        /// </summary>
        /// <param name="filter">The filter.</param>
        public ScanQuery(ICacheEntryFilter<K, V> filter = null)
        {
            Filter = filter;
        }

        /// <summary>
        /// Gets or sets the predicate.
        /// </summary>
        public ICacheEntryFilter<K, V> Filter { get; set; }

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
                    new CacheEntry<K, V>((K) key, (V) val)), writer.Marshaller, keepPortable);
                
                writer.WriteObject(holder);
                writer.WriteLong(holder.Handle);
            }
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QRY_SCAN; }
        }
    }
}

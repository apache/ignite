/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class QueryCursor<K, V> : AbstractQueryCursor<ICacheEntry<K, V>>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaler.</param>
        /// <param name="keepPortable">Keep poratble flag.</param>
        public QueryCursor(IUnmanagedTarget target, PortableMarshaller marsh,
            bool keepPortable) : base(target, marsh, keepPortable)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override ICacheEntry<K, V> Read(PortableReaderImpl reader)
        {
            K key = reader.ReadObject<K>();
            V val = reader.ReadObject<V>();

            return new CacheEntry<K, V>(key, val);
        }
    }
}

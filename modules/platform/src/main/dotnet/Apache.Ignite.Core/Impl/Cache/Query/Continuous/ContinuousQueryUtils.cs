/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache.Query.Continuous
{
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Cache.Event;
    using GridGain.Impl.Cache.Event;
    using GridGain.Impl.Portable;

    /// <summary>
    /// Utility methods for continuous queries.
    /// </summary>
    static class ContinuousQueryUtils
    {
        /// <summary>
        /// Read single event.
        /// </summary>
        /// <param name="stream">Stream to read data from.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        /// <returns>Event.</returns>
        public static ICacheEntryEvent<K, V> ReadEvent<K, V>(IPortableStream stream, 
            PortableMarshaller marsh, bool keepPortable)
        {
            var reader = marsh.StartUnmarshal(stream, keepPortable);

            return ReadEvent0<K, V>(reader);
        }

        /// <summary>
        /// Read multiple events.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        /// <returns>Events.</returns>
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        public static ICacheEntryEvent<K, V>[] ReadEvents<K, V>(IPortableStream stream,
            PortableMarshaller marsh, bool keepPortable)
        {
            var reader = marsh.StartUnmarshal(stream, keepPortable);

            int cnt = reader.ReadInt();

            ICacheEntryEvent<K, V>[] evts = new ICacheEntryEvent<K, V>[cnt];

            for (int i = 0; i < cnt; i++)
                evts[i] = ReadEvent0<K, V>(reader);

            return evts;
        }

        /// <summary>
        /// Read event.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Event.</returns>
        private static ICacheEntryEvent<K, V> ReadEvent0<K, V>(PortableReaderImpl reader)
        {
            reader.DetachNext();
            K key = reader.ReadObject<K>();

            reader.DetachNext();
            V oldVal = reader.ReadObject<V>();

            reader.DetachNext();
            V val = reader.ReadObject<V>();

            return CreateEvent(key, oldVal, val);
        }

        /// <summary>
        /// Create event.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="oldVal">Old value.</param>
        /// <param name="val">Value.</param>
        /// <returns>Event.</returns>
        public static ICacheEntryEvent<K, V> CreateEvent<K, V>(K key, V oldVal, V val)
        {
            if (oldVal == null)
            {
                Debug.Assert(val != null);

                return new CacheEntryCreateEvent<K, V>(key, val);
            }

            if (val == null)
            {
                Debug.Assert(oldVal != null);

                return new CacheEntryRemoveEvent<K, V>(key, oldVal);
            }
            
            return new CacheEntryUpdateEvent<K, V>(key, oldVal, val);
        }
    }
}

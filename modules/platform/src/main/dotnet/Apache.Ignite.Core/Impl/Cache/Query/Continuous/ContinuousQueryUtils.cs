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

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Cache.Event;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;

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

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
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Event;

    /// <summary>
    /// Utility methods for continuous queries.
    /// </summary>
    internal static class ContinuousQueryUtils
    {
        /// <summary>
        /// Read single event.
        /// </summary>
        /// <param name="stream">Stream to read data from.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <returns>Event.</returns>
        public static ICacheEntryEvent<TK, TV> ReadEvent<TK, TV>(IBinaryStream stream, 
            Marshaller marsh, bool keepBinary)
        {
            var reader = marsh.StartUnmarshal(stream, keepBinary);

            return ReadEvent0<TK, TV>(reader);
        }

        /// <summary>
        /// Read multiple events.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <returns>Events.</returns>
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        public static ICacheEntryEvent<TK, TV>[] ReadEvents<TK, TV>(IBinaryStream stream,
            Marshaller marsh, bool keepBinary)
        {
            var reader = marsh.StartUnmarshal(stream, keepBinary);

            int cnt = reader.ReadInt();

            ICacheEntryEvent<TK, TV>[] evts = new ICacheEntryEvent<TK, TV>[cnt];

            for (int i = 0; i < cnt; i++)
                evts[i] = ReadEvent0<TK, TV>(reader);

            return evts;
        }

        /// <summary>
        /// Read event.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Event.</returns>
        private static ICacheEntryEvent<TK, TV> ReadEvent0<TK, TV>(BinaryReader reader)
        {
            var key = reader.DetachNext().ReadObject<TK>();

            // Read as objects: TV may be value type
            TV oldVal, val;

            var hasOldVal = reader.DetachNext().TryDeserialize(out oldVal);
            var hasVal = reader.DetachNext().TryDeserialize(out val);

            Debug.Assert(hasVal || hasOldVal);

            var eventType = reader.ReadByte();
            switch (eventType)
            {
                case 0:
                    return new CacheEntryCreateEvent<TK, TV>(key, val);
                case 1:
                    return new CacheEntryUpdateEvent<TK, TV>(key, oldVal, val);
                case 2:
                    return new CacheEntryRemoveEvent<TK, TV>(key, oldVal);
                default:
                    throw new NotSupportedException(eventType.ToString());
            }
        }
    }
}

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

namespace Apache.Ignite.Core.Events
{
    using System;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Event reader.
    /// </summary>
    internal static class EventReader
    {
        /// <summary>
        /// Reads an event.
        /// </summary>
        /// <typeparam name="T">Type of the event</typeparam>
        /// <param name="reader">Reader.</param>
        /// <returns>Deserialized event.</returns>
        /// <exception cref="System.InvalidCastException">Incompatible event type.</exception>
        public static T Read<T>(IPortableReader reader) where T : IEvent
        {
            var r = reader.RawReader();

            var clsId = r.ReadInt();

            if (clsId == -1)
                return default(T);

            return (T) CreateInstance(clsId, r);
        }

        /// <summary>
        /// Creates an event instance by type id.
        /// </summary>
        /// <param name="clsId">Type id.</param>
        /// <param name="reader">Reader.</param>
        /// <returns>Created and deserialized instance.</returns>
        /// <exception cref="System.InvalidOperationException">Invalid event class id:  + clsId</exception>
        private static IEvent CreateInstance(int clsId, IPortableRawReader reader)
        {
            switch (clsId)
            {
                case 2: return new CacheEvent(reader);
                case 3: return new CacheQueryExecutedEvent(reader);
                case 4: return new CacheQueryReadEvent(reader);
                case 5: return new CacheRebalancingEvent(reader);
                case 6: return new CheckpointEvent(reader);
                case 7: return new DiscoveryEvent(reader);
                case 8: return new JobEvent(reader);
                case 9: return new SwapSpaceEvent(reader);
                case 10: return new TaskEvent(reader);
            }

            throw new InvalidOperationException("Invalid event class id: " + clsId);
        }
    }
}
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

namespace Apache.Ignite.Core.Impl.Cache.Event
{
    using Apache.Ignite.Core.Cache.Event;

    /// <summary>
    /// Cache entry create event.
    /// </summary>
    internal class CacheEntryCreateEvent<K, V> : ICacheEntryEvent<K, V>
    {
        /** Key.*/
        private readonly K key;

        /** Value.*/
        private readonly V val;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public CacheEntryCreateEvent(K key, V val)
        {
            this.key = key;
            this.val = val;
        }

        /** <inheritdoc /> */
        public K Key
        {
            get { return key; }
        }

        /** <inheritdoc /> */
        public V Value
        {
            get { return val; }
        }

        /** <inheritdoc /> */
        public V OldValue
        {
            get { return default(V); }
        }

        /** <inheritdoc /> */
        public bool HasOldValue
        {
            get { return false; }
        }

        /** <inheritdoc /> */
        public CacheEntryEventType EventType
        {
            get { return CacheEntryEventType.CREATED; }
        }
    }
}

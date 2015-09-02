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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache enumerable.
    /// </summary>
    internal class CacheEnumerable<K, V> : IEnumerable<ICacheEntry<K, V>>
    {
        /** Target cache. */
        private readonly CacheImpl<K, V> cache;

        /** Local flag. */
        private readonly bool loc;

        /** Peek modes. */
        private readonly int peekModes;

        /// <summary>
        /// Constructor for distributed iterator.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        public CacheEnumerable(CacheImpl<K, V> cache) : this(cache, false, 0)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor for local iterator.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        /// <param name="peekModes">Peek modes.</param>
        public CacheEnumerable(CacheImpl<K, V> cache, int peekModes) : this(cache, true, peekModes)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        /// <param name="loc">Local flag.</param>
        /// <param name="peekModes">Peek modes.</param>
        private CacheEnumerable(CacheImpl<K, V> cache, bool loc, int peekModes)
        {
            this.cache = cache;
            this.loc = loc;
            this.peekModes = peekModes;
        }

        /** <inheritdoc /> */
        public IEnumerator<ICacheEntry<K, V>> GetEnumerator()
        {
            return new CacheEnumeratorProxy<K, V>(cache, loc, peekModes);
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}

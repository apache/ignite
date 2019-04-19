/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    internal class CacheEnumerable<TK, TV> : IEnumerable<ICacheEntry<TK, TV>>
    {
        /** Target cache. */
        private readonly CacheImpl<TK, TV> _cache;

        /** Local flag. */
        private readonly bool _loc;

        /** Peek modes. */
        private readonly int _peekModes;

        /// <summary>
        /// Constructor for distributed iterator.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        public CacheEnumerable(CacheImpl<TK, TV> cache) : this(cache, false, 0)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor for local iterator.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        /// <param name="peekModes">Peek modes.</param>
        public CacheEnumerable(CacheImpl<TK, TV> cache, int peekModes) : this(cache, true, peekModes)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        /// <param name="loc">Local flag.</param>
        /// <param name="peekModes">Peek modes.</param>
        private CacheEnumerable(CacheImpl<TK, TV> cache, bool loc, int peekModes)
        {
            _cache = cache;
            _loc = loc;
            _peekModes = peekModes;
        }

        /** <inheritdoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            return new CacheEnumeratorProxy<TK, TV>(_cache, _loc, _peekModes);
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}

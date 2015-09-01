/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache
{
    using System.Collections;
    using System.Collections.Generic;

    using GridGain.Cache;

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

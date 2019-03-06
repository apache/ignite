/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;

    /// <summary>
    /// Holds WithExpiry caches per expiration interval to avoid garbage on frequent WithExpiry calls.
    /// </summary>
    internal class ExpiryCacheHolder<TK, TV>
    {
        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** */
        private readonly ICache<TK, TV> _cache;

        /** Cached caches per expiry seconds. */
        private volatile Dictionary<long, ICache<TK, TV>> _expiryCaches =
            new Dictionary<long, ICache<TK, TV>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="ExpiryCacheHolder{TK, TV}"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public ExpiryCacheHolder(ICache<TK, TV> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        public ICache<TK, TV> Cache
        {
            get { return _cache; }
        }

        /// <summary>
        /// Gets the cache with expiry policy according to provided expiration date.
        /// </summary>
        /// <param name="utcExpiry">The UTC expiry.</param>
        /// <returns>Cache with expiry policy.</returns>
        public ICache<TK, TV> GetCacheWithExpiry(DateTime utcExpiry)
        {
            if (utcExpiry == DateTime.MaxValue)
                return _cache;

            Debug.Assert(utcExpiry.Kind == DateTimeKind.Utc);

            // Round up to seconds ([OutputCache] duration is in seconds).
            var expirySeconds = (long)Math.Round((utcExpiry - DateTime.UtcNow).TotalSeconds);

            if (expirySeconds < 0)
                expirySeconds = 0;

            return GetCacheWithExpiry(expirySeconds);
        }

        /// <summary>
        /// Gets the cache with expiry.
        /// </summary>
        /// <param name="expiry">The expiration interval (in seconds).</param>
        public ICache<TK, TV> GetCacheWithExpiry(long expiry)
        {
            ICache<TK, TV> expiryCache;

            if (_expiryCaches.TryGetValue(expiry, out expiryCache))
                return expiryCache;

            lock (_syncRoot)
            {
                if (_expiryCaches.TryGetValue(expiry, out expiryCache))
                    return expiryCache;

                // Copy on write with size limit
                _expiryCaches = _expiryCaches.Count > MaxExpiryCaches
                    ? new Dictionary<long, ICache<TK, TV>>()
                    : new Dictionary<long, ICache<TK, TV>>(_expiryCaches);

                expiryCache = Cache.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromSeconds(expiry), null, null));

                _expiryCaches[expiry] = expiryCache;

                return expiryCache;
            }
        }
    }
}

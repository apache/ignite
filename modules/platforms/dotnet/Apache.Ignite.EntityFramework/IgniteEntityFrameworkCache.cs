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

namespace Apache.Ignite.EntityFramework
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;
    using EFCache;

    /// <summary>
    /// Ignite-base EntityFramework second-level cache.
    /// </summary>
    public class IgniteEntityFrameworkCache : ICache
    {
        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** Cache. */
        private readonly ICache<string, object> _cache;

        /** Cached caches per expiry seconds. */
        private volatile Dictionary<double, ICache<string, object>> _expiryCaches =
            new Dictionary<double, ICache<string, object>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteEntityFrameworkCache"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public IgniteEntityFrameworkCache(ICache<string, object> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            var atomicityMode = cache.GetConfiguration().AtomicityMode;
            IgniteArgumentCheck.Ensure(atomicityMode == CacheAtomicityMode.Transactional, "cache",
                string.Format("{0} requires {1} cache. Specified '{2}' cache is {3}.",
                    GetType(), CacheAtomicityMode.Transactional, cache.Name, atomicityMode));

            _cache = cache;
        }

        /** <inheritdoc /> */
        public bool GetItem(string key, out object value)
        {
            return _cache.TryGet(key, out value);
        }

        /** <inheritdoc /> */
        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, 
            TimeSpan slidingExpiration, DateTimeOffset absoluteExpiration)
        {
            var cache = GetCacheWithExpiry(slidingExpiration, absoluteExpiration);

            using (var tx = TxStart())
            {
                // Put the value
                cache.Put(key, value);

                // Update the entitySet -> key[] mapping
                foreach (var entitySet in dependentEntitySets)
                {
                    object existingKeys;
                    if (cache.TryGet(entitySet, out existingKeys))
                    {
                        // Existing entity set mapping - append new dependent key
                        var keysArr = (object[]) existingKeys;
                        var keys = new string[keysArr.Length + 1];
                        keys[0] = key;
                        Array.Copy(keysArr, 0, keys, 1, keysArr.Length);
                        cache.Put(entitySet, keys);
                    }
                    else
                    {
                        cache.Put(entitySet, new[] {key});
                    }
                }

                tx.Commit();
            }
        }

        private ITransaction TxStart()
        {
            return _cache.Ignite.GetTransactions()
                .TxStart(TransactionConcurrency.Optimistic, TransactionIsolation.RepeatableRead);
        }

        /** <inheritdoc /> */
        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            using (var tx = TxStart())
            {
                var sets = _cache.GetAll(entitySets);

                foreach (var dependentKeys in sets)
                {
                    // TODO: OfType? Wtf?
                    _cache.RemoveAll(((object[]) dependentKeys.Value).OfType<string>());
                }

                _cache.RemoveAll(sets.Keys);

                tx.Commit();
            }
        }

        /** <inheritdoc /> */
        public void InvalidateItem(string key)
        {
            _cache.Remove(key);
        }

        /// <summary>
        /// Gets the cache with expiry policy according to provided expiration date.
        /// </summary>
        /// <returns>Cache with expiry policy.</returns>
        // ReSharper disable once UnusedParameter.Local
        private ICache<string, object> GetCacheWithExpiry(TimeSpan slidingExpiration, 
            DateTimeOffset absoluteExpiration)
        {
            if (slidingExpiration != TimeSpan.MaxValue)
            {
                // Sliding expiration requires that "touch" operations (like Get)
                // are performed on WithExpiryPolicy cache instance, which is not possible here
                throw new NotSupportedException(GetType() + " does not support sliding expiration.");
            }

            if (absoluteExpiration == DateTimeOffset.MaxValue)
                return _cache;

            // Round up to 0.1 of a second so that we share expiry caches
            var expirySeconds = GetSeconds(absoluteExpiration);

            ICache<string, object> expiryCache;

            if (_expiryCaches.TryGetValue(expirySeconds, out expiryCache))
                return expiryCache;

            lock (_syncRoot)
            {
                if (_expiryCaches.TryGetValue(expirySeconds, out expiryCache))
                    return expiryCache;

                // Copy on write with size limit
                _expiryCaches = _expiryCaches.Count > MaxExpiryCaches
                    ? new Dictionary<double, ICache<string, object>>()
                    : new Dictionary<double, ICache<string, object>>(_expiryCaches);

                expiryCache =
                    _cache.WithExpiryPolicy(GetExpiryPolicy(expirySeconds));

                _expiryCaches[expirySeconds] = expiryCache;

                return expiryCache;
            }
        }

        /// <summary>
        /// Gets the expiry policy.
        /// </summary>
        private static ExpiryPolicy GetExpiryPolicy(double absoluteSeconds)
        {
            var absolute = !double.IsNaN(absoluteSeconds)
                ? TimeSpan.FromSeconds(absoluteSeconds)
                : (TimeSpan?) null;

            return new ExpiryPolicy(absolute, null, null);
        }

        /// <summary>
        /// Gets the seconds.
        /// </summary>
        private static double GetSeconds(DateTimeOffset ts)
        {
            var seconds = ts == DateTimeOffset.MaxValue ? double.NaN : (ts - DateTimeOffset.Now).TotalSeconds;

            if (seconds < 0)
                seconds = 0;

            return Math.Round(seconds, 1);
        }
    }
}

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
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
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

        /** Main cache: stores SQL -> QueryResult mappings. */
        private readonly ICache<string, object> _cache;

        /** 
         * Dependency cache: stores EntitySet -> SQL[] mappings. 
         * Each query uses one or more EntitySets (SQL tables). This cache tracks which queries should be 
         * removed from cache when specific entity set (table) gets updated.
         */
        private readonly ICache<string, string[]> _dependencyCache;

        /** Cached caches per (expiry_seconds * 10). */
        private volatile Dictionary<long, ICache<string, object>> _expiryCaches =
            new Dictionary<long, ICache<string, object>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteEntityFrameworkCache"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        [SuppressMessage("Microsoft.Globalization", "CA1303:Do not pass literals as localized parameters")]
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            Justification = "Validation is present")]
        public IgniteEntityFrameworkCache(ICache<string, object> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            var atomicityMode = cache.GetConfiguration().AtomicityMode;
            IgniteArgumentCheck.Ensure(atomicityMode == CacheAtomicityMode.Transactional, "cache",
                string.Format(CultureInfo.InvariantCulture, "{0} requires {1} cache. Specified '{2}' cache is {3}.",
                    GetType(), CacheAtomicityMode.Transactional, cache.Name, atomicityMode));

            _cache = cache;
            _dependencyCache = cache.Ignite.GetCache<string, string[]>(cache.Name);  // same cache with different types
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
            if (dependentEntitySets == null)
                return;

            var cache = GetCacheWithExpiry(slidingExpiration, absoluteExpiration);

            using (var tx = TxStart())
            {
                // Put the value
                cache.Put(key, value);

                // Update the entitySet -> key[] mapping
                // With a finite set of cached queries, we will reach a point when all dependencies are populated,
                // so _dependencyCache won't be updated on PutItem any more.
                foreach (var entitySet in Sort(dependentEntitySets))
                {
                    string[] keys;

                    if (!_dependencyCache.TryGet(entitySet, out keys))
                    {
                        _dependencyCache.Put(entitySet, new[] {key});
                    }
                    else if (!keys.Contains(entitySet))
                    {
                        _dependencyCache.Put(entitySet, Append(keys, key));
                    }
                }

                tx.Commit();
            }
        }

        /** <inheritdoc /> */
        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            using (var tx = TxStart())
            {
                var sets = _dependencyCache.GetAll(Sort(entitySets));

                // Remove all cached queries that depend on specific entity set
                foreach (var dependentKeys in sets)
                    _cache.RemoveAll(dependentKeys.Value);

                // Do not remove dependency information: same query always depends on same entity sets

                tx.Commit();
            }
        }

        /// <summary>
        /// Sorts the strings.
        /// </summary>
        private static List<string> Sort(IEnumerable<string> strings)
        {
            var res = strings.ToList();

            res.Sort();

            return res;
        }

        /** <inheritdoc /> */
        public void InvalidateItem(string key)
        {
            _cache.Remove(key);
        }

        /// <summary>
        /// Appends element to array and returns resulting array.
        /// </summary>
        private static string[] Append(string[] array, string element)
        {
            var len = array.Length;
            var keys = new string[len + 1];
            Array.Copy(array, 0, keys, 0, len);
            keys[len] = element;
            return keys;
        }

        /// <summary>
        /// Starts the transaction
        /// </summary>
        /// <returns></returns>
        private ITransaction TxStart()
        {
            return _cache.Ignite.GetTransactions()
                .TxStart(TransactionConcurrency.Pessimistic, TransactionIsolation.RepeatableRead);
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
                    ? new Dictionary<long, ICache<string, object>>()
                    : new Dictionary<long, ICache<string, object>>(_expiryCaches);

                expiryCache =
                    _cache.WithExpiryPolicy(GetExpiryPolicy(expirySeconds));

                _expiryCaches[expirySeconds] = expiryCache;

                return expiryCache;
            }
        }

        /// <summary>
        /// Gets the expiry policy.
        /// </summary>
        private static ExpiryPolicy GetExpiryPolicy(long absoluteSeconds)
        {
            var absolute = absoluteSeconds != long.MaxValue
                ? TimeSpan.FromSeconds((double)absoluteSeconds / 10)
                : (TimeSpan?) null;

            return new ExpiryPolicy(absolute, null, null);
        }

        /// <summary>
        /// Gets the seconds.
        /// </summary>
        private static long GetSeconds(DateTimeOffset ts)
        {
            if (ts == DateTimeOffset.MaxValue)
                return long.MaxValue;

            var seconds = (ts - DateTimeOffset.Now).TotalSeconds;

            if (seconds < 0)
                seconds = 0;

            return (long) (seconds * 10);
        }
    }
}

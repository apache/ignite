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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Common;
    using EFCache;

    /// <summary>
    /// Ignite-base EntityFramework second-level cache.
    /// </summary>
    public class IgniteEntityFrameworkCache : ICache
    {
        /** Value field name. */
        private const string ValueField = "value";
        
        /** Entity sets field name. */
        private const string EntitySetsField = "entitySets";

        /** Binary type name. */
        private const string CacheEntryTypeName = "IgniteEntityFrameworkCacheEntry";

        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** Cache. */
        private readonly ICache<string, IBinaryObject> _cache;

        /** Binary API. */
        private readonly IBinary _binary;

        /** Cached caches per expiry seconds. */
        private volatile Dictionary<double, ICache<string, IBinaryObject>> _expiryCaches =
            new Dictionary<double, ICache<string, IBinaryObject>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteEntityFrameworkCache"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public IgniteEntityFrameworkCache(ICache<string, object> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            _cache = cache.WithKeepBinary<string, IBinaryObject>();
            _binary = _cache.Ignite.GetBinary();
        }

        /** <inheritdoc /> */
        public bool GetItem(string key, out object value)
        {
            IBinaryObject binVal;

            if (!_cache.TryGet(key, out binVal))
            {
                value = null;
                return false;
            }

            value = binVal.GetField<object>(ValueField);
            return true;
        }

        /** <inheritdoc /> */
        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, 
            TimeSpan slidingExpiration, DateTimeOffset absoluteExpiration)
        {
            var cache = GetCacheWithExpiry(slidingExpiration, absoluteExpiration);

            var binVal = _binary.GetBuilder(CacheEntryTypeName)
                .SetField(ValueField, value)
                .SetField(EntitySetsField, dependentEntitySets.ToArray())
                .Build();

            cache.Put(key, binVal);
        }

        /** <inheritdoc /> */
        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            var invalidSets = new HashSet<string>(entitySets);

            // TODO: IGNITE-2546 or IGNITE-3222
            // TODO: ScanQuery, Compute, or even Java implementation?
            // TODO: Or store entity set<->keys mapping separately?
            foreach (var entry in _cache)
            {
                var cachedSets = entry.Value.GetField<string[]>(EntitySetsField);

                foreach (var cachedSet in cachedSets)
                {
                    if (invalidSets.Contains(cachedSet))
                        _cache.Remove(entry.Key);
                }
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
        private ICache<string, IBinaryObject> GetCacheWithExpiry(TimeSpan slidingExpiration, 
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

            ICache<string, IBinaryObject> expiryCache;

            if (_expiryCaches.TryGetValue(expirySeconds, out expiryCache))
                return expiryCache;

            lock (_syncRoot)
            {
                if (_expiryCaches.TryGetValue(expirySeconds, out expiryCache))
                    return expiryCache;

                // Copy on write with size limit
                _expiryCaches = _expiryCaches.Count > MaxExpiryCaches
                    ? new Dictionary<double, ICache<string, IBinaryObject>>()
                    : new Dictionary<double, ICache<string, IBinaryObject>>(_expiryCaches);

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

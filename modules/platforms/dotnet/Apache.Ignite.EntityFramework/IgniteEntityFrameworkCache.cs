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
        private volatile Dictionary<KeyValuePair<long, long>, ICache<string, IBinaryObject>> _expiryCaches =
            new Dictionary<KeyValuePair<long, long>, ICache<string, IBinaryObject>>();

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
            var binVal = _binary.GetBuilder(CacheEntryTypeName)
                .SetField(ValueField, value)
                .SetField(EntitySetsField, dependentEntitySets.ToArray())
                .Build();

            GetCacheWithExpiry(slidingExpiration, absoluteExpiration).Put(key, binVal);
        }

        /** <inheritdoc /> */
        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            var invalidSets = new HashSet<string>(entitySets);

            // TODO: IGNITE-2546
            // TODO: ScanQuery, Compute, or even Java implementation?
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
        private ICache<string, IBinaryObject> GetCacheWithExpiry(TimeSpan slidingExpiration, 
            DateTimeOffset absoluteExpiration)
        {
            if (slidingExpiration == TimeSpan.MaxValue && absoluteExpiration == DateTimeOffset.MaxValue)
                return _cache;

            // Round up to seconds
            var absoluteExpirySeconds = absoluteExpiration == DateTimeOffset.MaxValue
                ? long.MaxValue
                : (long) (absoluteExpiration - DateTime.UtcNow).TotalSeconds;

            if (absoluteExpirySeconds < 1)
                absoluteExpirySeconds = 0;

            var slidingExpirySeconds = slidingExpiration == TimeSpan.MaxValue
                ? long.MaxValue
                : (long) slidingExpiration.TotalSeconds;

            if (slidingExpirySeconds < 1)
                slidingExpirySeconds = 0;

            var key = new KeyValuePair<long, long>(absoluteExpirySeconds, slidingExpirySeconds);

            ICache<string, IBinaryObject> expiryCache;

            if (_expiryCaches.TryGetValue(key, out expiryCache))
                return expiryCache;

            lock (_syncRoot)
            {
                if (_expiryCaches.TryGetValue(key, out expiryCache))
                    return expiryCache;

                // Copy on write with size limit
                _expiryCaches = _expiryCaches.Count > MaxExpiryCaches
                    ? new Dictionary<KeyValuePair<long, long>, ICache<string, IBinaryObject>>()
                    : new Dictionary<KeyValuePair<long, long>, ICache<string, IBinaryObject>>(_expiryCaches);

                expiryCache =
                    _cache.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromSeconds(absoluteExpirySeconds),
                        TimeSpan.FromSeconds(slidingExpirySeconds), TimeSpan.FromSeconds(slidingExpirySeconds)));

                _expiryCaches[key] = expiryCache;

                return expiryCache;
            }
        }
    }
}

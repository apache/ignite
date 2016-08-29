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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Read-write cache with strict concurrency control.
    /// </summary>
    internal class StrictReadWriteCache : IDbCache
    {
        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** Main cache: stores SQL -> QueryResult mappings. */
        private readonly ICache<string, object> _cache;

        /** Entity set version cache. */
        private readonly ICache<string, long> _entitySetVersions;

        /** Cached caches per (expiry_seconds * 10). */
        private volatile Dictionary<long, ICache<string, object>> _expiryCaches =
            new Dictionary<long, ICache<string, object>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="StrictReadWriteCache"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        [SuppressMessage("Microsoft.Globalization", "CA1303:Do not pass literals as localized parameters")]
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            Justification = "Validation is present")]
        public StrictReadWriteCache(ICache<string, object> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            _cache = cache;
            _entitySetVersions = cache.Ignite.GetCache<string, long>(cache.Name);  // same cache with different types
        }

        /** <inheritdoc /> */
        public bool GetItem(string key, ICollection<EntitySetBase> dependentEntitySets, out object value)
        {
            var verKey = GetVersionedKey(key, dependentEntitySets);

            return _cache.TryGet(verKey, out value);
        }

        /** <inheritdoc /> */
        public void PutItem(string key, object value, ICollection<EntitySetBase> dependentEntitySets, 
            TimeSpan absoluteExpiration)
        {
            if (dependentEntitySets == null)
                return;

            var verKey = GetVersionedKey(key, dependentEntitySets);

            var cache = GetCacheWithExpiry(absoluteExpiration);

            cache[verKey] = value;
        }

        /** <inheritdoc /> */
        public void InvalidateSets(ICollection<EntitySetBase> entitySets)
        {
            // Increase version for each dependent entity set.
            _entitySetVersions.InvokeAll(entitySets.Select(x => x.Name), new AddOneProcessor(), null);

            // TODO: Use a background worker to purge outdated keys.
            // This worker should be able to work on any kind of node -> Java worker.
            // We'll need a special cache to share started EF cache names.
        }

        /// <summary>
        /// Gets the cache with expiry policy according to provided expiration date.
        /// </summary>
        /// <returns>Cache with expiry policy.</returns>
        // ReSharper disable once UnusedParameter.Local
        private ICache<string, object> GetCacheWithExpiry(TimeSpan absoluteExpiration)
        {
            if (absoluteExpiration == TimeSpan.MaxValue)
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
        private static long GetSeconds(TimeSpan ts)
        {
            if (ts == TimeSpan.MaxValue)
                return long.MaxValue;

            var seconds = ts.TotalSeconds;

            if (seconds < 0)
                seconds = 0;

            return (long) (seconds * 10);
        }

        /// <summary>
        /// Gets the versioned key.
        /// </summary>
        private string GetVersionedKey(string key, IEnumerable<EntitySetBase> sets)
        {
            // LINQ Select allocates less that a new List<> will do.
            var versions = _entitySetVersions.GetAll(sets.Select(x => x.Name));

            var sb = new StringBuilder(key);

            foreach (var ver in versions.Values)
                sb.AppendFormat("_{0}", ver);

            return sb.ToString();
        }

        /// <summary>
        /// TODO: Replace with a Java processor.
        /// </summary>
        [Serializable]
        private class AddOneProcessor : ICacheEntryProcessor<string, long, object, object>
        {
            public object Process(IMutableCacheEntry<string, long> entry, object arg)
            {
                entry.Value++;

                return null;
            }
        }
    }
}

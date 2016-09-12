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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.EntityFramework;

    /// <summary>
    /// Read-write cache with strict concurrency control.
    /// </summary>
    internal class StrictReadWriteCache : IDbCache
    {
        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** Java task to purge old entries. */
        public const string PurgeCacheTask =
            "org.apache.ignite.internal.processors.entityframework.PlatformDotNetEntityFrameworkPurgeOldEntriesTask";

        /** Main cache: stores SQL -> QueryResult mappings. */
        private readonly ICache<string, EntityFrameworkCacheEntry> _cache;

        /** Entity set version cache. */
        private readonly ICache<string, long> _entitySetVersions;

        /** Cached caches per (expiry_seconds * 10). */
        private volatile Dictionary<long, ICache<string, EntityFrameworkCacheEntry>> _expiryCaches =
            new Dictionary<long, ICache<string, EntityFrameworkCacheEntry>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="StrictReadWriteCache"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        [SuppressMessage("Microsoft.Globalization", "CA1303:Do not pass literals as localized parameters")]
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            Justification = "Validation is present")]
        public StrictReadWriteCache(ICache<string, EntityFrameworkCacheEntry> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            _cache = cache;
            _entitySetVersions = cache.Ignite.GetCache<string, long>(cache.Name);  // same cache with different types
        }

        /** <inheritdoc /> */
        public bool GetItem(string key, ICollection<EntitySetBase> dependentEntitySets, out object value)
        {
            var verKey = GetVersionedKey(key, dependentEntitySets);

            EntityFrameworkCacheEntry res;

            var success = _cache.TryGet(verKey, out res);

            value = success ? res.Data : null;

            return success;
        }

        /** <inheritdoc /> */
        public void PutItem(string key, object value, ICollection<EntitySetBase> dependentEntitySets, 
            TimeSpan absoluteExpiration)
        {
            if (dependentEntitySets == null)
                return;

            var entitySetVersions = GetEntitySetVersions(dependentEntitySets);

            var verKey = GetVersionedKey(key, entitySetVersions);

            var cache = GetCacheWithExpiry(absoluteExpiration);

            cache[verKey] = new EntityFrameworkCacheEntry(value, entitySetVersions);
        }

        /** <inheritdoc /> */
        public void InvalidateSets(ICollection<EntitySetBase> entitySets)
        {
            // Increase version for each dependent entity set.
            _entitySetVersions.InvokeAll(entitySets.Select(x => x.Name), new AddOneProcessor(), null);

            // Asynchronously purge old cache entries.
            var arg = new string[entitySets.Count + 1];

            arg[0] = _cache.Name;

            var i = 1;
            foreach (var set in entitySets)
                arg[i++] = set.Name;

            _cache.Ignite.GetCompute().ExecuteJavaTaskAsync<object>(PurgeCacheTask, arg);
        }

        /// <summary>
        /// Gets the cache with expiry policy according to provided expiration date.
        /// </summary>
        /// <returns>Cache with expiry policy.</returns>
        // ReSharper disable once UnusedParameter.Local
        private ICache<string, EntityFrameworkCacheEntry> GetCacheWithExpiry(TimeSpan absoluteExpiration)
        {
            if (absoluteExpiration == TimeSpan.MaxValue)
                return _cache;

            // Round up to 0.1 of a second so that we share expiry caches
            var expirySeconds = GetSeconds(absoluteExpiration);

            ICache<string, EntityFrameworkCacheEntry> expiryCache;

            if (_expiryCaches.TryGetValue(expirySeconds, out expiryCache))
                return expiryCache;

            lock (_syncRoot)
            {
                if (_expiryCaches.TryGetValue(expirySeconds, out expiryCache))
                    return expiryCache;

                // Copy on write with size limit
                _expiryCaches = _expiryCaches.Count > MaxExpiryCaches
                    ? new Dictionary<long, ICache<string, EntityFrameworkCacheEntry>>()
                    : new Dictionary<long, ICache<string, EntityFrameworkCacheEntry>>(_expiryCaches);

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
        private string GetVersionedKey(string key, ICollection<EntitySetBase> sets)
        {
            return GetVersionedKey(key, GetEntitySetVersions(sets));
        }

        /// <summary>
        /// Gets the versioned key.
        /// </summary>
        private static string GetVersionedKey(string key, IDictionary<string, long> versions)
        {
            var sb = new StringBuilder(key);

            foreach (var ver in versions.Values)
                sb.AppendFormat("_{0}", ver);

            return sb.ToString();
        }

        /// <summary>
        /// Gets the entity set versions.
        /// </summary>
        private IDictionary<string, long> GetEntitySetVersions(ICollection<EntitySetBase> sets)
        {
            // LINQ Select allocates less that a new List<> will do.
            var versions = _entitySetVersions.GetAll(sets.Select(x => x.Name));

            // Some versions may be missing, fill up with 0.
            foreach (var set in sets)
            {
                if (!versions.ContainsKey(set.Name))
                    versions[set.Name] = 0;
            }

            Debug.Assert(sets.Count == versions.Count);

            return versions;
        }

        /// <summary>
        /// TODO: Replace with a Java processor.
        /// </summary>
        [Serializable]
        private class AddOneProcessor : ICacheEntryProcessor<string, long, object, object>
        {
            public object Process(IMutableCacheEntry<string, long> entry, object arg)
            {
                // This will create a value if it is missing.
                entry.Value++;

                return null;
            }
        }
    }
}

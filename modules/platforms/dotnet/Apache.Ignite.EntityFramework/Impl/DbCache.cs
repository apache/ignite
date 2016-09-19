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
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.EntityFramework;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Database query cache.
    /// </summary>
    internal class DbCache
    {
        /** Extension id.  */
        private const int ExtensionId = 1;

        /** Invalidate sets extension operation. */
        private const int OpInvalidateSets = 1;

        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** Main cache: stores SQL -> QueryResult mappings. */
        private readonly ICache<string, EntityFrameworkCacheEntry> _cache;

        /** Entity set version cache. */
        private readonly ICache<string, long> _metaCache;

        /** Cached caches per (expiry_seconds * 10). */
        private volatile Dictionary<long, ICache<string, EntityFrameworkCacheEntry>> _expiryCaches =
            new Dictionary<long, ICache<string, EntityFrameworkCacheEntry>>();

        /** Sync object. */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="DbCache" /> class.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="metaCacheConfiguration">The meta cache configuration.</param>
        /// <param name="dataCacheConfiguration">The data cache configuration.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            Justification = "Validation is present")]
        public DbCache(IIgnite ignite, CacheConfiguration metaCacheConfiguration, 
            CacheConfiguration dataCacheConfiguration)
        {
            IgniteArgumentCheck.NotNull(ignite, "ignite");
            IgniteArgumentCheck.NotNull(metaCacheConfiguration, "metaCacheConfiguration");
            IgniteArgumentCheck.NotNull(dataCacheConfiguration, "metaCacheConfiguration");

            IgniteArgumentCheck.Ensure(metaCacheConfiguration.Name != dataCacheConfiguration.Name, 
                "dataCacheConfiguration", "Meta and Data cache can't have the same name.");

            if (metaCacheConfiguration.CacheMode == CacheMode.Partitioned && metaCacheConfiguration.Backups < 1)
                ignite.Logger.Warn("EntityFramework meta cache is partitioned and has no backups. " +
                                   "This can lead to data loss and incorrect query results.");

            _metaCache = ignite.GetOrCreateCache<string, long>(metaCacheConfiguration);
            _cache = ignite.GetOrCreateCache<string, EntityFrameworkCacheEntry>(dataCacheConfiguration);
        }

        /** <inheritdoc /> */
        public bool GetItem(string key, ICollection<EntitySetBase> dependentEntitySets, DbCachingStrategy strategy, 
            out object value)
        {
            if (strategy == DbCachingStrategy.ReadWrite)
                key = GetVersionedKey(key, dependentEntitySets);

            EntityFrameworkCacheEntry res;

            var success = _cache.TryGet(key, out res);

            value = success ? res.Data : null;

            return success;
        }

        /** <inheritdoc /> */
        public void PutItem(string key, object value, ICollection<EntitySetBase> dependentEntitySets,
            DbCachingStrategy strategy, TimeSpan absoluteExpiration)
        {
            switch (strategy)
            {
                case DbCachingStrategy.ReadWrite:
                {
                    if (dependentEntitySets == null)
                        return;

                    var entitySetVersions = GetEntitySetVersions(dependentEntitySets);

                    key = GetVersionedKey(key, entitySetVersions);

                    var cache = GetCacheWithExpiry(absoluteExpiration);

                    cache[key] = new EntityFrameworkCacheEntry(value, entitySetVersions);

                    return;
                }

                case DbCachingStrategy.ReadOnly:
                {
                    var cache = GetCacheWithExpiry(absoluteExpiration);

                    cache[key] = new EntityFrameworkCacheEntry(value, null);

                    return;
                }

                default:
                    throw new ArgumentOutOfRangeException("strategy", strategy, null);
            }
        }

        /** <inheritdoc /> */
        public void InvalidateSets(ICollection<EntitySetBase> entitySets)
        {
            Debug.Assert(entitySets != null && entitySets.Count > 0);

            // Increase version for each dependent entity set and run a task to clean up old entries.
            ((ICacheInternal) _metaCache).DoOutInOpExtension<object>(ExtensionId, OpInvalidateSets, w =>
            {
                // TODO: Send cache name to clear up.
                w.WriteInt(entitySets.Count);

                foreach (var set in entitySets)
                    w.WriteString(set.Name);
            }, null);
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
            var versions = _metaCache.GetAll(sets.Select(x => x.Name));

            // Some versions may be missing, fill up with 0.
            foreach (var set in sets)
            {
                if (!versions.ContainsKey(set.Name))
                    versions[set.Name] = 0;
            }

            Debug.Assert(sets.Count == versions.Count);

            return versions;
        }
    }
}

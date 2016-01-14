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

using System.Web.Caching;

namespace Apache.Ignite.AspNet
{
    using System;
    using System.Collections.Specialized;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;

    /// <summary>
    /// ASP.NET output cache provider that uses Ignite cache as a storage.
    /// </summary>
    public class IgniteOutputCacheProvider : OutputCacheProvider
    {
        // TODO: Tests (separate assembly?)

        /** */
        private const string GridName = "gridName";
        
        /** */
        private const string CacheName = "cacheName";

        /** */
        private ICache<string, object> _cache;

        /// <summary>
        /// Returns a reference to the specified entry in the output cache.
        /// </summary>
        /// <param name="key">A unique identifier for a cached entry in the output cache.</param>
        /// <returns>
        /// The <paramref name="key" /> value that identifies the specified entry in the cache, or null if the specified entry is not in the cache.
        /// </returns>
        public override object Get(string key)
        {
            return _cache[key];
        }

        /// <summary>
        /// Inserts the specified entry into the output cache.
        /// </summary>
        /// <param name="key">A unique identifier for <paramref name="entry" />.</param>
        /// <param name="entry">The content to add to the output cache.</param>
        /// <param name="utcExpiry">The time and date on which the cached entry expires.</param>
        /// <returns>
        /// A reference to the specified provider.
        /// </returns>
        public override object Add(string key, object entry, DateTime utcExpiry)
        {
            return GetCacheWithExpiration(utcExpiry).GetAndPutIfAbsent(key, entry);
        }

        /// <summary>
        /// Inserts the specified entry into the output cache, overwriting the entry if it is already cached.
        /// </summary>
        /// <param name="key">A unique identifier for <paramref name="entry" />.</param>
        /// <param name="entry">The content to add to the output cache.</param>
        /// <param name="utcExpiry">The time and date on which the cached <paramref name="entry" /> expires.</param>
        public override void Set(string key, object entry, DateTime utcExpiry)
        {
            GetCacheWithExpiration(utcExpiry)[key] = entry;
        }

        /// <summary>
        /// Removes the specified entry from the output cache.
        /// </summary>
        /// <param name="key">The unique identifier for the entry to remove from the output cache.</param>
        public override void Remove(string key)
        {
            _cache.Remove(key);
        }

        /// <summary>
        /// Initializes the provider.
        /// </summary>
        /// <param name="name">The friendly name of the provider.</param>
        /// <param name="config">A collection of the name/value pairs representing the provider-specific attributes specified in the configuration for this provider.</param>
        public override void Initialize(string name, NameValueCollection config)
        {
            base.Initialize(name, config);

            var gridName = config[GridName];
            var cacheName = config[CacheName];

            // TODO: GetOrStartIgnite, spring url?
            var grid = Ignition.GetIgnite(gridName);

            _cache = grid.GetOrCreateCache<string, object>(cacheName);
        }

        private ICache<string, object> GetCacheWithExpiration(DateTime utcExpiry)
        {
            if (utcExpiry == DateTime.MaxValue)
                return _cache;

            var expiration = utcExpiry - DateTime.UtcNow;

            // TODO: Cache caches with similar expiration? They are likely to be similar.
            return _cache.WithExpiryPolicy(new ExpiryPolicy(expiration, null, null));
        }
    }
}

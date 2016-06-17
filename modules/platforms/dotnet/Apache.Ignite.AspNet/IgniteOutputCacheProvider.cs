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

namespace Apache.Ignite.AspNet
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Configuration;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Web.Caching;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// ASP.NET output cache provider that uses Ignite cache as a storage.
    /// <para />
    /// You can either start Ignite yourself, and provide <c>gridName</c> attribute, 
    /// or provide <c>igniteConfigurationSectionName</c> attribute to start Ignite automatically from specified
    /// configuration section (see <see cref="IgniteConfigurationSection"/>).
    /// <para />
    /// <c>cacheName</c> attribute specifies Ignite cache name to use for data storage. This attribute can be omitted 
    /// if cache name is null.
    /// </summary>
    public class IgniteOutputCacheProvider : OutputCacheProvider
    {
        /** */
        private const string GridName = "gridName";
        
        /** */
        private const string CacheName = "cacheName";

        /** */
        private const string IgniteConfigurationSectionName = "igniteConfigurationSectionName";

        /** Max number of cached expiry caches. */
        private const int MaxExpiryCaches = 1000;

        /** */
        private volatile ICache<string, object> _cache;

        /** Cached caches per expiry seconds. */
        private volatile Dictionary<long, ICache<string, object>> _expiryCaches = 
            new Dictionary<long, ICache<string, object>>();

        /** Sync object. */ 
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Returns a reference to the specified entry in the output cache.
        /// </summary>
        /// <param name="key">A unique identifier for a cached entry in the output cache.</param>
        /// <returns>
        /// The <paramref name="key" /> value that identifies the specified entry in the cache, or null if the specified entry is not in the cache.
        /// </returns>
        public override object Get(string key)
        {
            object res;

            return Cache.TryGet(key, out res) ? res : null;
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
            return GetCacheWithExpiry(utcExpiry).GetAndPutIfAbsent(key, entry).Value;
        }

        /// <summary>
        /// Inserts the specified entry into the output cache, overwriting the entry if it is already cached.
        /// </summary>
        /// <param name="key">A unique identifier for <paramref name="entry" />.</param>
        /// <param name="entry">The content to add to the output cache.</param>
        /// <param name="utcExpiry">The time and date on which the cached <paramref name="entry" /> expires.</param>
        public override void Set(string key, object entry, DateTime utcExpiry)
        {
            GetCacheWithExpiry(utcExpiry)[key] = entry;
        }

        /// <summary>
        /// Removes the specified entry from the output cache.
        /// </summary>
        /// <param name="key">The unique identifier for the entry to remove from the output cache.</param>
        public override void Remove(string key)
        {
            Cache.Remove(key);
        }

        /// <summary>
        /// Initializes the provider.
        /// </summary>
        /// <param name="name">The friendly name of the provider.</param>
        /// <param name="config">A collection of the name/value pairs representing the provider-specific attributes specified in the configuration for this provider.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void Initialize(string name, NameValueCollection config)
        {
            base.Initialize(name, config);

            var gridName = config[GridName];
            var cacheName = config[CacheName];
            var cfgSection = config[IgniteConfigurationSectionName];

            try
            {
                var grid = cfgSection != null
                    ? StartFromApplicationConfiguration(cfgSection)
                    : Ignition.GetIgnite(gridName);

                _cache = grid.GetOrCreateCache<string, object>(cacheName);
            }
            catch (Exception ex)
            {
                throw new IgniteException(string.Format(CultureInfo.InvariantCulture,
                    "Failed to initialize {0}: {1}", GetType(), ex), ex);
            }
        }

        /// <summary>
        /// Starts Ignite from application configuration.
        /// </summary>
        private static IIgnite StartFromApplicationConfiguration(string sectionName)
        {
            var section = ConfigurationManager.GetSection(sectionName) as IgniteConfigurationSection;

            if (section == null)
                throw new ConfigurationErrorsException(string.Format(CultureInfo.InvariantCulture, 
                    "Could not find {0} with name '{1}'", typeof(IgniteConfigurationSection).Name, sectionName));

            var config = section.IgniteConfiguration;

            if (string.IsNullOrWhiteSpace(config.IgniteHome))
            {
                // IgniteHome not set by user: populate from default directory
                config = new IgniteConfiguration(config) {IgniteHome = IgniteWebUtils.GetWebIgniteHome()};
            }

            return Ignition.Start(config);
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<string, object> Cache
        {
            get
            {
                var cache = _cache;

                if (cache == null)
                    throw new InvalidOperationException("IgniteOutputCacheProvider has not been initialized.");

                return cache;
            }
        }

        /// <summary>
        /// Gets the cache with expiry policy according to provided expiration date.
        /// </summary>
        /// <param name="utcExpiry">The UTC expiry.</param>
        /// <returns>Cache with expiry policy.</returns>
        private ICache<string, object> GetCacheWithExpiry(DateTime utcExpiry)
        {
            if (utcExpiry == DateTime.MaxValue)
                return Cache;

            // Round up to seconds ([OutputCache] duration is in seconds).
            var expirySeconds = (long) Math.Round((utcExpiry - DateTime.UtcNow).TotalSeconds);

            if (expirySeconds < 0)
                expirySeconds = 0;

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

                expiryCache = Cache.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromSeconds(expirySeconds), null, null));

                _expiryCaches[expirySeconds] = expiryCache;

                return expiryCache;
            }
        }
    }
}

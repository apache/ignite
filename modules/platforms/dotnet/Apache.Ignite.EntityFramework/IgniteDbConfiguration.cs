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
    using System.Configuration;
    using System.Data.Entity;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.EntityFramework.Impl;

    /// <summary>
    /// <see cref="DbConfiguration"/> implementation that uses Ignite as a second-level cache 
    /// for Entity Framework queries.
    /// </summary>
    public class IgniteDbConfiguration : DbConfiguration
    {
        /// <summary>
        /// The configuration section name to be used when starting Ignite.
        /// </summary>
        public const string ConfigurationSectionName = "igniteConfiguration";

        /// <summary>
        /// The default cache name to be used for cached EF data.
        /// </summary>
        public const string DefaultCacheName = "entityFrameworkQueryCache";

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteDbConfiguration"/> class.
        /// <para />
        /// This constructor uses default Ignite instance (with null <see cref="IgniteConfiguration.GridName"/>) 
        /// and a cache with <see cref="DefaultCacheName"/> name.
        /// <para />
        /// Ignite instance will be started automatically, if it is not started yet.
        /// <para /> 
        /// <see cref="IgniteConfigurationSection"/> with name <see cref="ConfigurationSectionName"/> will be picked up 
        /// when starting Ignite, if present.
        /// </summary>
        public IgniteDbConfiguration() 
            : this(GetConfiguration(ConfigurationSectionName, false), DefaultCacheName, null)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteDbConfiguration"/> class.
        /// </summary>
        /// <param name="configurationSectionName">Name of the configuration section.</param>
        /// <param name="cacheName">Name of the cache.</param>
        /// <param name="policy">The caching policy. Null for default <see cref="IgniteEntityFrameworkCachingPolicy"/>.</param>
        [CLSCompliant(false)]
        public IgniteDbConfiguration(string configurationSectionName, string cacheName, IgniteEntityFrameworkCachingPolicy policy)
             : this(GetConfiguration(configurationSectionName, true), cacheName, policy)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteDbConfiguration" /> class.
        /// </summary>
        /// <param name="igniteConfiguration">The ignite configuration to use for starting Ignite instance.</param>
        /// <param name="cacheName">Name of the cache. Can be null. Cache will be created if it does not exist.</param>
        /// <param name="policy">The caching policy. Null for default <see cref="IgniteEntityFrameworkCachingPolicy"/>.</param>
        [CLSCompliant(false)]
        public IgniteDbConfiguration(IgniteConfiguration igniteConfiguration, string cacheName, 
            IgniteEntityFrameworkCachingPolicy policy)
            : this(GetOrStartIgnite(igniteConfiguration), cacheName, policy)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteDbConfiguration" /> class.
        /// </summary>
        /// <param name="ignite">The ignite instance to use.</param>
        /// <param name="cacheName">Name of the cache. Can be null. Cache will be created if it does not exist.</param>
        /// <param name="policy">The caching policy. Null for default <see cref="IgniteEntityFrameworkCachingPolicy"/>.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", 
            Justification = "Validation is present")]
        [CLSCompliant(false)]
        public IgniteDbConfiguration(IIgnite ignite, string cacheName, IgniteEntityFrameworkCachingPolicy policy)
        {
            IgniteArgumentCheck.NotNull(ignite, "ignite");

            var cache = ignite.GetOrCreateCache<string, object>(new CacheConfiguration(cacheName)
            {
                AtomicityMode = CacheAtomicityMode.Transactional
            });

            var efCache = new IgniteEntityFrameworkCache(cache);
            var transactionHandler = new TransactionInterceptor(efCache);

            AddInterceptor(transactionHandler);

            var providerSvc = new IgniteDbProviderServices(policy);
            SetProviderServices(providerSvc.GetType().FullName, providerSvc);
        }

        /// <summary>
        /// Gets the Ignite instance.
        /// </summary>
        private static IIgnite GetOrStartIgnite(IgniteConfiguration cfg)
        {
            cfg = cfg ?? new IgniteConfiguration();

            return Ignition.TryGetIgnite(cfg.GridName) ?? Ignition.Start(cfg);
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private static IgniteConfiguration GetConfiguration(string sectionName, bool throwIfAbsent)
        {
            IgniteArgumentCheck.NotNull(sectionName, "sectionName");

            var section = ConfigurationManager.GetSection(sectionName) as IgniteConfigurationSection;

            if (section != null)
                return section.IgniteConfiguration;

            if (!throwIfAbsent)
                return null;

            throw new IgniteException(string.Format(CultureInfo.InvariantCulture,
                "Failed to initialize {0}. Could not find {1} with name {2} in application configuration.",
                typeof (IgniteDbConfiguration), typeof (IgniteConfigurationSection), sectionName));
        }
    }
}
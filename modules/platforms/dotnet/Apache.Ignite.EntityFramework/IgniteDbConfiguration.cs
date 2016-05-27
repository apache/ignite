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
    using System.Configuration;
    using System.Data.Entity;
    using System.Data.Entity.Core.Common;
    using System.Globalization;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using EFCache;

    /// <summary>
    /// <see cref="DbConfiguration"/> implementation that uses Ignite as a second-level cache 
    /// for Entity Framework queries.
    /// </summary>
    public class IgniteDbConfiguration : DbConfiguration
    {
        // This class can be used directly
        // This class can be inherited
        // TODO: Can it be used in XML config?

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
        public IgniteDbConfiguration() : this(GetConfiguration(ConfigurationSectionName, false), DefaultCacheName)
        {
            // No-op.
        }

        public IgniteDbConfiguration(string configurationSectionName, string cacheName)
             : this(GetConfiguration(configurationSectionName, true), cacheName)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteDbConfiguration" /> class.
        /// </summary>
        /// <param name="igniteConfiguration">The ignite configuration to use for starting Ignite instance.</param>
        /// <param name="cacheName">Name of the cache. Can be null. Cache will be created if it does not exist.</param>
        public IgniteDbConfiguration(IgniteConfiguration igniteConfiguration, string cacheName)
            : this(GetOrStartIgnite(igniteConfiguration), cacheName)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteDbConfiguration"/> class.
        /// </summary>
        /// <param name="ignite">The ignite instance to use.</param>
        /// <param name="cacheName">Name of the cache. Can be null. Cache will be created if it does not exist.</param>
        public IgniteDbConfiguration(IIgnite ignite, string cacheName)
        {
            IgniteArgumentCheck.NotNull(ignite, "ignite");

            var cache = ignite.GetOrCreateCache<string, object>(cacheName);

            var efCache = new IgniteEntityFrameworkCache(cache);
            var transactionHandler = new CacheTransactionHandler(efCache);

            AddInterceptor(transactionHandler);

            var cachingPolicy = new CachingPolicy();

            Loaded +=
                (sender, args) => args.ReplaceService<DbProviderServices>(
                    (s, _) => new CachingProviderServices(s, transactionHandler,
                        cachingPolicy));
        }

        /// <summary>
        /// Gets the Ignite instance.
        /// </summary>
        private static IIgnite GetOrStartIgnite(IgniteConfiguration igniteConfiguration)
        {
            IgniteArgumentCheck.NotNull(igniteConfiguration, "igniteConfiguration");

            return Ignition.TryGetIgnite(igniteConfiguration.GridName) ?? Ignition.Start(igniteConfiguration);
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
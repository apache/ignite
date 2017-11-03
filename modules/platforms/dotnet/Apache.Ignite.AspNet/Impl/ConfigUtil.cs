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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Collections.Specialized;
    using System.Configuration;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Config utils.
    /// </summary>
    internal static class ConfigUtil
    {
        /** */
        private const string GridName = "gridName";

        /** */
        private const string CacheName = "cacheName";

        /** */
        private const string IgniteConfigurationSectionName = "igniteConfigurationSectionName";

        /// <summary>
        /// Initializes the cache from configuration.
        /// </summary>
        public static ICache<TK, TV> InitializeCache<TK, TV>(NameValueCollection config, Type callerType, 
            string defaultCacheName)
        {
            Debug.Assert(config != null);
            Debug.Assert(callerType != null);

            var gridName = config[GridName];
            var cacheName = config.AllKeys.Contains(CacheName) ? config[CacheName] : defaultCacheName;
            var cfgSection = config[IgniteConfigurationSectionName];

            try
            {
                var grid = StartFromApplicationConfiguration(cfgSection, gridName);

                var cacheConfiguration = new CacheConfiguration(cacheName);

                return grid.GetOrCreateCache<TK, TV>(cacheConfiguration);
            }
            catch (Exception ex)
            {
                throw new IgniteException(string.Format(CultureInfo.InvariantCulture,
                    "Failed to initialize {0}: {1}", callerType, ex), ex);
            }
        }

        /// <summary>
        /// Starts Ignite from application configuration.
        /// </summary>
        private static IIgnite StartFromApplicationConfiguration(string sectionName, string gridName)
        {
            IgniteConfiguration config;

            if (!string.IsNullOrEmpty(sectionName))
            {
                var section = ConfigurationManager.GetSection(sectionName) as IgniteConfigurationSection;

                if (section == null)
                    throw new ConfigurationErrorsException(string.Format(CultureInfo.InvariantCulture,
                        "Could not find {0} with name '{1}'", typeof(IgniteConfigurationSection).Name, sectionName));

                config = section.IgniteConfiguration;

                if (config == null)
                    throw new ConfigurationErrorsException(string.Format(CultureInfo.InvariantCulture,
                        "{0} with name '{1}' is defined in <configSections>, but not present in configuration.", 
                        typeof(IgniteConfigurationSection).Name, sectionName));
            }
            else
                config = new IgniteConfiguration {IgniteInstanceName = gridName};

            // Check if already started.
            var ignite = Ignition.TryGetIgnite(config.IgniteInstanceName);

            if (ignite != null)
                return ignite;

            // Start.
            if (string.IsNullOrWhiteSpace(config.IgniteHome))
            {
                // IgniteHome not set by user: populate from default directory.
                config = new IgniteConfiguration(config) { IgniteHome = IgniteWebUtils.GetWebIgniteHome() };
            }

            return Ignition.Start(config);
        }

    }
}

﻿/*
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

namespace Apache.Ignite.Core.Impl.Plugin
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Plugin context.
    /// </summary>
    internal class PluginContext : IPluginContext
    {
        /** */
        private readonly IgniteConfiguration _igniteConfiguration;

        /** */
        private readonly Dictionary<string, IPluginProvider> _pluginProviders;

        /** */
        private volatile IIgnite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginContext" /> class.
        /// </summary>
        /// <param name="igniteConfiguration">The ignite configuration.</param>
        /// <param name="log">The log.</param>
        public PluginContext(IgniteConfiguration igniteConfiguration, ILogger log)
        {
            Debug.Assert(igniteConfiguration != null);
            Debug.Assert(log != null);

            _igniteConfiguration = igniteConfiguration;
            _pluginProviders = LoadPlugins(igniteConfiguration.PluginConfigurations, log.GetLogger(GetType().Name));
        }

        /// <summary>
        /// Gets the Ignite.
        /// </summary>
        public IIgnite Ignite
        {
            get { return _ignite; }
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        public IgniteConfiguration IgniteConfiguration
        {
            get { return _igniteConfiguration; }
        }

        /// <summary>
        /// Called when Ignite has started.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        public void OnStart(IIgnite ignite)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;

            // Notify plugins.
            foreach (var provider in _pluginProviders.Values)
                provider.OnIgniteStart();
        }

        /// <summary>
        /// Called when Ignite is about to stop.
        /// </summary>
        public void Stop(bool cancel)
        {
            // Notify plugins.
            foreach (var provider in _pluginProviders.Values)
                provider.Stop(cancel);
        }

        /// <summary>
        /// Gets the provider.
        /// </summary>
        public IPluginProvider GetProvider(string name)
        {
            Debug.Assert(!string.IsNullOrEmpty(name));

            IPluginProvider provider;

            if (!_pluginProviders.TryGetValue(name, out provider))
                throw new PluginNotFoundException(
                    string.Format("Ignite plugin with name '{0}' not found. Make sure that containing assembly " +
                                  "is in '{1}' folder or configure IgniteConfiguration.PluginPaths.", 
                                  name, GetType().Assembly.Location));

            return provider;
        }

        /// <summary>
        /// Loads the plugins.
        /// </summary>
        private Dictionary<string, IPluginProvider> LoadPlugins(ICollection<IPluginConfiguration> pluginConfigurations, 
            ILogger log)
        {
            var res = new Dictionary<string, IPluginProvider>();

            log.Info("Configured .NET plugins:");

            if (pluginConfigurations != null && pluginConfigurations.Count > 0)
            {
                foreach (var cfg in pluginConfigurations)
                {
                    var provider = cfg.CreateProvider();

                    ValidateProvider(provider, res);

                    LogProviderInfo(log, provider);

                    provider.Start(this);

                    res[provider.Name] = provider;
                }
            }
            else
            {
                log.Info("  ^-- None");
            }

            return res;
        }

        /// <summary>
        /// Logs the provider information.
        /// </summary>
        private static void LogProviderInfo(ILogger log, IPluginProvider provider)
        {
            log.Info("  ^-- " + provider.Name + " " + provider.GetType().Assembly.GetName().Version);

            if (!string.IsNullOrEmpty(provider.Copyright))
                log.Info("  ^-- " + provider.Copyright);

            log.Info(string.Empty);
        }

        /// <summary>
        /// Validates the provider.
        /// </summary>
        private static void ValidateProvider(IPluginProvider provider, Dictionary<string, IPluginProvider> res)
        {
            if (provider == null)
            {
                throw new IgniteException(string.Format("{0}.CreateProvider can not return null",
                    typeof(IPluginConfiguration).Name));
            }

            if (string.IsNullOrEmpty(provider.Name))
            {
                throw new IgniteException(string.Format("{0}.Name should not be null or empty: {1}",
                    typeof(IPluginProvider), provider.GetType().AssemblyQualifiedName));
            }

            if (res.ContainsKey(provider.Name))
            {
                throw new IgniteException(string.Format("Duplicate plugin name '{0}' is used by " +
                                                        "plugin providers '{1}' and '{2}'", provider.Name,
                    provider.GetType().AssemblyQualifiedName,
                    res[provider.Name].GetType().AssemblyQualifiedName));
            }
        }
    }
}

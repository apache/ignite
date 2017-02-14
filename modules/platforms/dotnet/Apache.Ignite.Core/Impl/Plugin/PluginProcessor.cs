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

namespace Apache.Ignite.Core.Impl.Plugin
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Plugin processor.
    /// </summary>
    internal class PluginProcessor
    {
        /** Ordered list of plugin providers. */
        private readonly IList<IPluginProviderProxy> _pluginProviders = new List<IPluginProviderProxy>();

        /** Plugin providers by name. */
        private readonly Dictionary<string, IPluginProviderProxy> _pluginProvidersByName
            = new Dictionary<string, IPluginProviderProxy>();

        /** Plugin exception mappings. */
        private readonly Dictionary<string, ExceptionFactory> _exceptionMappings
            = new Dictionary<string, ExceptionFactory>();

        /** */
        private readonly Ignite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginProcessor"/> class.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        public PluginProcessor(Ignite ignite)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
        }

        /// <summary>
        /// Gets the Ignite.
        /// </summary>
        public Ignite Ignite
        {
            get { return _ignite; }
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        public IgniteConfiguration IgniteConfiguration
        {
            get { return _ignite.Configuration; }
        }

        /// <summary>
        /// Called when Ignite has started.
        /// </summary>
        public void OnIgniteStart()
        {
            LoadPlugins();

            // Notify plugins.
            foreach (var provider in _pluginProviders)
                provider.OnIgniteStart();
        }

        /// <summary>
        /// Stops the plugin processor.
        /// </summary>
        public void Stop(bool cancel)
        {
            foreach (var provider in _pluginProviders.Reverse())
                provider.Stop(cancel);
        }

        /// <summary>
        /// Called when Ignite is about to stop.
        /// </summary>
        public void OnIgniteStop(bool cancel)
        {
            foreach (var provider in _pluginProviders.Reverse())
                provider.OnIgniteStop(cancel);
        }

        /// <summary>
        /// Gets the provider.
        /// </summary>
        public IPluginProviderProxy GetProvider(string name)
        {
            Debug.Assert(!string.IsNullOrEmpty(name));

            IPluginProviderProxy provider;

            if (!_pluginProvidersByName.TryGetValue(name, out provider))
                throw new PluginNotFoundException(
                    string.Format("Ignite plugin with name '{0}' not found. Make sure that containing assembly " +
                                  "is in '{1}' folder or configure IgniteConfiguration.PluginPaths.", 
                                  name, GetType().Assembly.Location));

            return provider;
        }

        /// <summary>
        /// Gets the exception factory.
        /// </summary>
        public ExceptionFactory GetExceptionFactory(string className)
        {
            ExceptionFactory res;

            return _exceptionMappings.TryGetValue(className, out res) ? res : null;
        }

        /// <summary>
        /// Loads the plugins.
        /// </summary>
        private void LoadPlugins()
        {
            var log = _ignite.Logger.GetLogger(GetType().Name);

            log.Info("Configured .NET plugins:");

            var pluginConfigurations = IgniteConfiguration.PluginConfigurations;

            if (pluginConfigurations != null && pluginConfigurations.Count > 0)
            {
                foreach (var cfg in pluginConfigurations)
                {
                    var provider = CreateProviderProxy(cfg);

                    ValidateProvider(provider, _pluginProvidersByName);

                    LogProviderInfo(log, provider);

                    _pluginProviders.Add(provider);
                    _pluginProvidersByName[provider.Name] = provider;

                    AddExceptionMappings(provider);

                    provider.Start(this);
                }
            }
            else
            {
                log.Info("  ^-- None");
            }
        }

        /// <summary>
        /// Adds the exception mappings.
        /// </summary>
        private void AddExceptionMappings(IPluginProviderProxy provider)
        {
            var map = provider.GetExceptionMappings();

            if (map != null)
            {
                foreach (var pair in map)
                {
                    _exceptionMappings[pair.Key] = pair.Value;
                }
            }
        }

        /// <summary>
        /// Logs the provider information.
        /// </summary>
        private static void LogProviderInfo(ILogger log, IPluginProviderProxy provider)
        {
            log.Info("  ^-- " + provider.Name + " " + provider.GetType().Assembly.GetName().Version);

            if (!string.IsNullOrEmpty(provider.Copyright))
                log.Info("  ^-- " + provider.Copyright);

            log.Info(string.Empty);
        }

        /// <summary>
        /// Validates the provider.
        /// </summary>
        private static void ValidateProvider(IPluginProviderProxy provider, 
            Dictionary<string, IPluginProviderProxy> res)
        {
            if (provider == null)
            {
                throw new IgniteException(string.Format("{0}.CreateProvider can not return null",
                    typeof(IPluginConfiguration).Name));
            }

            if (string.IsNullOrEmpty(provider.Name))
            {
                throw new IgniteException(string.Format("{0}.Name should not be null or empty: {1}",
                    typeof(IPluginProvider<>), provider.Provider.GetType()));
            }

            if (res.ContainsKey(provider.Name))
            {
                throw new IgniteException(string.Format("Duplicate plugin name '{0}' is used by " +
                                                        "plugin providers '{1}' and '{2}'", provider.Name,
                    provider.Provider.GetType(), res[provider.Name].Provider.GetType()));
            }
        }

        /// <summary>
        /// Creates the provider proxy.
        /// </summary>
        private static IPluginProviderProxy CreateProviderProxy(IPluginConfiguration pluginConfiguration)
        {
            Debug.Assert(pluginConfiguration != null);

            var cfgType = pluginConfiguration.GetType();

            var attributes = cfgType.GetCustomAttributes(true).OfType<PluginProviderTypeAttribute>().ToArray();

            if (attributes.Length == 0)
            {
                throw new IgniteException(string.Format("{0} of type {1} has no {2}",
                    typeof(IPluginConfiguration), cfgType, typeof(PluginProviderTypeAttribute)));

            }

            if (attributes.Length > 1)
            {
                throw new IgniteException(string.Format("{0} of type {1} has more than one {2}",
                    typeof(IPluginConfiguration), cfgType, typeof(PluginProviderTypeAttribute)));
            }

            var providerType = attributes[0].PluginProviderType;

            var iface = providerType.GetInterfaces()
                .SingleOrDefault(i => i.IsGenericType &&
                                      i.GetGenericTypeDefinition() == typeof(IPluginProvider<>) &&
                                      i.GetGenericArguments()[0] == cfgType);

            if (iface == null)
            {
                throw new IgniteException(string.Format("{0} does not implement {1}",
                    providerType, typeof(IPluginProvider<>).MakeGenericType(cfgType)));
            }

            var pluginProvider = Activator.CreateInstance(providerType);

            var providerProxyType = typeof(PluginProviderProxy<>).MakeGenericType(cfgType);

            return (IPluginProviderProxy) Activator.CreateInstance(
                providerProxyType, pluginConfiguration, pluginProvider);
        }
    }
}

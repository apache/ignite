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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Plugin context.
    /// </summary>
    internal class PluginContext : IPluginContext
    {
        /** */
        private readonly IgniteConfiguration _igniteConfiguration;

        /** */
        private readonly Task<Dictionary<string, IPluginProvider>> _pluginTask;

        /** */
        private volatile IIgnite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginContext"/> class.
        /// </summary>
        /// <param name="igniteConfiguration">The ignite configuration.</param>
        public PluginContext(IgniteConfiguration igniteConfiguration)
        {
            Debug.Assert(igniteConfiguration != null);

            _igniteConfiguration = igniteConfiguration;

            // Load plugins in a separate thread while Ignite is starting.
            _pluginTask = Task<Dictionary<string, IPluginProvider>>.Factory.StartNew(LoadPlugins);
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
            foreach (var provider in _pluginTask.Result.Values)
                provider.OnIgniteStart();
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
        /// Loads the plugins.
        /// </summary>
        private Dictionary<string, IPluginProvider> LoadPlugins()
        {
            var res = new Dictionary<string, IPluginProvider>();

            // Load from memory.
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();

            var providerTypes = assemblies.SelectMany(x => x.GetTypes())
                .Where(t => typeof(IPluginProvider).IsAssignableFrom(t));

            foreach (var providerType in providerTypes)
            {
                var provider = (IPluginProvider) Activator.CreateInstance(providerType);

                provider.Start(this);

                res[provider.Name] = provider;
            }


            // Scan folders.
            // TODO: Test with dynamic assemblies.

            return res;
        }
    }
}

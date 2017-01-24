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

namespace Apache.Ignite.Core.Impl.Plugin.Cache
{
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Plugin.Cache;

    /// <summary>
    /// Wraps user-defined generic <see cref="ICachePluginProvider{TConfig}"/>.
    /// </summary>
    internal class CachePluginProviderProxy<T> : ICachePluginProviderProxy where T : ICachePluginConfiguration
    {
        /** */
        private readonly T _cachePluginConfiguration;

        /** */
        private readonly ICachePluginProvider<T> _pluginProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePluginProviderProxy{T}" /> class.
        /// </summary>
        /// <param name="cachePluginConfiguration">The cache plugin configuration.</param>
        /// <param name="pluginProvider">The plugin provider.</param>
        public CachePluginProviderProxy(T cachePluginConfiguration, ICachePluginProvider<T> pluginProvider)
        {
            Debug.Assert(cachePluginConfiguration != null);
            Debug.Assert(pluginProvider != null);

            _cachePluginConfiguration = cachePluginConfiguration;
            _pluginProvider = pluginProvider;
        }

        /** <inheritdoc /> */
        public void Start(IgniteConfiguration igniteConfiguration, CacheConfiguration cacheConfiguration,
            IIgnite ignite)
        {
            _pluginProvider.Start(new CachePluginContext<T>(igniteConfiguration, 
                cacheConfiguration, _cachePluginConfiguration, ignite));
        }

        /** <inheritdoc /> */
        public void Stop(bool cancel)
        {
            _pluginProvider.Stop(cancel);
        }

        /** <inheritdoc /> */
        public void OnIgniteStart()
        {
            _pluginProvider.OnIgniteStart();
        }

        /** <inheritdoc /> */
        public void OnIgniteStop(bool cancel)
        {
            _pluginProvider.OnIgniteStop(cancel);
        }
    }
}

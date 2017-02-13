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
    using System;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Plugin;
    using Apache.Ignite.Core.Plugin.Cache;

    /// <summary>
    /// Cache plugin processor.
    /// </summary>
    internal static class CachePluginProcessor
    {
        /// <summary>
        /// Creates the provider proxy.
        /// </summary>
        public static ICachePluginProviderProxy CreateProviderProxy(ICachePluginConfiguration pluginConfiguration)
        {
            Debug.Assert(pluginConfiguration != null);

            var cfgType = pluginConfiguration.GetType();

            var attributes = cfgType.GetCustomAttributes(true).OfType<CachePluginProviderTypeAttribute>().ToArray();

            if (attributes.Length == 0)
            {
                throw new IgniteException(string.Format("{0} of type {1} has no {2}",
                    typeof(IPluginConfiguration), cfgType, typeof(CachePluginProviderTypeAttribute)));

            }

            if (attributes.Length > 1)
            {
                throw new IgniteException(string.Format("{0} of type {1} has more than one {2}",
                    typeof(IPluginConfiguration), cfgType, typeof(CachePluginProviderTypeAttribute)));
            }

            var providerType = attributes[0].CachePluginProviderType;

            var iface = providerType.GetInterfaces()
                .SingleOrDefault(i => i.IsGenericType &&
                                      i.GetGenericTypeDefinition() == typeof(ICachePluginProvider<>) &&
                                      i.GetGenericArguments()[0] == cfgType);

            if (iface == null)
            {
                throw new IgniteException(string.Format("{0} does not implement {1}",
                    providerType, typeof(ICachePluginProvider<>).MakeGenericType(cfgType)));
            }

            var pluginProvider = Activator.CreateInstance(providerType);

            var providerProxyType = typeof(CachePluginProviderProxy<>).MakeGenericType(cfgType);

            return (ICachePluginProviderProxy)Activator.CreateInstance(
                providerProxyType, pluginConfiguration, pluginProvider);
        }
    }
}

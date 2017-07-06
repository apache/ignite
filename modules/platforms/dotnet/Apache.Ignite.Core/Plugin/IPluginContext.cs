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

namespace Apache.Ignite.Core.Plugin
{
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Interop;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Plugin execution context.
    /// </summary>
    public interface IPluginContext<out T> where T : IPluginConfiguration
    {
        /// <summary>
        /// Gets the Ignite.
        /// </summary>
        IIgnite Ignite { get; }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        IgniteConfiguration IgniteConfiguration { get; }

        /// <summary>
        /// Gets the plugin configuration.
        /// </summary>
        T PluginConfiguration { get; }

        /// <summary>
        /// Gets a reference to plugin extension on Java side.
        /// <para />
        /// Extensions on Java side are configured via PluginProvider.initExtensions().
        /// Extension should implement PlatformExtension interface to be accessible from this method.
        /// </summary>
        /// <param name="id">Extension id. Equal to PlatformExtension.id().</param>
        /// <returns>Reference to a plugin extension on Java side.</returns>
        IPlatformTarget GetExtension(int id);

        /// <summary>
        /// Registers custom exception mapping: when Java exception of specified class occurs, it will be mapped
        /// using provided factory delegate.
        /// </summary>
        /// <param name="className">Name of the Java exception class to be mapped.</param>
        /// <param name="factory">Exception factory delegate.</param>
        void RegisterExceptionMapping(string className, ExceptionFactory factory);

        /// <summary>
        /// Registers Java->.NET callback.
        /// </summary>
        /// <param name="callbackId">Callback id.</param>
        /// <param name="callback">Callback delegate.</param>
        void RegisterCallback(long callbackId, PluginCallback callback);

        /// <summary>
        /// Injects resources into specified target:
        /// populates members marked with <see cref="InstanceResourceAttribute"/>.
        /// </summary>
        /// <param name="target">Target object.</param>
        void InjectResources(object target);
    }
}
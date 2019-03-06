/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
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
    /// <summary>
    /// Pluggable Ignite component. Should have parameterless constructor.
    /// <para />
    /// Ignite plugins are loaded according to <see cref="IgniteConfiguration.PluginConfigurations"/>.
    /// </summary>
    public interface IPluginProvider<in TConfig> where TConfig : IPluginConfiguration
    {
        /// <summary>
        /// Gets the plugin name. Plugins can be retrieved by name via <see cref="IIgnite.GetPlugin{T}"/>.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the copyright information.
        /// </summary>
        string Copyright { get; }

        /// <summary>
        /// Gets the plugin instance.
        /// </summary>
        /// <typeparam name="T">Plugin instance type.</typeparam>
        /// <returns>Plugin instance.</returns>
        T GetPlugin<T>() where T : class;

        /// <summary>
        /// Starts the plugin provider.
        /// <para />
        /// <see cref="IPluginContext{T}.Ignite"/> can be null until <see cref="OnIgniteStart"/> is called.
        /// </summary>
        /// <param name="context">The context.</param>
        void Start(IPluginContext<TConfig> context);

        /// <summary>
        /// Stops the plugin provider.
        /// </summary>
        /// <param name="cancel">if set to <c>true</c>, all ongoing operations should be canceled.</param>
        void Stop(bool cancel);

        /// <summary>
        /// Called when Ignite has been started and is fully functional.
        /// </summary>
        void OnIgniteStart();

        /// <summary>
        /// Callback to notify that Ignite is about to stop.
        /// </summary>
        /// <param name="cancel">if set to <c>true</c>, all ongoing operations should be canceled.</param>
        void OnIgniteStop(bool cancel);
    }
}

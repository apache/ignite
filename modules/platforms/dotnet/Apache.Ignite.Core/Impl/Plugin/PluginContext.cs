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

namespace Apache.Ignite.Core.Impl.Plugin
{
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Interop;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Plugin context.
    /// </summary>
    internal class PluginContext<T> : IPluginContext<T> where T : IPluginConfiguration
    {
        /** */
        private readonly T _pluginConfiguration;

        /** */
        private readonly PluginProcessor _pluginProcessor;

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginContext{T}"/> class.
        /// </summary>
        public PluginContext(PluginProcessor pluginProcessor, T pluginConfiguration)
        {
            _pluginProcessor = pluginProcessor;
            _pluginConfiguration = pluginConfiguration;
        }

        /** <inheritdoc /> */
        public IIgnite Ignite
        {
            get { return _pluginProcessor.Ignite; }
        }

        /** <inheritdoc /> */
        public IgniteConfiguration IgniteConfiguration
        {
            get { return _pluginProcessor.IgniteConfiguration; }
        }

        /** <inheritdoc /> */
        public T PluginConfiguration
        {
            get { return _pluginConfiguration; }
        }

        /** <inheritdoc /> */
        public IPlatformTarget GetExtension(int id)
        {
            return _pluginProcessor.Ignite.GetExtension(id);
        }

        /** <inheritdoc /> */
        public void RegisterExceptionMapping(string className, ExceptionFactory factory)
        {
            IgniteArgumentCheck.NotNull(className, "className");
            IgniteArgumentCheck.NotNull(factory, "factory");

            _pluginProcessor.RegisterExceptionMapping(className, factory);
        }

        /** <inheritdoc /> */
        public void RegisterCallback(long callbackId, PluginCallback callback)
        {
            IgniteArgumentCheck.NotNull(callback, "callback");

            _pluginProcessor.RegisterCallback(callbackId, callback);
        }

        /** <inheritdoc /> */
        public void InjectResources(object target)
        {
            IgniteArgumentCheck.NotNull(target, "target");

            ResourceProcessor.Inject(target, _pluginProcessor.Ignite);
        }
    }
}

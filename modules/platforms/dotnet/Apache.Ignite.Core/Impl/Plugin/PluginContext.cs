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

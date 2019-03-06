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

namespace Apache.Ignite.Core.Tests.Plugin
{
    using System;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Plugin;
    using NUnit.Framework;

    /// <summary>
    /// Test provider.
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    public class TestIgnitePluginProvider : IPluginProvider<TestIgnitePluginConfiguration>
    {
        /** */
        public const string PluginName = "TestPlugin1";

        /** */
        private readonly TestIgnitePlugin _plugin;

        /// <summary>
        /// Initializes a new instance of the <see cref="TestIgnitePluginProvider"/> class.
        /// </summary>
        public TestIgnitePluginProvider()
        {
            Name = PluginName;
            _plugin = new TestIgnitePlugin(this);
        }
        
        /** <inheritdoc /> */
        public string Name { get; set; }

        /** <inheritdoc /> */
        public string Copyright
        {
            get { return "Copyright (c) FooBar."; }
        }

        /** <inheritdoc /> */
        public T GetPlugin<T>() where T : class
        {
            var p = _plugin as T;

            if (p != null)
                return p;

            throw new Exception("Invalid plugin type: " + typeof(T));
        }

        /** <inheritdoc /> */
        public void Start(IPluginContext<TestIgnitePluginConfiguration> context)
        {
            context.RegisterExceptionMapping("org.apache.ignite.platform.plugin.PlatformTestPluginException",
                (className, message, inner, ignite) =>
                    new TestIgnitePluginException(className, message, ignite, inner));

            context.RegisterCallback(1, (input, output) =>
            {
                CallbackResult = input.ReadString();
                output.WriteString(CallbackResult.ToUpper());

                return CallbackResult.Length;
            });

            var ex = Assert.Throws<IgniteException>(() => context.RegisterCallback(1, (input, output) => 0));
            Assert.AreEqual("Plugin callback with id 1 is already registered", ex.Message);

            Context = context;

            EnsureIgniteWorks(false);
        }

        /** <inheritdoc /> */
        public void Stop(bool cancel)
        {
            Stopped = cancel;

            EnsureIgniteWorks(false);
        }

        /** <inheritdoc /> */
        public void OnIgniteStart()
        {
            Started = true;

            EnsureIgniteWorks(!Context.PluginConfiguration.SkipCacheCheck);
        }

        /** <inheritdoc /> */
        public void OnIgniteStop(bool cancel)
        {
            IgniteStopped = cancel;

            EnsureIgniteWorks();
        }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="TestIgnitePluginProvider"/> is started.
        /// </summary>
        public bool Started { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="TestIgnitePluginProvider"/> is stopped.
        /// </summary>
        public bool? Stopped { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="TestIgnitePluginProvider"/> is stopped.
        /// </summary>
        public bool? IgniteStopped { get; set; }

        public string CallbackResult { get; private set; }

        /// <summary>
        /// Gets the context.
        /// </summary>
        public IPluginContext<TestIgnitePluginConfiguration> Context { get; private set; }

        /// <summary>
        /// Ensures that Ignite instance is functional.
        /// </summary>
        private void EnsureIgniteWorks(bool checkCache = true)
        {
            Assert.NotNull(Context);
            Assert.NotNull(Context.Ignite);
            Assert.NotNull(Context.IgniteConfiguration);
            Assert.NotNull(Context.PluginConfiguration);

            if (checkCache)
            {
                var cache = Context.Ignite.GetOrCreateCache<int, int>("pluginCache");

                Assert.AreEqual(0, cache.GetSize());
            }
        }
    }
}
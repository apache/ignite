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
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
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Interop;
    using Apache.Ignite.Core.Plugin;
    using NUnit.Framework;

    /// <summary>
    /// Ignite plugin test.
    /// </summary>
    public class PluginTest
    {
        /** Plugin log. */
        private static readonly List<string> PluginLog = new List<string>();

        /// <summary>
        /// Tests the plugin life cycle.
        /// </summary>
        [Test]
        public void TestIgniteStartStop()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                PluginConfigurations = new[] {new TestIgnitePluginConfiguration {PluginProperty = "barbaz"}}
            };

            TestIgnitePlugin plugin;

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.Throws<PluginNotFoundException>(() => ignite.GetPlugin<object>("foobar"));
                Assert.Throws<Exception>(() => ignite.GetPlugin<string>(TestIgnitePluginProvider.PluginName));

                plugin = ignite.GetPlugin<TestIgnitePlugin>(TestIgnitePluginProvider.PluginName);
                Assert.IsNotNull(plugin);

                var prov = plugin.Provider;
                Assert.IsTrue(prov.Started);
                Assert.AreEqual(null, prov.Stopped);
                Assert.AreEqual(TestIgnitePluginProvider.PluginName, prov.Name);
                Assert.IsNotNullOrEmpty(prov.Copyright);
                Assert.IsNotNull(prov.Context);

                var ctx = prov.Context;
                Assert.IsNotNull(ctx.Ignite);
                Assert.AreEqual(cfg, ctx.IgniteConfiguration);
                Assert.AreEqual("barbaz", ctx.PluginConfiguration.PluginProperty);

                var plugin2 = ignite.GetPlugin<TestIgnitePlugin>(TestIgnitePluginProvider.PluginName);
                Assert.AreEqual(plugin, plugin2);

                var ex = Assert.Throws<IgniteException>(() => plugin.Provider.Context.GetExtension(0));
                Assert.AreEqual("Platform extension is not registered [id=0]", ex.Message);
            }

            Assert.AreEqual(true, plugin.Provider.Stopped);
            Assert.AreEqual(true, plugin.Provider.IgniteStopped);
        }

        /// <summary>
        /// Tests invalid plugins.
        /// </summary>
        [Test]
        public void TestInvalidPlugins()
        {
            Action<ICollection<IPluginConfiguration>> check = x => Ignition.Start(
                new IgniteConfiguration(TestUtils.GetTestConfiguration()) {PluginConfigurations = x});

            // Missing attribute.
            var ex = Assert.Throws<IgniteException>(() => check(new[] { new NoAttributeConfig(),  }));
            Assert.AreEqual(string.Format("{0} of type {1} has no {2}", typeof(IPluginConfiguration),
                typeof(NoAttributeConfig), typeof(PluginProviderTypeAttribute)), ex.Message);

            // Empty plugin name.
            ex = Assert.Throws<IgniteException>(() => check(new[] {new EmptyNameConfig()}));
            Assert.AreEqual(string.Format("{0}.Name should not be null or empty: {1}", typeof(IPluginProvider<>),
                typeof(EmptyNamePluginProvider)), ex.Message);

            // Duplicate plugin name.
            ex = Assert.Throws<IgniteException>(() => check(new[]
            {
                new TestIgnitePluginConfiguration(),
                new TestIgnitePluginConfiguration()
            }));
            Assert.AreEqual(string.Format("Duplicate plugin name 'TestPlugin1' is used by plugin providers " +
                                          "'{0}' and '{0}'", typeof(TestIgnitePluginProvider)), ex.Message);

            // Provider throws an exception.
            PluginLog.Clear();

            var ioex = Assert.Throws<IOException>(() => check(new IPluginConfiguration[]
            {
                new NormalConfig(), new ExceptionConfig()
            }));
            Assert.AreEqual("Failure in plugin provider", ioex.Message);

            // Verify that plugins are started and stopped in correct order:
            Assert.AreEqual(
                new[]
                {
                    "normalPlugin.Start", "errPlugin.Start",
                    "errPlugin.OnIgniteStop", "normalPlugin.OnIgniteStop",
                    "errPlugin.Stop", "normalPlugin.Stop"
                }, PluginLog);
        }

        private class NoAttributeConfig : IPluginConfiguration
        {
            // No-op.
        }

        [PluginProviderType(typeof(EmptyNamePluginProvider))]
        private class EmptyNameConfig : IPluginConfiguration
        {
            // No-op.
        }

        private class EmptyNamePluginProvider : IPluginProvider<EmptyNameConfig>
        {
            public string Name { get { return null; } }
            public string Copyright { get { return null; } }
            public T GetPlugin<T>() where T : class { return default(T); }
            public void Start(IPluginContext<EmptyNameConfig> context) { /* No-op. */ }
            public void Stop(bool cancel) { /* No-op. */ }
            public void OnIgniteStart() { /* No-op. */ }
            public void OnIgniteStop(bool cancel) { /* No-op. */ }
        }

        [PluginProviderType(typeof(ExceptionPluginProvider))]
        private class ExceptionConfig : IPluginConfiguration
        {
            // No-op.
        }

        private class ExceptionPluginProvider : IPluginProvider<ExceptionConfig>
        {
            public string Name { get { return "errPlugin"; } }
            public string Copyright { get { return null; } }
            public T GetPlugin<T>() where T : class { return default(T); }

            public void Stop(bool cancel)
            {
                PluginLog.Add(Name + ".Stop");
            }

            public void OnIgniteStart()
            {
                PluginLog.Add(Name + ".OnIgniteStart");
            }

            public void OnIgniteStop(bool cancel)
            {
                PluginLog.Add(Name + ".OnIgniteStop");
            }

            public void Start(IPluginContext<ExceptionConfig> context)
            {
                PluginLog.Add(Name + ".Start");
                throw new IOException("Failure in plugin provider");
            }
        }

        [PluginProviderType(typeof(NormalPluginProvider))]
        private class NormalConfig : IPluginConfiguration
        {
            // No-op.
        }

        private class NormalPluginProvider : IPluginProvider<NormalConfig>
        {
            public string Name { get { return "normalPlugin"; } }
            public string Copyright { get { return null; } }
            public T GetPlugin<T>() where T : class { return default(T); }

            public void Stop(bool cancel)
            {
                PluginLog.Add(Name + ".Stop");
            }

            public void OnIgniteStart()
            {
                PluginLog.Add(Name + ".OnIgniteStart");
            }

            public void OnIgniteStop(bool cancel)
            {
                PluginLog.Add(Name + ".OnIgniteStop");
            }

            public void Start(IPluginContext<NormalConfig> context)
            {
                PluginLog.Add(Name + ".Start");
            }
        }
    }
}

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
    using Apache.Ignite.Core.Plugin;
    using NUnit.Framework;

    /// <summary>
    /// Ignite plugin test.
    /// </summary>
    public class PluginTest
    {
        /// <summary>
        /// Tests the plugin life cycle.
        /// </summary>
        [Test]
        public void TestIgniteStartStop()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                PluginConfigurations = new[] {new TestIgnitePluginConfiguration()}
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

                var plugin2 = ignite.GetPlugin<TestIgnitePlugin>(TestIgnitePluginProvider.PluginName);
                Assert.AreEqual(plugin, plugin2);
            }

            Assert.AreEqual(true, plugin.Provider.Stopped);
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
            Assert.AreEqual("Apache.Ignite.Core.Plugin.IPluginProvider.Name should not be null or empty: " +
                            typeof(TestIgnitePluginProvider).AssemblyQualifiedName, ex.Message);

            // Empty plugin name.
            ex = Assert.Throws<IgniteException>(() => check(new[] {new EmptyNameConfig()}));
            Assert.AreEqual("Apache.Ignite.Core.Plugin.IPluginProvider.Name should not be null or empty: " +
                            typeof(TestIgnitePluginProvider).AssemblyQualifiedName, ex.Message);

            // Duplicate plugin name.
            ex = Assert.Throws<IgniteException>(() => check(new[]
            {
                new TestIgnitePluginConfiguration(),
                new TestIgnitePluginConfiguration()
            }));
            Assert.AreEqual(string.Format("Duplicate plugin name 'TestPlugin1' is used by plugin providers " +
                                          "'{0}' and '{0}'", typeof(TestIgnitePluginProvider).AssemblyQualifiedName),
                ex.Message);

            // Provider throws an exception.
            var ioex = Assert.Throws<IOException>(() => check(new[] {new ExceptionConfig()}));
            Assert.AreEqual("Failure in plugin provider", ioex.Message);
        }

        private class NoAttributeConfig : IPluginConfiguration
        {
            // No-op.
        }

        [PluginProviderType(typeof(NoNamePluginProvider))]
        private class EmptyNameConfig : IPluginConfiguration
        {
            // No-op.
        }

        private class NoNamePluginProvider : TestIgnitePluginProvider
        {
            public NoNamePluginProvider()
            {
                Name = "";
            }
        }

        [PluginProviderType(typeof(ExceptionPluginProvider))]
        private class ExceptionConfig : IPluginConfiguration
        {
            // No-op.
        }

        private class ExceptionPluginProvider : TestIgnitePluginProvider
        {
            public ExceptionPluginProvider()
            {
                ThrowError = true;
            }
        }

    }
}

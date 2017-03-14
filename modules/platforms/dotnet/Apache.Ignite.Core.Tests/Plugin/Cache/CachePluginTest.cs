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

namespace Apache.Ignite.Core.Tests.Plugin.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Plugin.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for cache plugins.
    /// </summary>
    public class CachePluginTest
    {
        /** */
        private const string CacheName = "staticCache";

        /** */
        private const string DynCacheName = "dynamicCache";

        /** */
        private IIgnite _grid1;

        /** */
        private IIgnite _grid2;

        /** */
        private readonly List<CachePlugin> _plugins = new List<CachePlugin>();

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _plugins.Clear();

            _grid1 = Ignition.Start(GetConfig("grid1"));
            _grid2 = Ignition.Start(GetConfig("grid2"));
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            // One plugin is expected in registry.
            TestUtils.AssertHandleRegistryHasItems(10, 1, _grid1, _grid2);

            Ignition.StopAll(true);

            // Check IgniteStop callbacks.
            foreach (var plugin in _plugins)
            {
                Assert.AreEqual(true, plugin.IgniteStopped);
            }
        }

        /// <summary>
        /// Tests with static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            foreach (var ignite in new[] {_grid1, _grid2})
            {
                var plugin = CheckCachePlugin(ignite, CacheName, "foo");

                _plugins.Add(plugin);
            }
        }

        /// <summary>
        /// Tests with dynamic cache.
        /// </summary>
        [Test]
        public void TestDynamicCache()
        {
            var cacheConfig = new CacheConfiguration(DynCacheName)
            {
                PluginConfigurations = new[] {new CachePluginConfiguration {TestProperty = "bar"}}
            };

            _grid1.CreateCache<int, int>(cacheConfig);

            var plugins = new List<CachePlugin>();

            foreach (var ignite in new[] { _grid1, _grid2 })
            {
                var plugin = CheckCachePlugin(ignite, DynCacheName, "bar");

                plugins.Add(plugin);
            }

            // Destroy cache to remove plugin from handle registry.
            _grid1.DestroyCache(DynCacheName);

            foreach (var plugin in plugins)
            {
                Assert.AreEqual(true, plugin.Stopped);
                Assert.AreEqual(true, plugin.IgniteStopped);  // This is weird, but correct from Java logic POV.
            }
        }

        /// <summary>
        /// Non-serializable plugin config results in a meaningful exception.
        /// </summary>
        [Test]
        public void TestNonSerializablePlugin()
        {
            var ex = Assert.Throws<InvalidOperationException>(() => _grid1.CreateCache<int, int>(new CacheConfiguration
            {
                PluginConfigurations = new[] {new NonSerializableCachePluginConfig()}
            }));

            Assert.AreEqual("Invalid cache configuration: ICachePluginConfiguration should be Serializable.", 
                ex.Message);
        }

        /// <summary>
        /// Errors in plugin configuration result in meaningful exception.
        /// </summary>
        [Test]
        [Ignore("IGNITE-4474 Ignite.createCache hangs on exception in CachePluginConfiguration.createProvider")]
        public void TestErrorInPlugin()
        {
            // Throws exception.
            var cacheEx = Assert.Throws<CacheException>(() => _grid1.CreateCache<int, int>(new CacheConfiguration
            {
                PluginConfigurations = new[] { new ThrowCachePluginConfig()  }
            }));

            Assert.AreEqual("hi!", cacheEx.Message);
        }

        /// <summary>
        /// Checks the cache plugin.
        /// </summary>
        private static CachePlugin CheckCachePlugin(IIgnite ignite, string cacheName, string propValue)
        {
            // Check config.
            var plugCfg = ignite.GetCache<int, int>(cacheName).GetConfiguration()
                .PluginConfigurations.Cast<CachePluginConfiguration>().Single();
            Assert.AreEqual(propValue, plugCfg.TestProperty);

            // Check started plugin.
            var plugin = CachePlugin.GetInstances().Single(x => x.Context.Ignite == ignite &&
                                                                x.Context.CacheConfiguration.Name == cacheName);
            Assert.IsTrue(plugin.Started);
            Assert.IsTrue(plugin.IgniteStarted);
            Assert.IsNull(plugin.Stopped);
            Assert.IsNull(plugin.IgniteStopped);

            var ctx = plugin.Context;
            Assert.AreEqual(ignite.Name, ctx.IgniteConfiguration.GridName);
            Assert.AreEqual(cacheName, ctx.CacheConfiguration.Name);
            Assert.AreEqual(propValue, ctx.CachePluginConfiguration.TestProperty);

            return plugin;
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private static IgniteConfiguration GetConfig(string name)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = name,
                CacheConfiguration = new[]
                {
                    new CacheConfiguration(CacheName)
                    {
                        PluginConfigurations = new[]
                        {
                            new CachePluginConfiguration {TestProperty = "foo"}
                        }
                    }
                }
            };
        }

        [CachePluginProviderType(typeof(CachePlugin))]
        private class NonSerializableCachePluginConfig : ICachePluginConfiguration
        {
            public int? CachePluginConfigurationClosureFactoryId { get { return null; } }
            public void WriteBinary(IBinaryRawWriter writer) { /* No-op. */ }
        }

        [Serializable]
        [CachePluginProviderType(typeof(string))]
        private class ThrowCachePluginConfig : ICachePluginConfiguration
        {
            public int? CachePluginConfigurationClosureFactoryId { get { return null; } }
            public void WriteBinary(IBinaryRawWriter writer) { /* No-op. */ }
        }
    }
}

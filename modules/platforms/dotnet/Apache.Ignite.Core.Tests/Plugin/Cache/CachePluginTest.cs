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
    using System.Linq;
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

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
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
        }

        /// <summary>
        /// Tests with static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            foreach (var ignite in new[] {_grid1, _grid2})
            {
                CheckCachePlugin(ignite, CacheName, "foo");
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

            foreach (var ignite in new[] { _grid1, _grid2 })
            {
                CheckCachePlugin(ignite, DynCacheName, "bar");
            }

            // Destroy cache to remove plugin from handle registry.
            _grid1.DestroyCache(DynCacheName);
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
            // Returns null plugin.
            var cacheEx = Assert.Throws<CacheException>(() => _grid1.CreateCache<int, int>(new CacheConfiguration
            {
                PluginConfigurations = new[] { new NullCachePluginConfig()  }
            }));

            Assert.AreEqual("ICachePluginConfiguration.CreateProvider should not return null.", cacheEx.Message);
            
            // Throws exception.
            cacheEx = Assert.Throws<CacheException>(() => _grid1.CreateCache<int, int>(new CacheConfiguration
            {
                PluginConfigurations = new[] { new ThrowCachePluginConfig()  }
            }));

            Assert.AreEqual("hi!", cacheEx.Message);
        }

        /// <summary>
        /// Checks the cache plugin.
        /// </summary>
        private static void CheckCachePlugin(IIgnite ignite, string cacheName, string propValue)
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

            var ctx = plugin.Context;
            Assert.AreEqual(ignite.Name, ctx.IgniteConfiguration.GridName);
            Assert.AreEqual(cacheName, ctx.CacheConfiguration.Name);
            Assert.AreEqual(propValue, ((CachePluginConfiguration)ctx.CachePluginConfiguration).TestProperty);
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

        private class NonSerializableCachePluginConfig : ICachePluginConfiguration
        {
            public ICachePluginProvider CreateProvider(ICachePluginContext pluginContext)
            {
                return new CachePlugin(pluginContext);
            }
        }

        [Serializable]
        private class NullCachePluginConfig : ICachePluginConfiguration
        {
            public ICachePluginProvider CreateProvider(ICachePluginContext pluginContext)
            {
                return null;
            }
        }

        [Serializable]
        private class ThrowCachePluginConfig : ICachePluginConfiguration
        {
            public ICachePluginProvider CreateProvider(ICachePluginContext pluginContext)
            {
                throw new ApplicationException("hi!");
            }
        }
    }
}

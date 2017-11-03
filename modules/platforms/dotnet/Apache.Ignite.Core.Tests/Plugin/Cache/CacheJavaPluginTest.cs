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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests the plugin with Java part.
    /// </summary>
    public class CacheJavaPluginTest
    {
        /** */
        private const string CacheName = "staticCache";

        /** */
        private const string DynCacheName = "dynamicCache";

        /** */
        private const string GetPluginsTask = "org.apache.ignite.platform.plugin.cache.PlatformGetCachePluginsTask";

        /** */
        private const string PluginConfigurationClass = 
            "org.apache.ignite.platform.plugin.cache.PlatformTestCachePluginConfiguration";

        /** */
        private IIgnite _grid;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration(CacheName)
                    {
                        PluginConfigurations = new[] {new CacheJavaPluginConfiguration()}
                    }
                }
            };

            _grid = Ignition.Start(cfg);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that cache plugin works with static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            var cache = _grid.GetCache<int, int>(CacheName);

            VerifyCachePlugin(cache);
        }

        /// <summary>
        /// Tests that cache plugin works with static cache.
        /// </summary>
        [Test]
        public void TestDynamicCache()
        {
            var cache = _grid.CreateCache<int, int>(new CacheConfiguration(DynCacheName)
            {
                PluginConfigurations = new[] {new CacheJavaPluginConfiguration()}
            });

            VerifyCachePlugin(cache);
        }

        /// <summary>
        /// Verifies the cache plugin.
        /// </summary>
        private void VerifyCachePlugin(ICache<int, int> cache)
        {
            Assert.IsNull(cache.GetConfiguration().PluginConfigurations); // Java cache plugins are not returned.

            cache[1] = 1;
            Assert.AreEqual(1, cache[1]);

            var plugins = _grid.GetCompute().ExecuteJavaTask<string[]>(GetPluginsTask, cache.Name);
            Assert.AreEqual(new[] {PluginConfigurationClass}, plugins);
        }
    }
}

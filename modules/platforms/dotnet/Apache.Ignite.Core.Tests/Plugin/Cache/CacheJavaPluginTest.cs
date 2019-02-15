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

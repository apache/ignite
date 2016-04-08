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

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using NUnit.Framework;

    /// <summary>
    /// Near cache test.
    /// </summary>
    public class CacheNearTest
    {
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
                    new CacheConfiguration
                    {
                        NearConfiguration = new NearCacheConfiguration
                        {
                            EvictionPolicy = new FifoEvictionPolicy {MaxSize = 5}
                        }
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
        /// Tests the existing near cache.
        /// </summary>
        [Test]
        public void TestExistingNearCache()
        {
            var cache = _grid.GetCache<int, string>(null);

            cache[1] = "1";

            var nearCache = _grid.GetOrCreateNearCache<int, string>(null, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);

            // GetOrCreate when exists
            nearCache = _grid.GetOrCreateNearCache<int, string>(null, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);
        }

        /// <summary>
        /// Tests the created near cache.
        /// </summary>
        [Test]
        public void TestCreateNearCache()
        {
            const string cacheName = "dyn_cache";

            var cache = _grid.CreateCache<int, string>(cacheName);
            cache[1] = "1";

            var nearCache = _grid.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);

            // Create when exists
            nearCache = _grid.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestOnClientNode()
        {
            const string cacheName = "client_cache";

            using (var clientGrid =
                    Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()) {ClientMode = true}))
            {
                var cache = clientGrid.CreateCache<int, string>(cacheName);
                cache[1] = "1";

                var nearCache = clientGrid.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
                Assert.AreEqual("1", nearCache[1]);
            }
        }
    }
}

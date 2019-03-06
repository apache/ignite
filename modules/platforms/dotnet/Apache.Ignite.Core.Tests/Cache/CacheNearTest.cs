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

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Near cache test.
    /// </summary>
    public class CacheNearTest : IEventListener<CacheEvent>
    {
        /** */
        private const string DefaultCacheName = "default";

        /** */
        private IIgnite _grid;

        /** */
        private volatile CacheEvent _lastEvent;

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
                        },
                        Name = DefaultCacheName
                    }
                },
                IncludedEventTypes = new[] { EventType.CacheEntryCreated },
                IgniteInstanceName = "server"
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
            var cache = _grid.GetCache<int, string>(DefaultCacheName);

            cache[1] = "1";

            var nearCache = _grid.GetOrCreateNearCache<int, string>(DefaultCacheName, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);

            // GetOrCreate when exists
            nearCache = _grid.GetOrCreateNearCache<int, string>(DefaultCacheName, new NearCacheConfiguration());
            Assert.AreEqual("1", nearCache[1]);
        }

        /// <summary>
        /// Tests the created near cache.
        /// </summary>
        [Test]
        public void TestCreateNearCache()
        {
            const string cacheName = "dyn_cache";

            var cache = _grid.CreateCache<int, string>(new CacheConfiguration(cacheName));
            cache[1] = "1";

            using (var client = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "client"
            }))
            {
                var nearCache = client.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
                Assert.AreEqual("1", nearCache[1]);

                // Create when exists.
                nearCache = client.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration());
                Assert.AreEqual("1", nearCache[1]);
            }
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateNearCacheOnClientNode()
        {
            const string cacheName = "client_cache";

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "clientGrid"
            };

            using (var clientGrid = Ignition.Start(cfg))
            {
                clientGrid.CreateCache<int, string>(cacheName);

                // Near cache can't be started on client node
                Assert.Throws<CacheException>(
                    () => clientGrid.CreateNearCache<int, string>(cacheName, new NearCacheConfiguration()));
            }
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateCacheWithNearConfigOnClientNode()
        {
            const string cacheName = "client_with_near_cache";

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "clientGrid",
                IncludedEventTypes = new[] {EventType.CacheEntryCreated}
            };

            using (var clientGrid = Ignition.Start(cfg))
            {
                var cache = clientGrid.CreateCache<int, string>(new CacheConfiguration(cacheName), 
                    new NearCacheConfiguration());

                AssertCacheIsNear(cache);

                cache[1] = "1";
                Assert.AreEqual("1", cache[1]);

                var cache2 = clientGrid.GetOrCreateCache<int, string>(new CacheConfiguration(cacheName),
                    new NearCacheConfiguration());

                Assert.AreEqual("1", cache2[1]);
            }
        }

        /// <summary>
        /// Asserts the cache is near.
        /// </summary>
        private void AssertCacheIsNear(ICache<int, string> cache)
        {
            var events = cache.Ignite.GetEvents();
            events.LocalListen(this, EventType.CacheEntryCreated);

            _lastEvent = null;
            cache[-1] = "test";

            TestUtils.WaitForCondition(() => _lastEvent != null, 500);
            Assert.IsNotNull(_lastEvent);
            Assert.IsTrue(_lastEvent.IsNear);

            events.StopLocalListen(this, EventType.CacheEntryCreated);
        }

        /** <inheritdoc /> */
        public bool Invoke(CacheEvent evt)
        {
            _lastEvent = evt;
            return true;
        }
    }
}

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

namespace Apache.Ignite.Core.Tests.Cache.Near
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Near cache tests: check create / destroy cache scenarios.
    /// </summary>
    public class CacheNearTestCreateDestroy : IEventListener<CacheEvent>
    {
        /** */
        private IIgnite _grid;

        /** */
        private IIgnite _grid2;

        /** */
        private IIgnite _client;
        
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
                IncludedEventTypes = new[] { EventType.CacheEntryCreated },
                IgniteInstanceName = "server1"
            };

            _grid = Ignition.Start(cfg);
            
            var cfg2 = new IgniteConfiguration(cfg)
            {
                IgniteInstanceName = "server2"
            };

            _grid2 = Ignition.Start(cfg2);

            var clientCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "client",
                IncludedEventTypes = new[] {EventType.CacheEntryCreated}
            };

            _client = Ignition.Start(clientCfg);
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
        /// Tests the created near cache.
        /// </summary>
        [Test]
        public void TestCreateNearCache(
            [Values(CacheMode.Partitioned, CacheMode.Replicated)]
            CacheMode cacheMode,
            [Values(CacheAtomicityMode.Atomic, CacheAtomicityMode.Transactional)]
            CacheAtomicityMode atomicityMode)
        {
            var cacheName = string.Format("dyn_cache_{0}_{1}", cacheMode, atomicityMode);

            var cfg = new CacheConfiguration(cacheName)
            {
                AtomicityMode = atomicityMode,
                CacheMode = cacheMode
            };

            var cache = _grid.CreateCache<int, int>(cfg);
            cache[1] = 1;

            var nearCacheConfiguration = new NearCacheConfiguration();
            var nearCache = _client.CreateNearCache<int, int>(cacheName, nearCacheConfiguration);
            Assert.AreEqual(1, nearCache[1]);

            // Create when exists.
            nearCache = _client.CreateNearCache<int, int>(cacheName, nearCacheConfiguration);
            Assert.AreEqual(1, nearCache[1]);

            // Update entry.
            cache[1] = 2;
            Assert.True(TestUtils.WaitForCondition(() => nearCache[1] == 2, 300));

            // Update through near.
            nearCache[1] = 3;
            Assert.AreEqual(3, nearCache[1]);

            // Remove.
            cache.Remove(1);
            Assert.True(TestUtils.WaitForCondition(() => !nearCache.ContainsKey(1), 300));
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateNearCacheOnClientNode()
        {
            const string cacheName = "client_cache";

            _client.CreateCache<int, int>(cacheName);

            // Near cache can't be started on client node
            Assert.Throws<CacheException>(
                () => _client.CreateNearCache<int, int>(cacheName, new NearCacheConfiguration()));
        }

        /// <summary>
        /// Tests near cache on the client node.
        /// </summary>
        [Test]
        public void TestCreateCacheWithNearConfigOnClientNode()
        {
            const string cacheName = "client_with_near_cache";

            var cache = _client.CreateCache<int, int>(new CacheConfiguration(cacheName),
                new NearCacheConfiguration());

            AssertCacheIsNear(cache);

            cache[1] = 1;
            Assert.AreEqual(1, cache[1]);

            var cache2 = _client.GetOrCreateCache<int, int>(new CacheConfiguration(cacheName), 
                new NearCacheConfiguration());

            Assert.AreEqual(1, cache2[1]);

            cache[1] = 2;
            Assert.AreEqual(2, cache2[1]);
        }
        
        /// <summary>
        /// Tests that near cache data is cleared when underlying cache is destroyed.
        /// </summary>
        [Test]
        public void TestDestroyCacheClearsNearCacheData(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName + mode,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            var ignite = GetIgnite(mode);
            
            var cache = ignite.CreateCache<int, int>(cfg, new NearCacheConfiguration());
            
            cache[1] = 1;
            ignite.DestroyCache(cache.Name);

            var ex = Assert.Throws<InvalidOperationException>(() => cache.Get(1));
            
            StringAssert.EndsWith(
                "Failed to perform cache operation (cache is stopped): " + cache.Name, 
                ex.Message);
        }

        /// <summary>
        /// Tests that near cache data is cleared when cache is destroyed and then created again with the same name.
        /// </summary>
        [Test]
        public void TestCreateWithSameNameAfterDestroyClearsOldData(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName + mode,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            var ignite = GetIgnite(mode);

            var cache = ignite.CreateCache<int, int>(cfg,  new NearCacheConfiguration());
            
            cache[1] = 1;
            ignite.DestroyCache(cache.Name);
            
            cache = ignite.CreateCache<int, int>(cfg, new NearCacheConfiguration());
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests that platform near cache can be started on a given client node only, avoiding server near cache.
        /// Checks different overloads of CreateCache/GetOrCreateCache.
        /// </summary>
        [Test]
        public void TestClientOnlyPlatformNearCache()
        {
            var cache = _grid.CreateCache<int, int>(TestUtils.TestName);
            cache[1] = 2;
            
            var clientCache = _client.CreateNearCache<int, int>(cache.Name, new NearCacheConfiguration(), 
                new PlatformNearCacheConfiguration());
            
            Assert.AreEqual(2, clientCache[1]);
            Assert.AreEqual(1, clientCache.GetLocalSize(CachePeekMode.PlatformNear));

            var clientCache2 = _client.GetOrCreateNearCache<int, int>(cache.Name, new NearCacheConfiguration(),
                new PlatformNearCacheConfiguration());
            
            Assert.AreEqual(2, clientCache2[1]);
            Assert.AreEqual(1, clientCache2.GetLocalSize(CachePeekMode.PlatformNear));

            var clientCache3 = _client.CreateCache<int, int>(new CacheConfiguration(cache.Name + "3"), 
                new NearCacheConfiguration(), new PlatformNearCacheConfiguration());

            clientCache3[1] = 2;
            Assert.AreEqual(2, clientCache3.LocalPeek(1, CachePeekMode.PlatformNear));
            
            var clientCache4 = _client.GetOrCreateCache<int, int>(new CacheConfiguration(cache.Name + "4"), 
                new NearCacheConfiguration(), new PlatformNearCacheConfiguration());

            clientCache4[1] = 2;
            Assert.AreEqual(2, clientCache4.LocalPeek(1, CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests that Java near cache is not necessary for .NET near cache to function on server nodes.
        /// </summary>
        [Test]
        public void TestPlatformNearCacheOnServerWithoutJavaNearCache()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            var cache1 = _grid.CreateCache<int, Foo>(cfg);
            var cache2 = _grid2.GetCache<int, Foo>(cfg.Name);
            
            var key = TestUtils.GetPrimaryKey(_grid, cfg.Name);
            
            // Not in near on non-primary node.
            cache2[key] = new Foo(key);
            Assert.AreEqual(key, cache2[key].Bar);
            
            Assert.AreEqual(0, cache2.GetLocalEntries(CachePeekMode.PlatformNear).Count());
            
            // In near on primary node.
            Assert.AreEqual(key, cache1.GetLocalEntries(CachePeekMode.PlatformNear).Single().Key);
        }

        /// <summary>
        /// Tests that invalid config is handled properly.
        /// </summary>
        [Test]
        public void TestCreateNearCacheWithInvalidKeyValueTypeNames([Values(true, false)] bool client, 
            [Values(true, false)] bool keyOrValue)
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                PlatformNearConfiguration = new PlatformNearCacheConfiguration
                {
                    KeyTypeName = keyOrValue ? "invalid" : null,
                    ValueTypeName = keyOrValue ? null : "invalid"
                }
            };

            var ignite = client ? _client : _grid;

            var err = Assert.Throws<InvalidOperationException>(() => ignite.CreateCache<int, int>(cfg));

            var expectedMessage = string.Format(
                "Can not create .NET Near Cache: PlatformNearCacheConfiguration.{0} is invalid. " +
                "Failed to resolve type: 'invalid'", keyOrValue ? "KeyTypeName" : "ValueTypeName");
            
            Assert.AreEqual(expectedMessage, err.Message);
        }
        
        /// <summary>
        /// Asserts the cache is near.
        /// </summary>
        private void AssertCacheIsNear(ICache<int, int> cache)
        {
            var events = cache.Ignite.GetEvents();
            events.LocalListen(this, EventType.CacheEntryCreated);

            _lastEvent = null;
            cache[-1] = int.MinValue;

            TestUtils.WaitForCondition(() => _lastEvent != null, 500);
            Assert.IsNotNull(_lastEvent);
            Assert.IsTrue(_lastEvent.IsNear);

            events.StopLocalListen(this, EventType.CacheEntryCreated);
        }

        /// <summary>
        /// Gets Ignite instance for mode.
        /// </summary>
        private IIgnite GetIgnite(CacheTestMode mode)
        {
            return new[] {_grid, _grid2, _client}[(int) mode];
        }

        /** <inheritdoc /> */
        public bool Invoke(CacheEvent evt)
        {
            _lastEvent = evt;
            return true;
        }
        
        /** */
        public enum CacheTestMode
        {
            ServerLocal,
            ServerRemote,
            Client
        }
    }
}

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

namespace Apache.Ignite.Core.Tests.Cache.Platform
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Platform cache tests: check create / destroy cache scenarios.
    /// </summary>
    public class PlatformCacheTestCreateDestroy
    {
        /** */
        private IIgnite _grid;

        /** */
        private IIgnite _grid2;

        /** */
        private IIgnite _client;
        
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
        /// Tests that platform cache data is cleared when underlying cache is destroyed.
        /// </summary>
        [Test]
        public void TestDestroyCacheClearsPlatformCacheData(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName + mode,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformCacheConfiguration = new PlatformCacheConfiguration()
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
        /// Tests that platform cache data is cleared when cache is destroyed and then created again with the same name.
        /// </summary>
        [Test]
        public void TestCreateWithSameNameAfterDestroyClearsOldData(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName + mode,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformCacheConfiguration = new PlatformCacheConfiguration()
            };

            var ignite = GetIgnite(mode);

            var cache = ignite.CreateCache<int, int>(cfg,  new NearCacheConfiguration());
            
            cache[1] = 1;
            ignite.DestroyCache(cache.Name);
            
            cache = ignite.CreateCache<int, int>(cfg, new NearCacheConfiguration());
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.Platform));
        }

        /// <summary>
        /// Tests that platform cache can be started on a given client node only, avoiding server platform cache.
        /// Checks different overloads of CreateCache/GetOrCreateCache.
        /// </summary>
        [Test]
        public void TestClientOnlyPlatformCache()
        {
            var cache = _grid.CreateCache<int, int>(TestUtils.TestName);
            cache[1] = 2;
            
            var clientCache = _client.CreateNearCache<int, int>(cache.Name, new NearCacheConfiguration(), 
                new PlatformCacheConfiguration());
            
            Assert.AreEqual(2, clientCache[1]);
            Assert.AreEqual(1, clientCache.GetLocalSize(CachePeekMode.Platform));

            var clientCache2 = _client.GetOrCreateNearCache<int, int>(cache.Name, new NearCacheConfiguration(),
                new PlatformCacheConfiguration());
            
            Assert.AreEqual(2, clientCache2[1]);
            Assert.AreEqual(1, clientCache2.GetLocalSize(CachePeekMode.Platform));

            var clientCache3 = _client.CreateCache<int, int>(new CacheConfiguration(cache.Name + "3"), 
                new NearCacheConfiguration(), new PlatformCacheConfiguration());

            clientCache3[1] = 2;
            Assert.AreEqual(2, clientCache3.LocalPeek(1, CachePeekMode.Platform));
            
            var clientCache4 = _client.GetOrCreateCache<int, int>(new CacheConfiguration(cache.Name + "4"), 
                new NearCacheConfiguration(), new PlatformCacheConfiguration());

            clientCache4[1] = 2;
            Assert.AreEqual(2, clientCache4.LocalPeek(1, CachePeekMode.Platform));
        }

        /// <summary>
        /// Tests that Java near cache is not necessary for .NET platform cache to function on server nodes.
        /// </summary>
        [Test]
        public void TestPlatformCacheOnServerWithoutJavaNearCache()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                PlatformCacheConfiguration = new PlatformCacheConfiguration()
            };

            var cache1 = _grid.CreateCache<int, Foo>(cfg);
            var cache2 = _grid2.GetCache<int, Foo>(cfg.Name);
            
            var key = TestUtils.GetPrimaryKey(_grid, cfg.Name);
            
            // Not in platform cache on non-primary node.
            cache2[key] = new Foo(key);
            Assert.AreEqual(key, cache2[key].Bar);
            
            Assert.AreEqual(0, cache2.GetLocalEntries(CachePeekMode.Platform).Count());
            
            // In platform cache on primary node.
            Assert.AreEqual(key, cache1.GetLocalEntries(CachePeekMode.Platform).Single().Key);
        }

        /// <summary>
        /// Tests that invalid config is handled properly.
        /// </summary>
        [Test]
        public void TestCreatePlatformCacheWithInvalidKeyValueTypeNames([Values(true, false)] bool client, 
            [Values(true, false)] bool keyOrValue)
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    KeyTypeName = keyOrValue ? "invalid" : null,
                    ValueTypeName = keyOrValue ? null : "invalid"
                }
            };

            var ignite = client ? _client : _grid;

            var err = Assert.Throws<InvalidOperationException>(() => ignite.CreateCache<int, int>(cfg));

            var expectedMessage = string.Format(
                "Can not create .NET Platform Cache: PlatformCacheConfiguration.{0} is invalid. " +
                "Failed to resolve type: 'invalid'", keyOrValue ? "KeyTypeName" : "ValueTypeName");
            
            Assert.AreEqual(expectedMessage, err.Message);
        }
        
        /// <summary>
        /// Gets Ignite instance for mode.
        /// </summary>
        private IIgnite GetIgnite(CacheTestMode mode)
        {
            return new[] {_grid, _grid2, _client}[(int) mode];
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

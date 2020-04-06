﻿/*
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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Near cache test.
    /// </summary>
    public class CacheNearTest
    {
        /** */
        private const string CacheName = "default";

        /** */
        private const int NearCacheMaxSize = 3;

        /** */
        private IIgnite _grid;

        /** */
        private IIgnite _grid2;

        /** */
        private IIgnite _client;

        /** */
        private ListLogger _logger;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void FixtureSetUp()
        {
            _logger = new ListLogger(new ConsoleLogger())
            {
                EnabledLevels = new[] {LogLevel.Error}
            };
            
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        NearConfiguration = new NearCacheConfiguration
                        {
                            EvictionPolicy = new FifoEvictionPolicy {MaxSize = NearCacheMaxSize},
                        },
                        PlatformNearConfiguration = new PlatformNearCacheConfiguration(),
                        Name = CacheName,
                        QueryEntities = new[]
                        {
                            new QueryEntity(typeof(Foo))
                        }
                    }
                },
                IgniteInstanceName = "server1",
                Logger = _logger
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
            
            WaitForRebalance();
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
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            _grid.GetCache<int, int>(CacheName).RemoveAll();
            _logger.Clear();
        }

        /// <summary>
        /// Tests the existing near cache.
        /// </summary>
        [Test]
        public void TestExistingNearCache()
        {
            var cache = _grid.GetCache<int, int>(CacheName);
            cache[1] = 1;

            var nearCache = _grid.GetOrCreateNearCache<int, int>(CacheName, new NearCacheConfiguration());
            
            Assert.AreEqual(1, nearCache[1]);

            // GetOrCreate when exists
            nearCache = _grid.GetOrCreateNearCache<int, int>(CacheName, new NearCacheConfiguration());
            
            Assert.AreEqual(1, nearCache[1]);

            cache[1] = 2;
            Assert.AreEqual(2, nearCache[1]);
        }

        /// <summary>
        /// Tests that near cache does not return same instance that we Put there:
        /// there is always serialize-deserialize roundtrip.
        /// </summary>
        [Test]
        public void TestNearCachePutGetReturnsNewObject(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode,
            [Values(true, false)] bool primaryKey)
        {
            var cache = GetCache<int, Foo>(mode);
            var key = TestUtils.GetKey(_grid, cache.Name, primaryKey: primaryKey);

            var obj = new Foo(key);
            
            cache[key] = obj;
            var res1 = cache[key];
            var res2 = cache[key];

            // Returned object is Equal to the initial.
            Assert.AreEqual(obj, res1);
            
            // But not the same - new instance is stored in Near Cache,
            // except primary on servers - thread-local optimization avoids extra deserialization there.
            if (primaryKey && mode == CacheTestMode.ServerLocal || !primaryKey && mode == CacheTestMode.ServerRemote)
            {
                Assert.AreSame(obj, res1);
            }
            else
            {
                Assert.AreNotSame(obj, res1);
            }

            // Repeated Get call returns same instance from Near Cache.
            Assert.AreSame(res1, res2);
        }

        /// <summary>
        /// Tests that near cache returns the same object on every get.
        /// </summary>
        [Test]
        public void TestNearCacheRepeatedGetReturnsSameObjectReference(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode,
            [Values(true, false)] bool primaryKey,
            [Values(true, false)] bool localPut)
        {
            var cache = GetCache<int, Foo>(mode);
            var key = TestUtils.GetKey(_grid, cache.Name, primaryKey: primaryKey);

            var obj = new Foo(3);

            if (localPut)
            {
                // Local put through the same cache instance: obj is in .NET Near Cache directly.
                cache[key] = obj;
            }
            else
            {
                // Put through remote node: near cache is updated only on Get.
                var remoteCache = GetCache<int, Foo>(
                    mode == CacheTestMode.Client ? CacheTestMode.ServerRemote : CacheTestMode.Client);
                
                remoteCache[key] = obj;
            }

            Assert.AreEqual(3, cache[key].Bar);
            Assert.AreSame(cache[key], cache[key]);
        }
        
        /// <summary>
        /// Tests that near cache returns the same object on every get.
        /// </summary>
        [Test]
        public void TestNearCacheRepeatedRemoteGetReturnsSameObjectReference(
            [Values(CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode,
            [Values(true, false)] bool primaryKey)
        {
            var remoteCache = GetCache<int, Foo>(CacheTestMode.ServerLocal);
            var localCache = GetCache<int, Foo>(mode);
            var key = TestUtils.GetKey(_grid, remoteCache.Name, primaryKey: primaryKey);

            remoteCache[key] = new Foo();

            TestUtils.WaitForTrueCondition(() =>
            {
                Foo val;

                return localCache.TryGet(key, out val) && 
                       ReferenceEquals(val, localCache.Get(key));
            }, 300);
            
            // Invalidate after get.
            remoteCache[key] = new Foo(1);
            
            TestUtils.WaitForTrueCondition(() =>
            {
                Foo val;

                return localCache.TryGet(key, out val) &&
                       val.Bar == 1 &&
                       ReferenceEquals(val, localCache.Get(key));
            }, 300);
        }
        
        /// <summary>
        /// Tests that near cache is updated from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesFromRemoteNode(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode1,
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode2)
        {
            var cache1 = GetCache<int, int>(mode1);
            var cache2 = GetCache<int, int>(mode2);

            cache1[1] = 1;
            cache2[1] = 2;

            Assert.True(TestUtils.WaitForCondition(() => cache1[1] == 2, 300));
        }

        /// <summary>
        /// Tests that near cache is updated from another cache instance after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheUpdatesFromAnotherLocalInstance()
        {
            var cache1 = _grid.GetCache<int, int>(CacheName);
            var cache2 = _grid.GetCache<int, int>(CacheName);

            cache1[1] = 1;
            cache2.Replace(1, 2);

            Assert.True(TestUtils.WaitForCondition(() => cache1[1] == 2, 300));
        }

        /// <summary>
        /// Tests that near cache is cleared from remote node after being populated with local Put call.
        /// </summary>
        [Test]
        public void TestNearCacheRemoveFromRemoteNodeAfterLocalPut()
        {
            var localCache = _client.GetOrCreateNearCache<int, int>(CacheName, new NearCacheConfiguration());
            
            var remoteCache = _grid.GetCache<int, int>(CacheName);

            localCache[1] = 1;
            remoteCache.Remove(1);

            int unused;
            Assert.True(TestUtils.WaitForCondition(() => !localCache.TryGet(1, out unused), 300));
        }

        /// <summary>
        /// Tests that primary keys are always up-to-date in .NET Near Cache.
        /// </summary>
        [Test]
        public void TestPrimaryKeyOnServerNodeIsAddedToNearCacheAfterRemotePut()
        {
            var clientCache = _client.GetCache<int, int>(CacheName);
            var serverCache = _grid.GetCache<int, int>(CacheName);

            var key = TestUtils.GetPrimaryKey(_grid, CacheName);

            clientCache[key] = 2;
            Assert.AreEqual(2, serverCache.LocalPeek(key, CachePeekMode.PlatformNear));
            
            clientCache[key] = 3;
            Assert.AreEqual(3, serverCache.LocalPeek(key, CachePeekMode.PlatformNear));

            var nonPrimaryNodeCache = _grid2.GetCache<int, int>(CacheName);
            Assert.AreEqual(0, nonPrimaryNodeCache.GetLocalSize(CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests that same near cache can be used with different sets of generic type parameters.
        /// </summary>
        [Test]
        public void TestSameNearCacheWithDifferentGenericTypeParameters()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };
            
            var cache1 = _grid.CreateCache<int, int>(cfg);
            var cache2 = _grid.GetCache<string, string>(cfg.Name);
            var cache3 = _grid.GetCache<int, Foo>(cfg.Name);
            var cache4 = _grid.GetCache<object, object>(cfg.Name);

            cache1[1] = 1;
            cache2["1"] = "1";
            cache3[2] = new Foo(5);

            Assert.AreEqual(cache4[1], 1);
            Assert.AreEqual(cache4["1"], "1");
            Assert.AreSame(cache4[2], cache3[2]);
        }

        /// <summary>
        /// Tests that reference semantics is preserved on repeated get after generic downgrade.
        /// </summary>
        [Test]
        public void TestRepeatedGetReturnsSameInstanceAfterGenericDowngrade()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };
            
            var cache1 = _grid.CreateCache<int, Foo>(cfg);
            cache1[1] = new Foo(42);
            cache1[2] = new Foo(43);
            
            // Perform generic downgrade by using different type parameters.
            // Existing near cache data is thrown away.
            var cache2 = _grid.GetCache<int, string>(cfg.Name);
            cache2[1] = "x";

            // Check that near cache still works for old entries. 
            Assert.Throws<InvalidCastException>(() => cache1.Get(1));
            Assert.AreEqual(43, cache1[2].Bar);
            Assert.AreSame(cache1[2], cache1[2]);
        }

        /// <summary>
        /// Tests that cache data is invalidated in the existing cache instance after generic downgrade.
        /// </summary>
        [Test]
        public void TestDataInvalidationAfterGenericDowngrade()
        {
            var cacheName = TestUtils.TestName;
            var cfg = new CacheConfiguration
            {
                Name = cacheName,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            var cache = _client.CreateCache<int, int>(cfg, cfg.NearConfiguration);
            cache[1] = 1;

            var newCache = _client.GetOrCreateNearCache<int, object>(cacheName, cfg.NearConfiguration);
            newCache[1] = 2;

            Assert.AreEqual(2, cache[1]);
        }

        /// <summary>
        /// Tests that error during Put does not affect correct data in near cache.
        /// </summary>
        [Test]
        public void TestFailedPutKeepsCorrectNearCacheValue(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration(),
                CacheStoreFactory = new FailingCacheStore(),
                WriteThrough = true
            };

            var cache = GetIgnite(mode).CreateCache<int, Foo>(cfg, new NearCacheConfiguration(), 
                new PlatformNearCacheConfiguration());
            
            // First write succeeds.
            cache.Put(1, new Foo(1));
            Assert.AreEqual(1, cache.LocalPeek(1, CachePeekMode.PlatformNear).Bar);
            
            // Special value causes write failure. Near cache value is still correct.
            Assert.Throws<CacheStoreException>(() => cache.Put(1, new Foo(FailingCacheStore.FailingValue)));
            Assert.AreEqual(1, cache.LocalPeek(1, CachePeekMode.PlatformNear).Bar);
        }

        /// <summary>
        /// Tests that near cache is updated/invalidated by SQL DML operations.
        /// </summary>
        [Test]
        public void TestSqlUpdatesNearCache()
        {
            var cache = GetCache<int, Foo>(CacheTestMode.Client);

            var value = new Foo(5);
            cache[1] = value;
            
            cache.Query(new SqlFieldsQuery("update Foo set Bar = 7 where Bar = 5"));

            var res = cache[1];
            Assert.AreEqual(7, res.Bar);
        }

        /// <summary>
        /// Tests that eviction policy removes near cache data for the key. 
        /// </summary>
        [Test]
        public void TestFifoEvictionPolicyRemovesNearCacheValue(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cache = GetCache<int, Foo>(mode);
            
            TestEvictionPolicyRemovesNearCacheValue(mode, cache);
        }

        /// <summary>
        /// Tests that eviction policy removes near cache data for the key. 
        /// </summary>
        [Test]
        public void TestLruEvictionPolicyRemovesNearCacheValue(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cfg = new CacheConfiguration
            {
                Name = "lru-test-" + mode,
                NearConfiguration = new NearCacheConfiguration
                {
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = NearCacheMaxSize,
                        BatchSize = 1
                    },
                },
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            var ignite = GetIgnite(mode);
            var cache = ignite.CreateCache<int, Foo>(cfg, cfg.NearConfiguration);
            
            TestEvictionPolicyRemovesNearCacheValue(mode, cache);
        }

        /// <summary>
        /// Tests that last N added entries are in near cache, where N is MaxSize.
        /// </summary>
        [Test]
        public void TestEvictionPolicyKeepsLastEntriesInNearCache(
            [Values(true, false)] bool lruOrFifo, 
            [Values(true, false)] bool getOrCreate)
        {
            const int maxSize = 30;
            
            var serverCache = _grid.CreateCache<int, Foo>(TestUtils.TestName);

            var nearCfg = new NearCacheConfiguration
            {
                EvictionPolicy = lruOrFifo 
                    ? (IEvictionPolicy) new LruEvictionPolicy
                    {
                        MaxSize = maxSize
                    } 
                    : new FifoEvictionPolicy
                    {
                        MaxSize = maxSize
                    }
            };

            var platformCfg = new PlatformNearCacheConfiguration();

            var clientCache = getOrCreate 
                ? _client.GetOrCreateNearCache<int, Foo>(serverCache.Name, nearCfg, platformCfg)
                : _client.CreateNearCache<int, Foo>(serverCache.Name, nearCfg, platformCfg);

            var keys = Enumerable.Range(1, maxSize * 5).ToList();
            var nearKeys = keys.AsEnumerable().Reverse().Take(maxSize).ToArray();
            
            keys.ForEach(k => serverCache.Put(k, new Foo(k)));

            // Get from client to populate near cache.
            clientCache.GetAll(nearKeys);
            Assert.AreEqual(nearKeys.Length, clientCache.GetLocalSize(CachePeekMode.PlatformNear));

            // Check that Get returns instance from .NET near cache.
            foreach (var key in nearKeys)
            {
                Assert.AreSame(clientCache.LocalPeek(key, CachePeekMode.PlatformNear), clientCache.Get(key));
            }
            
            // Check that GetAll returns instances from .NET near cache.
            var all = clientCache.GetAll(nearKeys);
            foreach (var entry in all)
            {
                Assert.AreSame(clientCache.LocalPeek(entry.Key, CachePeekMode.PlatformNear), entry.Value);
            }
        }

        /// <summary>
        /// Tests that evicted entry is reloaded from Java after update from another node.
        /// Eviction on Java side for non-local entry (not a primary key for this node) disconnects near cache notifier.
        /// This test verifies that eviction on Java side causes eviction on .NET side, and does not cause stale data.
        /// </summary>
        [Test]
        public void TestCacheGetFromEvictedEntryAfterUpdateFromAnotherNode()
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                NearConfiguration = new NearCacheConfiguration
                {
                    EvictionPolicy = new FifoEvictionPolicy
                    {
                        MaxSize = 1
                    }
                },
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            var serverCache = _grid.CreateCache<int, int>(cfg);
            var clientCache = _client.GetOrCreateNearCache<int, int>(cfg.Name, cfg.NearConfiguration);
            
            clientCache[1] = 1;
            clientCache[2] = 2;
            serverCache[1] = 11;
            
            Assert.AreEqual(11, clientCache[1]);
        }

        [Test]
        public void TestScanQueryFilterUsesValueFromNearCache(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cache = GetCache<int, Foo>(mode);
            
            const int count = 100;
            cache.PutAll(Enumerable.Range(1, count).Select(x => new KeyValuePair<int, Foo>(x, new Foo(x))));

            // Filter will check that value comes from native near cache.
            var filter = new ScanQueryNearCacheFilter
            {
                CacheName = cache.Name
            };
            
            var res = cache.Query(new ScanQuery<int, Foo>(filter));
            
            Assert.AreEqual(count, res.Count());
        }

        [Test]
        public void TestScanQueryFilterUsesFallbackValueWhenNotInNearCache(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cache = GetCache<int, Foo>(mode);
            
            const int count = 100;
            var data = Enumerable.Range(1, count).ToDictionary(x=> x, x => new Foo(x));
            
            cache.PutAll(data);

            // Filter will check that value does not come from native near cache.
            var filter = new ScanQueryNoNearCacheFilter
            {
                CacheName = cache.Name
            };
            
            // Clear near cache using internal API.
            foreach (var ignite in Ignition.GetAll())
            {
                var nearCache = ((Ignite) ignite).NearCacheManager.TryGetNearCache(BinaryUtils.GetCacheId(cache.Name));

                if (nearCache != null)
                {
                    nearCache.Clear();
                }
            }
            
            var res = cache.Query(new ScanQuery<int, Foo>(filter));
            
            Assert.AreEqual(count, res.Count());
        }

        /// <summary>
        /// Tests that expiry policy functionality plays well with platform near cache.
        /// </summary>
        [Test]
        public void TestExpiryPolicyRemovesValuesFromNearCache(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode)
        {
            var cache = GetCache<int, Foo>(mode)
                .WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromSeconds(0.2), null, null));
            
            cache[1] = new Foo(1);
            
            Assert.AreEqual(1, cache[1].Bar);
            Assert.AreEqual(1, cache.LocalPeek(1, CachePeekMode.PlatformNear).Bar);
            Assert.AreEqual(1, cache.Count());

            Foo _;
            TestUtils.WaitForTrueCondition(() => !cache.TryLocalPeek(1, out _, CachePeekMode.PlatformNear), 3000);
        }

        /// <summary>
        /// Tests server-side near cache binary mode.
        /// </summary>
        [Test]
        public void TestKeepBinaryServer()
        {
            // Create server near cache with binary mode enabled.
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration
                {
                    KeepBinary = true
                }
            };
            
            var clientCache = _client.CreateCache<int, Foo>(cfg);
            var serverCache = _grid2.GetCache<int, object>(cfg.Name);
            Assert.IsTrue(serverCache.GetConfiguration().PlatformNearConfiguration.KeepBinary);
            
            // Put non-binary from client. There is no near cache on client.
            clientCache[1] = new Foo(2);
            
            // Read from near on server.
            var res = (IBinaryObject) serverCache.LocalPeek(1, CachePeekMode.PlatformNear);
            Assert.AreEqual(2, res.GetField<int>("Bar"));
        }

        [Test]
        public void TestMultithreadedConcurrentUpdates()
        {
            var localCache = GetCache<int, Foo>(CacheTestMode.Client);
            var remoteCache = GetCache<int, Foo>(CacheTestMode.ServerRemote);
            var cancel = false;
            const int key = 1;
            var id = 1;
            remoteCache[1] = new Foo(id);

            var localUpdater = Task.Factory.StartNew(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                while (!cancel)
                {
                    Interlocked.Increment(ref id);
                    localCache.Put(key, new Foo(id));
                }
            });

            var remoteUpdater = Task.Factory.StartNew(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                while (!cancel)
                {
                    Interlocked.Increment(ref id);
                    remoteCache.Put(key, new Foo(id));
                }
            });
            
            var localReader = Task.Factory.StartNew(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                while (!cancel)
                {
                    var cur = localCache[key].Bar;
                    Assert.GreaterOrEqual(id, cur);
                }
            });

            Thread.Sleep(5000);
            cancel = true;
            Task.WaitAll(localUpdater, remoteUpdater, localReader);

            // Get actual value with SQL to bypass caches.
            // Actual value may not be equal to the latest id because two threads compete in Put calls.
            var actualValue = (int) localCache.Query(new SqlFieldsQuery("select Bar from Foo")).GetAll()[0][0];
            
            Assert.AreEqual(actualValue, localCache[key].Bar, "Local value");
            Assert.AreEqual(actualValue, remoteCache[key].Bar, "Remote value");
        }

        /// <summary>
        /// Tests GetAll operation.
        /// </summary>
        /// <param name="async"></param>
        [Test]
        public void TestGetAll([Values(true, false)] bool async)
        {
            var clientCache = GetCache<int, Foo>(CacheTestMode.Client);
            var serverCache = GetCache<int, Foo>(CacheTestMode.ServerRemote);

            // One entry is in near cache, another is not.
            clientCache[1] = new Foo(1);
            serverCache[2] = new Foo(2);

            var res = async 
                ? clientCache.GetAllAsync(Enumerable.Range(1, 2)).Result 
                : clientCache.GetAll(Enumerable.Range(1, 2));

            Assert.AreEqual(new[] {1, 2}, res.Select(x => x.Key));

            // First entry is from near cache.
            Assert.AreSame(res.First().Value, clientCache.LocalPeek(1, CachePeekMode.PlatformNear));

            // Second entry is now in near cache.
            Assert.AreEqual(2, clientCache.LocalPeek(2, CachePeekMode.PlatformNear).Bar);
        }

        /// <summary>
        /// Tests LocalPeek / TryLocalPeek with platform near cache.
        /// </summary>
        [Test]
        public void TestLocalPeek()
        {
            var clientCache = GetCache<int, Foo>(CacheTestMode.Client);
            var serverCache = GetCache<int, Foo>(CacheTestMode.ServerRemote);

            // One entry is in client near cache, another is not.
            clientCache[1] = new Foo(1);
            serverCache[2] = new Foo(2);

            Foo foo;
            
            Assert.IsTrue(clientCache.TryLocalPeek(1, out foo, CachePeekMode.PlatformNear));
            Assert.AreEqual(1, foo.Bar);
            
            Assert.IsTrue(clientCache.TryLocalPeek(1, out foo, CachePeekMode.PlatformNear | CachePeekMode.Near));
            Assert.AreEqual(1, foo.Bar);
            
            Assert.IsFalse(clientCache.TryLocalPeek(2, out foo, CachePeekMode.PlatformNear));
            Assert.IsFalse(clientCache.TryLocalPeek(2, out foo, CachePeekMode.Near));
            Assert.IsFalse(clientCache.TryLocalPeek(2, out foo, CachePeekMode.All));
            
            Assert.AreEqual(2, serverCache.LocalPeek(2, CachePeekMode.PlatformNear).Bar);
            Assert.AreEqual(2, serverCache.LocalPeek(2, CachePeekMode.Near).Bar);
            
            Assert.AreSame(serverCache[2], serverCache.LocalPeek(2, CachePeekMode.PlatformNear));
            Assert.AreNotSame(serverCache[2], serverCache.LocalPeek(2, CachePeekMode.Near));
        }

        [Test]
        public void TestContainsKey(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode,
            [Values(true, false)] bool async)
        {
            var cache = GetCache<int, int>(mode);
            var cache2 = GetCache<int, int>(CacheTestMode.ServerLocal);
            
            var data = Enumerable.Range(1, 100).ToDictionary(x => x, x => x);
            cache2.PutAll(data);

            var act = async
                ? (Func<int, bool>) (k => cache.ContainsKeyAsync(k).Result) 
                : k => cache.ContainsKey(k);

            foreach (var key in data.Keys)
            {
                Assert.IsTrue(act(key));
                Assert.IsFalse(act(-key));
            }
        }

        [Test]
        public void TestContainsKeys(
            [Values(CacheTestMode.ServerLocal, CacheTestMode.ServerRemote, CacheTestMode.Client)] CacheTestMode mode,
            [Values(true, false)] bool async)
        {
            var cache = GetCache<int, int>(mode);
            var cache2 = GetCache<int, int>(CacheTestMode.ServerLocal);
            
            var data = Enumerable.Range(1, 100).ToDictionary(x => x, x => x);
            cache2.PutAll(data);
            
            var act = async
                ? (Func<IEnumerable<int>, bool>) (k => cache.ContainsKeysAsync(k).Result) 
                : k => cache.ContainsKeys(k);

            foreach (var key in data.Keys)
            {
                Assert.IsTrue(act(new[] {key}));
                Assert.IsFalse(act(new[] {-key}));
            }
            
            Assert.IsTrue(act(data.Keys));
            Assert.IsTrue(act(data.Keys.Take(10)));
            Assert.IsTrue(act(data.Keys.Skip(10)));
            Assert.IsFalse(act(data.Keys.Concat(new[] {-1})));
        }
        
        /// <summary>
        /// Tests local size on server node.
        /// </summary>
        [Test]
        public void TestGetLocalSizeServer()
        {
            var cache = GetCache<int, int>(CacheTestMode.ServerRemote, TestUtils.TestName);
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.PlatformNear));
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.All));

            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));

            var primary = cache.GetLocalSize(CachePeekMode.Primary);

            Assert.AreEqual(NearCacheMaxSize, cache.GetLocalSize(CachePeekMode.Near));
            Assert.AreEqual(NearCacheMaxSize + primary, cache.GetLocalSize(CachePeekMode.PlatformNear));
            Assert.AreEqual(NearCacheMaxSize * 2 + primary,
                cache.GetLocalSize(CachePeekMode.Near | CachePeekMode.PlatformNear));
            Assert.AreEqual(NearCacheMaxSize * 2 + primary,
                cache.GetLocalSize(CachePeekMode.Near, CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests local size on client node.
        /// </summary>
        [Test]
        public void TestGetLocalSizeClient()
        {
            var cache = GetCache<int, int>(CacheTestMode.Client, TestUtils.TestName);
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.PlatformNear));
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.All));
            
            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));

            Assert.AreEqual(0, cache.GetLocalSize());
            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.Primary | CachePeekMode.Backup));
            Assert.AreEqual(NearCacheMaxSize, cache.GetLocalSize(CachePeekMode.Near));
            Assert.AreEqual(NearCacheMaxSize, cache.GetLocalSize(CachePeekMode.PlatformNear));
            Assert.AreEqual(NearCacheMaxSize * 2, cache.GetLocalSize(CachePeekMode.All));
            Assert.AreEqual(NearCacheMaxSize, cache.GetLocalSize(CachePeekMode.PlatformNear, CachePeekMode.Primary));
            Assert.AreEqual(NearCacheMaxSize, cache.GetLocalSize(CachePeekMode.PlatformNear | CachePeekMode.Primary));
            Assert.AreEqual(NearCacheMaxSize * 2, cache.GetLocalSize(CachePeekMode.Near | CachePeekMode.PlatformNear));
            Assert.AreEqual(NearCacheMaxSize * 2, cache.GetLocalSize(CachePeekMode.Near, CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests that <see cref="CachePeekMode.PlatformNear"/> works with distributed GetSize overloads.
        /// </summary>
        [Test]
        public void TestGetSizeWithPlatformNear([Values(true, false)] bool longMode, 
            [Values(true, false)] bool async)
        {
            var cache = GetCache<int, int>(CacheTestMode.Client, TestUtils.TestName);
            var primaryAndPlatform = new[] {CachePeekMode.Primary, CachePeekMode.PlatformNear};
            var platform = new[] {CachePeekMode.PlatformNear};
            var all = new[] {CachePeekMode.All};
            
            var func =
                longMode
                    ? async
                        ? (Func<CachePeekMode[], long>) (m => cache.GetSizeLongAsync(m).Result)
                        : m => cache.GetSizeLong(m)
                    : async
                        ? (Func<CachePeekMode[], long>) (m => cache.GetSizeAsync(m).Result)
                        : m => cache.GetSize(m); 

            Assert.AreEqual(0, func(all));
            Assert.AreEqual(0, func(platform));

            cache[1] = 2;
            Assert.AreEqual(1, func(platform));
            Assert.AreEqual(2, func(primaryAndPlatform));
            Assert.AreEqual(3, func(all));

            cache[2] = 3;
            Assert.AreEqual(2, func(platform));
            Assert.AreEqual(4, func(primaryAndPlatform));
            Assert.AreEqual(6, func(all));
        }

        /// <summary>
        /// Tests that <see cref="CachePeekMode.PlatformNear"/> works with distributed GetSize overloads
        /// with specific partition.
        /// </summary>
        [Test]
        public void TestGetSizeWithPlatformNearAndPartition([Values(true, false)] bool async)
        {
            var cache = GetCache<int, int>(CacheTestMode.Client, TestUtils.TestName);
            var primaryAndPlatform = new[] {CachePeekMode.Primary, CachePeekMode.PlatformNear};
            var platform = new[] {CachePeekMode.PlatformNear};
            var all = new[] {CachePeekMode.All};

            var func = async
                ? (Func<int, CachePeekMode[], long>) ((p, m) => cache.GetSizeLongAsync(p, m).Result)
                : (p, m) => cache.GetSizeLong(p, m);

            const int key = 1;
            var part = _grid.GetAffinity(cache.Name).GetPartition(key);

            Assert.AreEqual(0, func(part, all));
            Assert.AreEqual(0, func(part, platform));

            cache[1] = 2;
            Assert.AreEqual(1, func(part, platform));
            Assert.AreEqual(2, func(part, primaryAndPlatform));
            Assert.AreEqual(2, func(part, all));
            
            Assert.AreEqual(0, func(part + 1, platform));
            Assert.AreEqual(0, func(part + 1, primaryAndPlatform));
            Assert.AreEqual(0, func(part + 1, all));

            cache[2] = 3;
            Assert.AreEqual(1, func(part, platform));
            Assert.AreEqual(2, func(part, primaryAndPlatform));
            Assert.AreEqual(2, func(part, all));
        }

        /// <summary>
        /// Tests <see cref="ICache{TK,TV}.GetLocalEntries"/> with <see cref="CachePeekMode.PlatformNear"/>.
        /// </summary>
        [Test]
        public void TestGetLocalEntriesNearOnly()
        {
            var cache = GetCache<int, Foo>(CacheTestMode.Client, TestUtils.TestName);
            var keys = Enumerable.Range(1, 3).ToArray();
            cache.PutAll(keys.ToDictionary(x => x, x => new Foo(x)));

            var localEntries = cache.GetLocalEntries(CachePeekMode.PlatformNear).ToArray();
            
            // Same set of keys.
            CollectionAssert.AreEquivalent(keys, localEntries.Select(e => e.Key));
            
            // Returns same instances every time.
            CollectionAssert.AreEqual(localEntries.Select(e => e.Value),
                cache.GetLocalEntries(CachePeekMode.PlatformNear).Select(e => e.Value));
            
            // Every instance is from near cache.
            foreach (var entry in localEntries)
            {
                Assert.AreSame(entry.Value, cache[entry.Key]);
            }
        }

        /// <summary>
        /// Tests <see cref="ICache{TK,TV}.GetLocalEntries"/> with various modes.
        /// </summary>
        [Test]
        public void TestGetLocalEntriesCombinedModes()
        {
            var cache = GetCache<int, Foo>(CacheTestMode.ServerLocal, TestUtils.TestName);
            var keys = Enumerable.Range(1, 100).ToArray();
            cache.PutAll(keys.ToDictionary(x => x, x => new Foo(x)));

            Func<CachePeekMode, int[]> getKeys = mode =>
                cache.GetLocalEntries(mode).Select(e => e.Key).OrderBy(k => k).ToArray();

            var primary = getKeys(CachePeekMode.Primary);
            var near = getKeys(CachePeekMode.Near);
            var platformNear = getKeys(CachePeekMode.PlatformNear);
            var all = getKeys(CachePeekMode.All);
            var all2 = getKeys(CachePeekMode.Primary | CachePeekMode.Near | CachePeekMode.PlatformNear);
            
            CollectionAssert.AreEqual(all, all2);
            CollectionAssert.AreEquivalent(all, platformNear.Concat(primary).Concat(near));
            CollectionAssert.AreEquivalent(platformNear, primary.Concat(near));
        }

        /// <summary>
        /// Tests that backup entries are reflected in Platform Near Cache.
        /// </summary>
        [Test]
        public void TestNearCachingWithBackups()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                CacheMode = CacheMode.Partitioned,
                Backups = 1,
                PlatformNearConfiguration = new PlatformNearCacheConfiguration(),
                WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync
            };

            var cache1 = _grid.CreateCache<int, int>(cfg);
            var cache2 = _grid2.GetCache<int, int>(cfg.Name);

            const int count = 100;
            cache1.PutAll(Enumerable.Range(1, count).ToDictionary(x => x, x => x));
            
            Assert.AreEqual(count, cache1.GetLocalSize(CachePeekMode.PlatformNear));
            Assert.AreEqual(count, cache2.GetLocalSize(CachePeekMode.PlatformNear));
            
            Assert.AreEqual(42, cache1.LocalPeek(42, CachePeekMode.PlatformNear));
            Assert.AreEqual(42, cache2.LocalPeek(42, CachePeekMode.PlatformNear));

            cache1[42] = -42;
            Assert.AreEqual(-42, cache1.LocalPeek(42, CachePeekMode.PlatformNear));
            Assert.AreEqual(-42, cache2.LocalPeek(42, CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests that Replicated cache puts all entries on all nodes to Platform Near.
        /// </summary>
        [Test]
        public void TestNearCachingReplicated()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                CacheMode = CacheMode.Replicated,
                PlatformNearConfiguration = new PlatformNearCacheConfiguration(),
                WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync
            };

            var cache1 = _grid.CreateCache<int, int>(cfg);
            var cache2 = _grid2.GetCache<int, int>(cfg.Name);

            const int count = 100;
            cache1.PutAll(Enumerable.Range(1, count).ToDictionary(x => x, x => x));
            
            Assert.AreEqual(count, cache1.GetLocalSize(CachePeekMode.PlatformNear));
            Assert.AreEqual(count, cache2.GetLocalSize(CachePeekMode.PlatformNear));
            
            Assert.AreEqual(42, cache1.LocalPeek(42, CachePeekMode.PlatformNear));
            Assert.AreEqual(42, cache2.LocalPeek(42, CachePeekMode.PlatformNear));

            cache1[42] = -42;
            Assert.AreEqual(-42, cache1.LocalPeek(42, CachePeekMode.PlatformNear));
            Assert.AreEqual(-42, cache2.LocalPeek(42, CachePeekMode.PlatformNear));
        }

        /// <summary>
        /// Tests that active transaction disables near cache.
        /// </summary>
        [Test]
        public void TestNearCacheBypassedWithinTransaction()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                AtomicityMode = CacheAtomicityMode.Transactional,
                PlatformNearConfiguration = new PlatformNearCacheConfiguration(),
                NearConfiguration = new NearCacheConfiguration()
            };

            var cache = _grid.CreateCache<int, Foo>(cfg);

            cache[1] = new Foo(2);
            var foo = cache[1];
            
            Assert.AreEqual(2, foo.Bar);
            Assert.AreSame(foo, cache[1]);

            using (_grid.GetTransactions().TxStart())
            {
                Assert.AreNotSame(foo, cache[1]);
            }
            
            Assert.AreSame(foo, cache[1]);

            using (new TransactionScope())
            {
                cache[2] = new Foo(3);
                Assert.AreNotSame(foo, cache[1]);
            }
            
            Assert.AreSame(foo, cache[1]);
        } 

        /// <summary>
        /// Tests near cache misconfiguration / type mismatch.
        /// </summary>
        [Test]
        public void TestNearCacheTypeMismatchLogsErrorAndUpdatesMainCache()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                PlatformNearConfiguration = new PlatformNearCacheConfiguration
                {
                    KeyTypeName = typeof(int).FullName,
                    ValueTypeName = typeof(Guid).FullName
                }
            };
            
            var nearCfg = new NearCacheConfiguration();

            var clientCache = _client.CreateCache<int, Foo>(cfg, nearCfg);
            var serverCache = _grid.GetCache<int, Foo>(cfg.Name);

            // Put Foo, but near cache expects Guid.
            clientCache.GetAndPut(1, new Foo(2));
            
            // Entry is not in near cache.
            Assert.AreEqual(0, clientCache.GetLocalSize(CachePeekMode.PlatformNear));

            // Ignite cache is updated.
            Assert.AreEqual(2, serverCache[1].Bar);
            Assert.AreEqual(2, clientCache[1].Bar);

            // Error is logged.
            Func<ListLogger.Entry> getEntry = () => 
                _logger.Entries.FirstOrDefault(e => e.Message == "Failure in Java callback");

            var message = string.Join(" | ", _logger.Entries.Select(e => e.Message));
            TestUtils.WaitForTrueCondition(() => getEntry() != null, 3000, message);
        }

        /// <summary>
        /// <see cref="ICache{TK,TV}.LoadCache"/> uses same filter mechanism as <see cref="ScanQuery{TK,TV}"/>.
        /// Near cache should never be used for cache store load filters.  
        /// </summary>
        [Test]
        public void TestCacheStoreLoadFilterDoesNotUseNearCache()
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                NearConfiguration = new NearCacheConfiguration(),
                PlatformNearConfiguration = new PlatformNearCacheConfiguration(),
                CacheStoreFactory = new FailingCacheStore()
            };

            var cache = _grid.CreateCache<int, Foo>(cfg);

            // Put a value to be overwritten from store.
            var foo = FailingCacheStore.Foo;
            var key = foo.Bar;
            
            cache[key] = new Foo(1);
            
            // Filter asserts that values do not come from near cache.
            var filter = new StoreNoNearCacheFilter
            {
                CacheName = cache.Name
            };

            cache.LoadCache(filter);
        }

        /// <summary>
        /// Tests near cache with different eviction configuration on client and server nodes.
        /// </summary>
        [Test]
        public void TestDifferentEvictionPoliciesOnClientAndServer()
        {
            const int serverMaxSize = 4;
            const int clientMaxSize = serverMaxSize * 3;
            
            var serverCfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                NearConfiguration = new NearCacheConfiguration
                {
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = serverMaxSize
                    }
                }
            };
            
            var clientCfg = new NearCacheConfiguration
            {
                EvictionPolicy = new FifoEvictionPolicy
                {
                    MaxSize = clientMaxSize
                }
            };
            
            var platformCfg = new PlatformNearCacheConfiguration();

            var serverCache = _grid.CreateCache<int, int>(serverCfg);
            var clientCache = _client.CreateNearCache<int, int>(serverCache.Name, clientCfg, platformCfg);

            var keys = Enumerable.Range(1, 100).ToList();
            
            keys.ForEach(k => clientCache.Put(k, k));
            
            Assert.AreEqual(clientMaxSize, clientCache.GetLocalSize(CachePeekMode.Near));
            Assert.AreEqual(clientMaxSize, clientCache.GetLocalSize(CachePeekMode.PlatformNear));

            var expectedKeys = keys.AsEnumerable().Reverse().Take(clientMaxSize).ToArray();
            var nearKeys = 
                clientCache.GetLocalEntries(CachePeekMode.PlatformNear).Select(e => e.Key).ToArray();
            
            CollectionAssert.AreEquivalent(expectedKeys, nearKeys);
        }

        /// <summary>
        /// Tests that <see cref="IDataStreamer{TK,TV}"/> updates near cache.
        /// </summary>
        [Test]
        public void TestDataStreamerUpdatesNearCache()
        {
            const int entryCount = 10000;
            
            var serverCfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                NearConfiguration = new NearCacheConfiguration
                {
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = entryCount
                    }
                },
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };
            
            var clientCfg = new NearCacheConfiguration
            {
                EvictionPolicy = new LruEvictionPolicy
                {
                    MaxSize = entryCount
                }
            };
            
            var platformCfg = new PlatformNearCacheConfiguration();
            
            var serverCache = _grid.CreateCache<int, int>(serverCfg);
            var clientCache = _client.CreateNearCache<int, int>(serverCache.Name, clientCfg, platformCfg);

            var data = Enumerable.Range(1, entryCount).ToDictionary(x => x, x => x);
            serverCache.PutAll(data);

            // Get data on client.
            var res = clientCache.GetAll(data.Keys);
            Assert.AreEqual(entryCount, res.Count);
            
            // Check that all entries are in near cache on client.
            Assert.AreEqual(entryCount, clientCache.GetLocalSize(CachePeekMode.Near));
            Assert.AreEqual(entryCount, clientCache.GetLocalSize(CachePeekMode.PlatformNear));

            // Update all entries with streamer.
            using (var streamer = _grid2.GetDataStreamer<int, int>(serverCache.Name))
            {
                streamer.AllowOverwrite = true;
                
                foreach (var entry in data)
                {
                    streamer.AddData(entry.Key, entry.Value + 1);
                }
            }
            
            // Verify that near cache contains updated entries.
            foreach (var entry in data)
            {
                var key = entry.Key;
                var val = entry.Value + 1;

                TestUtils.WaitForTrueCondition(() => val == clientCache.LocalPeek(key, CachePeekMode.PlatformNear), 
                    message: string.Format("{0} = {1}", key, val));
                
                TestUtils.WaitForTrueCondition(() => val == serverCache.LocalPeek(key, CachePeekMode.PlatformNear), 
                    message: string.Format("{0} = {1}", key, val));
            }
        }

        /// <summary>
        /// Gets the cache instance.
        /// </summary>
        private ICache<TK, TV> GetCache<TK, TV>(CacheTestMode mode, string name = CacheName)
        {
            var nearConfiguration = _grid.GetCache<TK, TV>(CacheName).GetConfiguration().NearConfiguration;
            var cacheConfiguration = new CacheConfiguration
            {
                NearConfiguration = nearConfiguration, 
                Name = name,
                PlatformNearConfiguration = new PlatformNearCacheConfiguration()
            };

            return GetIgnite(mode).GetOrCreateCache<TK, TV>(cacheConfiguration, nearConfiguration);
        }

        /// <summary>
        /// Gets Ignite instance for mode.
        /// </summary>
        private IIgnite GetIgnite(CacheTestMode mode)
        {
            return new[] {_grid, _grid2, _client}[(int) mode];
        }

        /// <summary>
        /// Tests that eviction policy removes near cache data for the key. 
        /// </summary>
        private void TestEvictionPolicyRemovesNearCacheValue(CacheTestMode mode, ICache<int, Foo> cache)
        {
            // Use non-primary keys: primary keys are not evicted.
            var items = TestUtils
                .GetKeys(GetIgnite(mode), cache.Name, primary: false)
                .Take(NearCacheMaxSize + 1)
                .Select(x => new Foo(x))
                .ToArray();

            var cachedItems = new List<Foo>();

            foreach (var item in items)
            {
                cache[item.Bar] = item;
                cachedItems.Add(cache[item.Bar]);
            }
            
            // Recent items are in near cache:
            Assert.AreEqual(NearCacheMaxSize, cache.GetLocalSize(CachePeekMode.PlatformNear));
            foreach (var item in cachedItems.Skip(items.Length - NearCacheMaxSize))
            {
                Assert.AreSame(item, cache[item.Bar]);
            }

            // First item is not in near cache and is deserialized on get:
            var localItem = items[0];
            var key = localItem.Bar;

            Foo _;
            Assert.IsFalse(cache.TryLocalPeek(key, out _, CachePeekMode.PlatformNear));

            var fromCache = cache[key];
            Assert.AreNotSame(localItem, fromCache);

            // And now it is near again:
            Assert.IsTrue(cache.TryLocalPeek(key, out _, CachePeekMode.PlatformNear));
            Assert.AreSame(cache[key], cache[key]);
        }

        private void WaitForRebalance()
        {
            TestUtils.WaitForTrueCondition(() => _grid2.GetAffinity(CacheName).MapKeyToNode(1).IsLocal, 2000);
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

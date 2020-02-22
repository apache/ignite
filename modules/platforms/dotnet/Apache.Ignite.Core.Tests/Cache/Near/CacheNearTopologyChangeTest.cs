/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ReSharper disable AccessToModifiedClosure
namespace Apache.Ignite.Core.Tests.Cache.Near
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;
    using Apache.Ignite.Core.Lifecycle;
    using NUnit.Framework;

    /// <summary>
    /// Tests Near Cache behavior when cluster topology changes.
    /// </summary>
    public class CacheNearTopologyChangeTest
    {
        /** */
        private const string CacheName = "c";

        /** Key that is primary on node3 */
        private const int Key3 = 6;

        /** */
        private const int MaxNodes = 10;

        /** */
        private IIgnite[] _ignite;

        /** */
        private ICache<int, Foo>[] _cache;

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }
        
        /// <summary>
        /// Tests that near cache is cleared for the key when primary node leaves.
        /// </summary>
        [Test]
        public void TestPrimaryNodeLeaveClearsNearCache()
        {
            InitNodes(3);
            var clientCache = InitClientAndCache();

            _cache[0][Key3] = new Foo(Key3);

            Assert.AreSame(_cache[0].Get(Key3), _cache[0].Get(Key3));
            Assert.AreSame(_cache[1].Get(Key3), _cache[1].Get(Key3));
            Assert.AreEqual(Key3, _cache[0][Key3].Bar);
            Assert.AreEqual(Key3, _cache[1][Key3].Bar);
            Assert.AreEqual(Key3, clientCache[Key3].Bar);

            // Stop primary node for Key3.
            _ignite[2].Dispose();
            Assert.IsTrue(_ignite[0].WaitTopology(3));

            // Check that key is not stuck in near cache.
            TestUtils.WaitForTrueCondition(() => !_cache[0].ContainsKey(Key3));
            Assert.Throws<KeyNotFoundException>(() => _cache[0].Get(Key3));
            Assert.Throws<KeyNotFoundException>(() => _cache[1].Get(Key3));
            Assert.Throws<KeyNotFoundException>(() => clientCache.Get(Key3));
            
            // Check that updates for that key work on all nodes.
            _cache[0][Key3] = new Foo(1);
            Assert.AreEqual(1, _cache[0][Key3].Bar);
            Assert.AreEqual(1, _cache[1][Key3].Bar);
            Assert.AreEqual(1, clientCache[Key3].Bar);
            Assert.AreSame(_cache[0][Key3], _cache[0][Key3]);
            Assert.AreSame(_cache[1][Key3], _cache[1][Key3]);
            Assert.AreSame(clientCache[Key3], clientCache[Key3]);
            
            _cache[1][Key3] = new Foo(2);
            TestUtils.WaitForTrueCondition(() => _cache[0][Key3].Bar == 2, 500);
            Assert.AreEqual(2, _cache[0][Key3].Bar);
            Assert.AreEqual(2, _cache[1][Key3].Bar);
            Assert.AreEqual(2, clientCache[Key3].Bar);
            Assert.AreSame(_cache[0][Key3], _cache[0][Key3]);
            Assert.AreSame(_cache[1][Key3], _cache[1][Key3]);
            Assert.AreSame(clientCache[Key3], clientCache[Key3]);
        }

        /// <summary>
        /// Tests that near cache works correctly after primary node changes for a given key.
        /// GridNearCacheEntry -> GridDhtCacheEntry.
        /// </summary>
        [Test]
        public void TestPrimaryNodeChangeClearsNearCacheDataOnServer()
        {
            InitNodes(2);
            
            _cache[0][Key3] = new Foo(-1);
            for (var i = 0; i < 2; i++)
            {
                TestUtils.WaitForTrueCondition(() => _cache[i][Key3].Bar == -1);
            }

            // New node enters and becomes primary for the key.
            InitNode(2);
            
            // GridCacheNearEntry does not yet exist on old primary node, so near cache data is removed on .NET side.
            Foo foo;
            Assert.IsFalse(_cache[0].TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            Assert.IsFalse(_cache[1].TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            
            // Check value on the new node.
            Assert.AreEqual(-1, _cache[2][Key3].Bar);
            Assert.AreSame(_cache[2][Key3], _cache[2][Key3]);
            
            // Check that updates are propagated to all server nodes.
            _cache[2][Key3] = new Foo(3);
            
            for (var i = 0; i < 3; i++)
            {
                TestUtils.WaitForTrueCondition(() => _cache[i][Key3].Bar == 3);
                Assert.AreSame(_cache[i][Key3], _cache[i][Key3]);
            }
        }
        
        /// <summary>
        /// Tests that near cache works correctly on client node after primary node changes for a given key.
        /// </summary>
        [Test]
        public void TestPrimaryNodeChangeClearsNearCacheDataOnClient()
        {
            InitNodes(2, serverNear: false);
            var clientCache = InitClientAndCache();

            _cache[0][Key3] = new Foo(-1);

            var clientInstance = clientCache[Key3];
            Assert.AreEqual(-1, clientInstance.Bar);
            
            // New node enters and becomes primary for the key.
            InitNode(2);
            
            // Client node cache is cleared.
            Foo foo;
            Assert.IsFalse(clientCache.TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            
            // Updates are propagated to client near cache.
            _cache[2][Key3] = new Foo(3);
            
            TestUtils.WaitForTrueCondition(() => clientCache[Key3].Bar == 3);
            Assert.IsTrue(clientCache.TryLocalPeek(Key3, out foo, CachePeekMode.NativeNear));
            Assert.AreNotSame(clientInstance, foo);
            Assert.AreEqual(3, foo.Bar);
        }

        /// <summary>
        /// Tests that when new server node joins, near cache data is retained for all keys
        /// that are NOT moved to a new server.
        /// </summary>
        [Test]
        public void TestServerNodeJoinDoesNotAffectNonPrimaryKeysInNearCache()
        {
            var key = 0;
            InitNodes(2);
            var clientCache = InitClientAndCache();
            var serverCache = _cache[0];
            
            serverCache[key] = new Foo(-1);
            
            var serverInstance = serverCache[key];
            var clientInstance = clientCache[key];
            
            // New node enters, but key stays on the same primary node.
            InitNode(2);

            Assert.AreSame(clientInstance, clientCache[key]);
            Assert.AreSame(serverInstance, serverCache[key]);
            
            Assert.AreEqual(-1, clientInstance.Bar);
            Assert.AreEqual(-1, serverInstance.Bar);
        }

        /// <summary>
        /// Tests that client node entering topology does not cause any near caches invalidation.
        /// </summary>
        [Test]
        public void TestClientNodeJoinOrLeaveDoesNotAffectNearCacheDataOnOtherNodes()
        {
            InitNodes(2);
            _cache[2] = InitClientAndCache();
            
            TestUtils.WaitForTrueCondition(() => TestUtils.GetPrimaryKey(_ignite[1], CacheName) == 1, 3000);

            Action<Action<ICache<int, Foo>, int>> forEachCacheAndKey = act =>
            {
                for (var gridIdx = 0; gridIdx < 3; gridIdx++)
                {
                    var cache = _cache[gridIdx];

                    for (var key = 0; key < 100; key++)
                    {
                        act(cache, key);
                    }
                }
            };

            Action<int> putData = offset => forEachCacheAndKey((cache, key) => cache[key] = new Foo(key + offset));
            
            Action<int> checkNearData = offset => forEachCacheAndKey((cache, key) => 
                Assert.AreEqual(key + offset, cache.LocalPeek(key, CachePeekMode.NativeNear).Bar));

            // Put data and verify near cache.
            putData(0);
            checkNearData(0);

            // Start new client node, check near data, stop client node, check near data.
            using (InitClient())
            {
                checkNearData(0);
                putData(1);
                checkNearData(1);
            }

            checkNearData(1);
            putData(2);
            checkNearData(2);
            
            Assert.AreEqual(3, _cache[0][1].Bar);
        }

        /// <summary>
        /// Test multiple topology changes.
        /// </summary>
        [Test]
        public void TestContinuousTopologyChangeMaintainsCorrectNearCacheData([Values(0, 1, 2)] int backups)
        {
            // Start 5 servers and 1 client.
            // Server 0 and client node always run
            // Other servers start and stop periodically.
            InitNodes(5, backups: backups);
            var clientCache = InitClientAndCache();
            var serverCache = _cache[0];
            var rnd = new Random();
            var val = 1;
            var key = 1;
            var timeout = 5000;
            serverCache[key] = new Foo(val);

            var start = DateTime.Now;
            while (DateTime.Now - start < TimeSpan.FromSeconds(30))
            {
                // Change topology randomly.
                var idx = rnd.Next(1, 5);
                Console.WriteLine(">>> Changing topology: " + idx);
                
                if (_ignite[idx] == null)
                {
                    InitNode(idx, waitForPrimary: false);
                }
                else
                {
                    StopNode(idx);
                }

                var dataLost = serverCache.GetSize(CachePeekMode.Primary | CachePeekMode.Backup) == 0;
                var status = string.Format("Node {0}: {1}, data lost: {2}, current val: {3}",
                    _ignite[idx] == null ? "stopped" : "started", idx, dataLost, val);
                
                Console.WriteLine(">>> " + status);
                
                // Verify data.
                if (dataLost)
                {
                    TestUtils.WaitForTrueCondition(() => !serverCache.ContainsKey(key), timeout, status);
                    TestUtils.WaitForTrueCondition(() => !clientCache.ContainsKey(key), timeout, status);
                }
                else
                {
                    Assert.AreEqual(val, serverCache[key].Bar, status);
                    Assert.AreEqual(val, clientCache[key].Bar, status);
                }
                
                // Update data and verify.
                val++;
                (val % 2 == 0 ? serverCache : clientCache)[key] = new Foo(val);

                TestUtils.WaitForTrueCondition(() => val == serverCache[key].Bar, timeout, status);
                TestUtils.WaitForTrueCondition(() => val == clientCache[key].Bar, timeout, status);
            }
        }

        /// <summary>
        /// Tests that client reconnect to a restarted cluster stops near cache.
        /// </summary>
        [Test]
        public void TestClientNodeReconnectWithClusterRestartStopsNearCache()
        {
            InitNodes(1);
            var clientCache = InitClientAndCache();
            var client = Ignition.GetAll().Single(c => c.GetConfiguration().ClientMode);
            
            var keys = Enumerable.Range(1, 100).ToList();
            keys.ForEach(k => clientCache[k] = new Foo(k));
            Assert.AreEqual(keys.Count, clientCache.GetLocalSize(CachePeekMode.NativeNear));
            Assert.IsNotNull(clientCache.GetConfiguration().NearConfiguration);
            
            // Stop the only server node, client goes into disconnected mode.
            var evt = new ManualResetEventSlim(false);
            client.ClientDisconnected += (sender, args) => evt.Set(); 
            
            StopNode(0);
            Assert.IsTrue(evt.Wait(TimeSpan.FromSeconds(10)), "ClientDisconnected event should be fired");
            
            var reconnectTask = client.GetCluster().ClientReconnectTask;
            
            // Start server again, client reconnects.
            InitNodes(1);
            Assert.IsTrue(reconnectTask.Wait(TimeSpan.FromSeconds(10)));
            
            // Near cache is empty.
            Assert.AreEqual(0, clientCache.GetLocalSize(CachePeekMode.NativeNear));
            
            // Cache still works for new entries, near cache is being bypassed.
            var serverCache = _cache[0];
            
            serverCache[1] = new Foo(11);
            Assert.AreEqual(11, clientCache[1].Bar);
            
            serverCache[1] = new Foo(22);
            
            var foo = clientCache[1];
            Assert.AreEqual(22, foo.Bar);
            Assert.AreNotSame(foo, clientCache[1]);
            
            // This is a full cluster restart, so client near cache is stopped. 
            Assert.IsNull(clientCache.GetConfiguration().NearConfiguration);
            
            var ex = Assert.Throws<CacheException>(() =>
                client.GetOrCreateNearCache<int, Foo>(clientCache.Name, new NearCacheConfiguration(true)));
            StringAssert.Contains("cache with the same name without near cache is already started", ex.Message);
        }

        /// <summary>
        /// Tests client reconnect to the same cluster (no cluster restart).
        /// </summary>
        [Test]
        public void TestClientNodeReconnectWithoutClusterRestartKeepsNearCache()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CommunicationSpi = new TcpCommunicationSpi {IdleConnectionTimeout = TimeSpan.FromSeconds(2)},
                FailureDetectionTimeout = TimeSpan.FromSeconds(2),
                ClientFailureDetectionTimeout = TimeSpan.FromSeconds(2),
                IgniteInstanceName = "srv"
            };
            var server = Ignition.Start(cfg);
            var serverCache = server.CreateCache<int, Foo>(CacheName);

            var clientCfg = new IgniteConfiguration(cfg)
            {
                ClientMode = true,
                IgniteInstanceName = "client"
            };
            var client = Ignition.Start(clientCfg);

            var clientCache = client.GetOrCreateNearCache<int, Foo>(CacheName, new NearCacheConfiguration(true));
            clientCache[1] = new Foo(2);
            Assert.AreEqual(2, clientCache.LocalPeek(1, CachePeekMode.NativeNear).Bar);
            
            PerformClientReconnect(client);
            
            // Near cache data is removed after disconnect.
            Assert.AreEqual(0, clientCache.GetLocalSize(CachePeekMode.NativeNear));
            
            // Updates work as expected.
            Assert.AreEqual(2, clientCache[1].Bar);
            serverCache[1] = new Foo(33);
            TestUtils.WaitForTrueCondition(() => 33 == clientCache.LocalPeek(1, CachePeekMode.NativeNear).Bar);
        }

        /// <summary>
        /// Checks that near cache performance is adequate after topology change.
        /// (Topology change causes additional entry validation).
        /// </summary>
        [Test]
        public void TestNearCacheTopologyVersionValidationPerformance()
        {
            InitNodes(1);
            var cache = InitClientAndCache();
            
            cache[1] = new Foo(1);
            
            // Change topology by starting a new cache.
            _ignite[0].CreateCache<int, int>("x");

            var foo = cache.Get(1);
            Assert.AreEqual(1, foo.Bar);
            
            // Warmup.
            for (var i = 0; i < 100; i++)
            {
                cache.Get(1);
            }

            const int count = 1000000;
            var sw = Stopwatch.StartNew();
            
            for (var i = 0; i < count; i++)
            {
                var res = cache.Get(1);
                if (!ReferenceEquals(res, foo))
                {
                    Assert.Fail();
                }
            }
            
            var elapsed = sw.Elapsed;
            Assert.Less(elapsed, TimeSpan.FromSeconds(5));
            
            Console.WriteLine(">>> Retrieved {0} entries in {1}.", count, elapsed);
        }

        /// <summary>
        /// Inits a number of grids.
        /// </summary>
        private void InitNodes(int count, bool serverNear = true, int backups = 0)
        {
            Debug.Assert(count < MaxNodes);
            
            _ignite = new IIgnite[MaxNodes];
            _cache = new ICache<int, Foo>[MaxNodes];

            for (var i = 0; i < count; i++)
            {
                InitNode(i, serverNear, backups: backups);
            }
        }

        /// <summary>
        /// Inits a grid.
        /// </summary>
        private void InitNode(int i, bool serverNear = true, bool waitForPrimary = true, int backups = 0)
        {
            var cacheConfiguration = new CacheConfiguration(CacheName)
            {
                NearConfiguration = serverNear ? new NearCacheConfiguration(true) : null,
                Backups = backups
            };
            
            _ignite[i] = Ignition.Start(TestUtils.GetTestConfiguration(name: "node" + i));
            _cache[i] = _ignite[i].GetOrCreateCache<int, Foo>(cacheConfiguration);
            
            if (i == 2 && waitForPrimary)
            {
                // ReSharper disable once AccessToDisposedClosure
                TestUtils.WaitForTrueCondition(() => TestUtils.GetPrimaryKey(_ignite[2], CacheName) == Key3, 3000);
            }
        }
        
        /// <summary>
        /// Inits a client node and a near cache on it.
        /// </summary>
        private static ICache<int, Foo> InitClientAndCache()
        {
            var client = InitClient();

            return client.CreateNearCache<int, Foo>(CacheName, new NearCacheConfiguration(true));
        }

        /// <summary>
        /// Inits a client node.
        /// </summary>
        private static IIgnite InitClient()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "client" + Guid.NewGuid()))
            {
                ClientMode = true
            };

            return Ignition.Start(cfg);
        }
        
        /// <summary>
        /// Stops node.
        /// </summary>
        private void StopNode(int idx)
        {
            _ignite[idx].Dispose();
            _ignite[idx] = null;
        }
        
        private static void ResumeThreads(string gridName)
        {
            CallStringMethod(gridName, "org/apache/ignite/platform/PlatformThreadUtils", "resume",
                "(Ljava/lang/String;)V");
        }

        private static void PerformClientReconnect(IIgnite client)
        {
            var disconnectedEvt = new ManualResetEventSlim(false);
            client.ClientDisconnected += (sender, args) => { disconnectedEvt.Set(); };

            var reconnectedEvt = new ManualResetEventSlim(false);
            ClientReconnectEventArgs reconnectEventArgs = null;

            client.ClientReconnected += (sender, args) =>
            {
                reconnectEventArgs = args;
                reconnectedEvt.Set();
            };

            var gridName = string.Format("%{0}%", client.Name);
            SuspendThreads(gridName);
            Thread.Sleep(7000);
            ResumeThreads(gridName);

            Assert.Catch(() => client.CreateCache<int, int>("_fail").Put(1, 1));

            var disconnected = disconnectedEvt.Wait(TimeSpan.FromSeconds(3));
            Assert.IsTrue(disconnected);

            Assert.Catch(() => client.CreateCache<int, int>("_fail").Put(1, 1));
            var reconnected = reconnectedEvt.Wait(TimeSpan.FromSeconds(15));

            Assert.IsTrue(reconnected);
            Assert.IsFalse(reconnectEventArgs.HasClusterRestarted);
        }

        private static void SuspendThreads(string gridName)
        {
            CallStringMethod(gridName, "org/apache/ignite/platform/PlatformThreadUtils", "suspend",
                "(Ljava/lang/String;)V");
        }

        private static unsafe void CallStringMethod(string gridName, string className, string methodName, string methodSig)
        {
            var env = Jvm.Get().AttachCurrentThread();
            using (var cls = env.FindClass(className))
            {
                var methodId = env.GetStaticMethodId(cls, methodName, methodSig);
                using (var gridNameRef = env.NewStringUtf(gridName))
                {
                    var args = stackalloc long[1];
                    args[0] = gridNameRef.Target.ToInt64();

                    env.CallStaticVoidMethod(cls, methodId, args);
                }
            }
        }
    }
}
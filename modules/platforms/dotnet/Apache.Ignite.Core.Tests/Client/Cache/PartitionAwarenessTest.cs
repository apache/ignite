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

// ReSharper disable RedundantCast
namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests partition awareness functionality.
    /// </summary>
    public class PartitionAwarenessTest : ClientTestBase
    {
        /** */
        private const int ServerCount = 3;
        
        /** */
        private ICacheClient<int, int> _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionAwarenessTest"/> class.
        /// </summary>
        public PartitionAwarenessTest() 
            : base(ServerCount)
        {
            // No-op.
        }

        /// <summary>
        /// Fixture set up.
        /// </summary>
        public override void FixtureSetUp()
        {
            base.FixtureSetUp();

            _cache = Client.CreateCache<int, int>("c");

            // Warm up client partition data.
            InitTestData();
            _cache.Get(1);
            _cache.Get(2);
        }

        public override void TestSetUp()
        {
            base.TestSetUp();

            InitTestData();
            ClearLoggers();
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void CacheGet_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var res = _cache.Get(key);

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void CacheGetAsync_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var res = _cache.GetAsync(key).Result;

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        [TestCase(1, 0)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 2)]
        [TestCase(5, 2)]
        [TestCase(6, 1)]
        public void CacheGet_UserDefinedKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var cache = Client.GetOrCreateCache<TestKey, int>("c_custom_key");
            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => new TestKey(x, x.ToString()), x => x));
            cache.Get(new TestKey(1, "1")); // Warm up;

            ClearLoggers();
            var testKey = new TestKey(key, key.ToString());
            var res = cache.Get(testKey);

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
            Assert.AreEqual(gridIdx, GetPrimaryNodeIdx(testKey));
        }

        [Test]
        public void CachePut_UserDefinedTypeWithAffinityKey_ThrowsIgniteException()
        {
            // Note: annotation-based configuration is not supported on Java side.
            // Use manual configuration instead.
            var cacheClientConfiguration = new CacheClientConfiguration("c_custom_key_aff")
            {
                KeyConfiguration = new List<CacheKeyConfiguration>
                {
                    new CacheKeyConfiguration(typeof(TestKeyWithAffinity))
                    {
                        AffinityKeyFieldName = "_i"
                    }
                }
            };
            var cache = Client.GetOrCreateCache<TestKeyWithAffinity, int>(cacheClientConfiguration);

            var ex = Assert.Throws<IgniteException>(() => cache.Put(new TestKeyWithAffinity(1, "1"), 1));

            var expected = string.Format("Affinity keys are not supported. Object '{0}' has an affinity key.",
                typeof(TestKeyWithAffinity));

            Assert.AreEqual(expected, ex.Message);
        }

        [Test]
        public void CacheGet_NewNodeEnteredTopology_RequestIsRoutedToNewNode()
        {
            // Warm-up.
            Assert.AreEqual(1, _cache.Get(1));

            // Before topology change.
            Assert.AreEqual(12, _cache.Get(12));
            Assert.AreEqual(1, GetClientRequestGridIndex());

            Assert.AreEqual(14, _cache.Get(14));
            Assert.AreEqual(2, GetClientRequestGridIndex());

            // After topology change.
            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                TestUtils.WaitForTrueCondition(() =>
                {
                    // Keys 12 and 14 belong to a new node now (-1).
                    Assert.AreEqual(12, _cache.Get(12));
                    if (GetClientRequestGridIndex() != -1)
                    {
                        return false;
                    }

                    Assert.AreEqual(14, _cache.Get(14));
                    Assert.AreEqual(-1, GetClientRequestGridIndex());

                    return true;
                }, 3000);
            }
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void AllKeyBasedOperations_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            int unused;

            TestOperation(() => _cache.Get(key), gridIdx);
            TestAsyncOperation(() => _cache.GetAsync(key), gridIdx);

            TestOperation(() => _cache.TryGet(key, out unused), gridIdx);
            TestAsyncOperation(() => _cache.TryGetAsync(key), gridIdx);

            TestOperation(() => _cache.Put(key, key), gridIdx, "Put");
            TestAsyncOperation(() => _cache.PutAsync(key, key), gridIdx, "Put");

            TestOperation(() => _cache.PutIfAbsent(key, key), gridIdx, "PutIfAbsent");
            TestAsyncOperation(() => _cache.PutIfAbsentAsync(key, key), gridIdx, "PutIfAbsent");

            TestOperation(() => _cache.GetAndPutIfAbsent(key, key), gridIdx, "GetAndPutIfAbsent");
            TestAsyncOperation(() => _cache.GetAndPutIfAbsentAsync(key, key), gridIdx, "GetAndPutIfAbsent");

            TestOperation(() => _cache.Clear(key), gridIdx, "ClearKey");
            TestAsyncOperation(() => _cache.ClearAsync(key), gridIdx, "ClearKey");

            TestOperation(() => _cache.ContainsKey(key), gridIdx, "ContainsKey");
            TestAsyncOperation(() => _cache.ContainsKeyAsync(key), gridIdx, "ContainsKey");

            TestOperation(() => _cache.GetAndPut(key, key), gridIdx, "GetAndPut");
            TestAsyncOperation(() => _cache.GetAndPutAsync(key, key), gridIdx, "GetAndPut");

            TestOperation(() => _cache.GetAndReplace(key, key), gridIdx, "GetAndReplace");
            TestAsyncOperation(() => _cache.GetAndReplaceAsync(key, key), gridIdx, "GetAndReplace");

            TestOperation(() => _cache.GetAndRemove(key), gridIdx, "GetAndRemove");
            TestAsyncOperation(() => _cache.GetAndRemoveAsync(key), gridIdx, "GetAndRemove");

            TestOperation(() => _cache.Replace(key, key), gridIdx, "Replace");
            TestAsyncOperation(() => _cache.ReplaceAsync(key, key), gridIdx, "Replace");

            TestOperation(() => _cache.Replace(key, key, key + 1), gridIdx, "ReplaceIfEquals");
            TestAsyncOperation(() => _cache.ReplaceAsync(key, key, key + 1), gridIdx, "ReplaceIfEquals");

            TestOperation(() => _cache.Remove(key), gridIdx, "RemoveKey");
            TestAsyncOperation(() => _cache.RemoveAsync(key), gridIdx, "RemoveKey");

            TestOperation(() => _cache.Remove(key, key), gridIdx, "RemoveIfEquals");
            TestAsyncOperation(() => _cache.RemoveAsync(key, key), gridIdx, "RemoveIfEquals");
        }

        [Test]
        public void CacheGet_RepeatedCall_DoesNotRequestAffinityMapping()
        {
            // Test that affinity mapping is not requested when known.
            // Start new cache to enforce partition mapping request.
            Client.CreateCache<int, int>("repeat-call-test");
            ClearLoggers();

            _cache.Get(1);
            _cache.Get(1);
            _cache.Get(1);

            var requests = GetAllServerRequestNames(RequestNamePrefixCache);

            var expectedRequests = new[]
            {
                "Partitions",
                "Get",
                "Get",
                "Get"
            };

            CollectionAssert.AreEquivalent(expectedRequests, requests);
        }

        [Test]
        public void ReplicatedCacheGet_RepeatedCall_DoesNotRequestAffinityMapping()
        {
            // Test cache for which partition awareness is not applicable.
            var cfg = new CacheClientConfiguration("replicated_cache") {CacheMode = CacheMode.Replicated};
            var cache = Client.CreateCache<int, int>(cfg);

            // Init the replicated cache and start the new one to enforce partition mapping request.
            cache.PutAll(Enumerable.Range(1, 3).ToDictionary(x => x, x => x));
            Client.CreateCache<int, int>("repeat-call-test-replicated");
            ClearLoggers();

            cache.Get(1);
            cache.Get(2);
            cache.Get(3);

            var reqs = GetLoggers()
                .Select(l => GetServerRequestNames(l, RequestNamePrefixCache).ToArray())
                .Where(x => x.Length > 0)
                .ToArray();
            
            // All requests should go to a single (default) node, because partition awareness is not applicable.
            Assert.AreEqual(1, reqs.Length);

            // There should be only one partitions request.
            var expectedRequests = new[]
            {
                "Partitions",
                "Get",
                "Get",
                "Get"
            };

            Assert.AreEqual(expectedRequests, reqs[0]);
        }

        [Test]
        public void CacheGet_PartitionAwarenessDisabled_RequestIsRoutedToDefaultNode()
        {
            var cfg = GetClientConfiguration();
            cfg.EnablePartitionAwareness = false;

            using (var client = Ignition.StartClient(cfg))
            {
                var cache = client.GetCache<int, int>(_cache.Name);

                var requestTargets = Enumerable
                    .Range(1, 10)
                    .Select(x =>
                    {
                        cache.Get(x);
                        return GetClientRequestGridIndex();
                    })
                    .Distinct()
                    .ToArray();

                // Partition awareness disabled - all requests go to same socket, picked with round-robin on connect.
                Assert.AreEqual(1, requestTargets.Length);
            }
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase((uint) 1, 1)]
        [TestCase((uint) 2, 0)]
        [TestCase((byte) 1, 1)]
        [TestCase((byte) 2, 0)]
        [TestCase((sbyte) 1, 1)]
        [TestCase((sbyte) 2, 0)]
        [TestCase((short) 1, 1)]
        [TestCase((short) 2, 0)]
        [TestCase((ushort) 1, 1)]
        [TestCase((ushort) 2, 0)]
        [TestCase((long) 1, 1)]
        [TestCase((long) 2, 0)]
        [TestCase((ulong) 1, 1)]
        [TestCase((ulong) 2, 0)]
        [TestCase((float) 1.3, 0)]
        [TestCase((float) 1.4, 2)]
        [TestCase((double) 51.3, 1)]
        [TestCase((double) 415.5, 0)]
        [TestCase((double) 325.5, 0)]
        [TestCase((double) 255.5, 1)]
        [TestCase('1', 2)]
        [TestCase('2', 1)]
        [TestCase(true, 1)]
        [TestCase(false, 1)]
        public void CachePut_AllPrimitiveTypes_RequestIsRoutedToPrimaryNode(object key, int gridIdx)
        {
            var cache = Client.GetCache<object, object>(_cache.Name);
            TestOperation(() => cache.Put(key, key), gridIdx, "Put");

            // Verify against real Affinity.
            Assert.AreEqual(gridIdx, GetPrimaryNodeIdx(key));
        }

        [Test]
        [TestCase("00000000-0000-0000-0000-000000000000", 0)]
        [TestCase("0cb85a41-bd0d-405b-8f34-f515e8aabc39", 0)]
        [TestCase("b4addd17-218c-4054-a5fa-03c88f5ee71c", 0)]
        [TestCase("2611e3d2-618d-43b9-a318-2f5039f82568", 1)]
        [TestCase("1dd8bfae-29b8-4949-aa99-7c9bfabe2566", 2)]
        public void CachePut_GuidKey_RequestIsRoutedToPrimaryNode(string keyString, int gridIdx)
        {
            var key = Guid.Parse(keyString);

            var cache = Client.GetCache<object, object>(_cache.Name);
            TestOperation(() => cache.Put(key, key), gridIdx, "Put");

            // Verify against real Affinity.
            Assert.AreEqual(gridIdx, GetPrimaryNodeIdx(key));
        }

        [Test]
        [TestCase("2015-01-01T00:00:00.0000000Z", 2)]
        [TestCase("2016-02-02T00:00:00.0000000Z", 2)]
        [TestCase("2017-03-03T00:00:00.0000000Z", 0)]
        public void CachePut_DateTimeKey_RequestIsRoutedToPrimaryNode(string keyString, int gridIdx)
        {
            var key = DateTime.Parse(keyString, CultureInfo.InvariantCulture).ToUniversalTime();

            var cache = Client.GetCache<object, object>(_cache.Name);
            TestOperation(() => cache.Put(key, key), gridIdx, "Put");

            // Verify against real Affinity.
            Assert.AreEqual(gridIdx, GetPrimaryNodeIdx(key));
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        public void CachePut_IntPtrKeyKey_RequestIsRoutedToPrimaryNode(int keyInt, int gridIdx)
        {
            var key = new IntPtr(keyInt);

            var cache = Client.GetCache<object, object>(_cache.Name);
            TestOperation(() => cache.Put(key, key), gridIdx, "Put");

            // Verify against real Affinity.
            Assert.AreEqual(gridIdx, GetPrimaryNodeIdx(key));
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        public void CachePut_UIntPtrKeyKey_RequestIsRoutedToPrimaryNode(int keyInt, int gridIdx)
        {
            var key = new UIntPtr((uint) keyInt);

            var cache = Client.GetCache<object, object>(_cache.Name);
            TestOperation(() => cache.Put(key, key), gridIdx, "Put");

            // Verify against real Affinity.
            Assert.AreEqual(gridIdx, GetPrimaryNodeIdx(key));
        }

        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            var cfg = base.GetClientConfiguration();

            cfg.EnablePartitionAwareness = true;
            cfg.Endpoints.Add(string.Format("{0}:{1}", IPAddress.Loopback, IgniteClientConfiguration.DefaultPort + 1));
            cfg.Endpoints.Add(string.Format("{0}:{1}", IPAddress.Loopback, IgniteClientConfiguration.DefaultPort + 2));

            return cfg;
        }

        private int GetClientRequestGridIndex(string message = null)
        {
            message = message ?? "Get";

            try
            {
                for (var i = 0; i < ServerCount; i++)
                {
                    var requests = GetServerRequestNames(i, RequestNamePrefixCache);

                    if (requests.Contains(message))
                    {
                        return i;
                    }
                }

                return -1;
            }
            finally
            {
                ClearLoggers();
            }
        }

        private void TestOperation(Action action, int expectedGridIdx, string message = null)
        {
            InitTestData();
            ClearLoggers();
            action();
            Assert.AreEqual(expectedGridIdx, GetClientRequestGridIndex(message));
        }

        private void TestAsyncOperation<T>(Func<T> action, int expectedGridIdx, string message = null)
            where T : Task
        {
            ClearLoggers();
            action().Wait();
            Assert.AreEqual(expectedGridIdx, GetClientRequestGridIndex(message));
        }

        private void InitTestData()
        {
            _cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));
        }

        private int GetPrimaryNodeIdx<T>(T key)
        {
            var idx = 0;

            // GetAll is not ordered - sort the same way as _loggers.
            var ignites = Ignition.GetAll().OrderBy(i => i.Name);

            foreach (var ignite in ignites)
            {
                var aff = ignite.GetAffinity(_cache.Name);
                var localNode = ignite.GetCluster().GetLocalNode();

                if (aff.IsPrimary(localNode, key))
                {
                    return idx;
                }

                idx++;
            }

            throw new InvalidOperationException("Can't determine primary node");
        }
    }
}

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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests partition loss management functionality:
    /// <see cref="PartitionLossPolicy"/>, <see cref="IIgnite.ResetLostPartitions(IEnumerable{string})"/>,
    /// <see cref="ICache{TK,TV}.GetLostPartitions"/>, <see cref="ICache{TK,TV}.WithPartitionRecover"/>.
    /// </summary>
    public class PartitionLossTest
    {
        /** */
        private const string CacheName = "lossTestCache";

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
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
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            var ignite = Ignition.GetIgnite();

            ignite.GetCacheNames().ToList().ForEach(ignite.DestroyCache);
        }

        /// <summary>
        /// Tests the ReadOnlySafe mode.
        /// </summary>
        [Test]
        public void TestReadOnlySafe()
        {
            TestPartitionLoss(PartitionLossPolicy.ReadOnlySafe, false, true);
        }

        /// <summary>
        /// Tests the ReadWriteSafe mode.
        /// </summary>
        [Test]
        public void TestReadWriteSafe()
        {
            TestPartitionLoss(PartitionLossPolicy.ReadWriteSafe, true, true);
        }

        /// <summary>
        /// Tests the ReadOnlyAll mode.
        /// </summary>
        [Test]
        public void TestReadOnlyAll()
        {
            TestPartitionLoss(PartitionLossPolicy.ReadOnlyAll, false, true);
        }

        /// <summary>
        /// Tests the ReadWriteAll mode.
        /// </summary>
        [Test]
        public void TestReadWriteAll()
        {
            TestPartitionLoss(PartitionLossPolicy.ReadWriteAll, true, true);
        }

        /// <summary>
        /// Tests the Ignore mode.
        /// </summary>
        [Test]
        public void TestIgnoreLoss()
        {
            var ignite = Ignition.GetIgnite();

            var cache = CreateCache(PartitionLossPolicy.Ignore, ignite);

            var lostPart = PrepareTopology();

            Assert.IsEmpty(cache.GetLostPartitions());

            cache[lostPart] = lostPart;

            Assert.AreEqual(lostPart, cache[lostPart]);
        }

        /// <summary>
        /// Tests the partition loss.
        /// </summary>
        private static void TestPartitionLoss(PartitionLossPolicy policy, bool canWrite, bool safe)
        {
            var ignite = Ignition.GetIgnite();

            var cache = CreateCache(policy, ignite);

            // Loose data and verify lost partition.
            var lostPart = PrepareTopology();
            var lostParts = cache.GetLostPartitions();
            Assert.IsTrue(lostParts.Contains(lostPart));

            // Check cache operations.
            foreach (var part in lostParts)
            {
                VerifyCacheOperations(cache, part, canWrite, safe);

                // Check reads are possible from a cache in recovery mode.
                var recoverCache = cache.WithPartitionRecover();
                int res;
                Assert.IsFalse(recoverCache.TryGet(part, out res));
            }

            // Reset and verify.
            ignite.ResetLostPartitions(CacheName);
            Assert.IsEmpty(cache.GetLostPartitions());

            // Check another ResetLostPartitions overload.
            PrepareTopology();
            Assert.IsNotEmpty(cache.GetLostPartitions());
            ignite.ResetLostPartitions(new List<string> {CacheName, "foo"});
            Assert.IsEmpty(cache.GetLostPartitions());
        }

        /// <summary>
        /// Verifies the cache operations.
        /// </summary>
        private static void VerifyCacheOperations(ICache<int, int> cache, int part, bool canWrite, bool safe)
        {
            if (safe)
            {
                int val;
                var ex = Assert.Throws<CacheException>(() => cache.TryGet(part, out val));
                Assert.AreEqual(string.Format(
                    "class org.apache.ignite.internal.processors.cache.CacheInvalidStateException" +
                    ": Failed to execute the cache operation (all partition owners have left the grid, " +
                    "partition data has been lost) [cacheName={0}, partition={1}," +
                    " key=UserKeyCacheObjectImpl [part={1}, val={1}, hasValBytes=false]]",
                    CacheName, part), ex.Message);
            }
            else
            {
                int val;
                Assert.IsFalse(cache.TryGet(part, out val));
            }

            if (canWrite)
            {
                if (safe)
                {
                    var ex = Assert.Throws<CacheException>(() => cache.Put(part, part));
                    Assert.AreEqual(string.Format(
                        "class org.apache.ignite.internal.processors.cache.CacheInvalidStateException: " +
                        "Failed to execute the cache operation (all partition owners have left the grid, " +
                        "partition data has been lost) [cacheName={0}, partition={1}, key={1}]",
                        CacheName, part), ex.Message);
                }
                else
                {
                    cache[part] = part;
                    Assert.AreEqual(part, cache[part]);
                }
            }
            else
            {
                var ex = Assert.Throws<CacheException>(() => cache.Put(part, part));
                Assert.AreEqual(string.Format(
                    "class org.apache.ignite.internal.processors.cache.CacheInvalidStateException: " +
                    "Failed to write to cache (cache is moved to a read-only state): {0}",
                    CacheName), ex.Message);
            }
        }

        /// <summary>
        /// Creates the cache.
        /// </summary>
        private static ICache<int, int> CreateCache(PartitionLossPolicy policy, IIgnite ignite)
        {
            return ignite.CreateCache<int, int>(new CacheConfiguration(CacheName)
            {
                CacheMode = CacheMode.Partitioned,
                Backups = 0,
                WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                PartitionLossPolicy = policy,
                AffinityFunction = new RendezvousAffinityFunction
                {
                    ExcludeNeighbors = false,
                    Partitions = 32
                }
            });
        }

        /// <summary>
        /// Prepares the topology: starts a new node and stops it after rebalance to ensure data loss.
        /// </summary>
        /// <returns>Lost partition id.</returns>
        private static int PrepareTopology()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration(name: "ignite-2")))
            {
                var cache = ignite.GetCache<int, int>(CacheName);

                var affinity = ignite.GetAffinity(CacheName);

                var keys = Enumerable.Range(1, affinity.Partitions).ToArray();

                cache.PutAll(keys.ToDictionary(x => x, x => x));

                cache.Rebalance();

                // Wait for rebalance to complete.
                var node = ignite.GetCluster().GetLocalNode();
                Func<int, bool> isPrimary = x => affinity.IsPrimary(node, x);

                while (!keys.Any(isPrimary))
                {
                    Thread.Sleep(10);
                }

                Thread.Sleep(100);  // Some extra wait.

                return keys.First(isPrimary);
            }
        }
    }
}

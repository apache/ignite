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

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests user-defined <see cref="IAffinityFunction"/>
    /// </summary>
    public class AffinityFunctionTest
    {
        /** */
        private IIgnite _ignite;

        /** */
        private IIgnite _ignite2;

        /** */
        private const string CacheName = "cache";

        /** */
        private const int PartitionCount = 10;

        /** */
        private static readonly ConcurrentBag<Guid> RemovedNodes = new ConcurrentBag<Guid>();

        /** */
        private static readonly ConcurrentBag<AffinityFunctionContext> Contexts =
            new ConcurrentBag<AffinityFunctionContext>();

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
                        AffinityFunction = new SimpleAffinityFunction(),
                        Backups = 7
                    }
                }
            };

            _ignite = Ignition.Start(cfg);

            _ignite2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()) {GridName = "grid2"});
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            // Check that affinity handles are present
            TestUtils.AssertHandleRegistryHasItems(_ignite, _ignite.GetCacheNames().Count, 0);
            TestUtils.AssertHandleRegistryHasItems(_ignite2, _ignite.GetCacheNames().Count, 0);

            // Destroy all caches
            _ignite.GetCacheNames().ToList().ForEach(_ignite.DestroyCache);
            Assert.AreEqual(0, _ignite.GetCacheNames().Count);

            // Check that all affinity functions got released
            TestUtils.AssertHandleRegistryIsEmpty(1000, _ignite, _ignite2);

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            VerifyCacheAffinity(_ignite.GetCache<int, int>(CacheName));
            VerifyCacheAffinity(_ignite2.GetCache<int, int>(CacheName));
        }

        /// <summary>
        /// Tests the dynamic cache.
        /// </summary>
        [Test]
        public void TestDynamicCache()
        {
            const string cacheName = "dynCache";

            VerifyCacheAffinity(_ignite.CreateCache<int, int>(new CacheConfiguration(cacheName)
            {
                AffinityFunction = new SimpleAffinityFunction(),
                Backups = 5
            }));

            VerifyCacheAffinity(_ignite2.GetCache<int, int>(cacheName));
            
            // Verify context for new cache
            var lastCtx = Contexts.Where(x => x.GetPreviousAssignment(1) == null)
                .OrderBy(x => x.DiscoveryEvent.Timestamp).Last();

            Assert.AreEqual(new AffinityTopologyVersion(2, 1), lastCtx.CurrentTopologyVersion);
            Assert.AreEqual(5, lastCtx.Backups);

            // Verify context for old cache
            var ctx = Contexts.Where(x => x.GetPreviousAssignment(1) != null)
                .OrderBy(x => x.DiscoveryEvent.Timestamp).Last();

            Assert.AreEqual(new AffinityTopologyVersion(2, 0), ctx.CurrentTopologyVersion);
            Assert.AreEqual(7, ctx.Backups);
            CollectionAssert.AreEquivalent(_ignite.GetCluster().GetNodes(), ctx.CurrentTopologySnapshot);

            var evt = ctx.DiscoveryEvent;
            CollectionAssert.AreEquivalent(_ignite.GetCluster().GetNodes(), evt.TopologyNodes);
            CollectionAssert.Contains(_ignite.GetCluster().GetNodes(), evt.EventNode);
            Assert.AreEqual(_ignite.GetCluster().TopologyVersion, evt.TopologyVersion);

            var firstTop = _ignite.GetCluster().GetTopology(1);
            var parts = Enumerable.Range(0, PartitionCount).ToArray();
            CollectionAssert.AreEqual(parts.Select(x => firstTop), parts.Select(x => ctx.GetPreviousAssignment(x)));
        }

        /// <summary>
        /// Verifies the cache affinity.
        /// </summary>
        private static void VerifyCacheAffinity(ICache<int, int> cache)
        {
            Assert.IsInstanceOf<SimpleAffinityFunction>(cache.GetConfiguration().AffinityFunction);

            var aff = cache.Ignite.GetAffinity(cache.Name);
            Assert.AreEqual(PartitionCount, aff.Partitions);

            for (int i = 0; i < 100; i++)
                Assert.AreEqual(i % PartitionCount, aff.GetPartition(i));
        }

        /// <summary>
        /// Tests the RemoveNode method.
        /// </summary>
        [Test]
        public void TestRemoveNode()
        {
            Assert.AreEqual(0, RemovedNodes.Count);

            Guid expectedNodeId;

            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = "grid3",
            }))
            {
                expectedNodeId = ignite.GetCluster().GetLocalNode().Id;
                Assert.AreEqual(0, RemovedNodes.Count);
                VerifyCacheAffinity(ignite.GetCache<int, int>(CacheName));
            }

            // Called on both nodes
            TestUtils.WaitForCondition(() => RemovedNodes.Count > 0, 3000);
            Assert.AreEqual(expectedNodeId, RemovedNodes.Distinct().Single());
        }

        /// <summary>
        /// Tests the error on non-serializable function.
        /// </summary>
        [Test]
        public void TestNonSerializableFunction()
        {
            var ex = Assert.Throws<IgniteException>(() =>
                _ignite.CreateCache<int, int>(new CacheConfiguration("failCache")
                {
                    AffinityFunction = new NonSerializableAffinityFunction()
                }));

            Assert.AreEqual(ex.Message, "AffinityFunction should be serializable.");
        }

        /// <summary>
        /// Tests the exception propagation.
        /// </summary>
        [Test]
        public void TestExceptionInFunction()
        {
            var cache = _ignite.CreateCache<int, int>(new CacheConfiguration("failCache2")
            {
                AffinityFunction = new FailInGetPartitionAffinityFunction()
            });

            var ex = Assert.Throws<CacheException>(() => cache.Put(1, 2));
            Assert.AreEqual("User error", ex.InnerException.Message);
        }

        /// <summary>
        /// Tests user-defined function that inherits predefined function.
        /// </summary>
        [Test]
        public void TestInheritPredefinedFunction()
        {
            var ex = Assert.Throws<IgniteException>(() =>
                _ignite.CreateCache<int, int>(
                    new CacheConfiguration("failCache3")
                    {
                        AffinityFunction = new FairAffinityFunctionInheritor()
                    }));

            Assert.AreEqual("User-defined AffinityFunction can not inherit from " +
                            "Apache.Ignite.Core.Cache.Affinity.AffinityFunctionBase: " +
                            "Apache.Ignite.Core.Tests.Cache.Affinity.AffinityFunctionTest" +
                            "+FairAffinityFunctionInheritor", ex.Message);
        }

        [Serializable]
        private class SimpleAffinityFunction : IAffinityFunction
        {
            #pragma warning disable 649  // field is never assigned
            [InstanceResource] private readonly IIgnite _ignite;

            public int Partitions
            {
                get { return PartitionCount; }
            }

            public int GetPartition(object key)
            {
                Assert.IsNotNull(_ignite);

                return (int) key % Partitions;
            }

            public void RemoveNode(Guid nodeId)
            {
                RemovedNodes.Add(nodeId);
            }

            public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(AffinityFunctionContext context)
            {
                Assert.IsNotNull(_ignite);

                Contexts.Add(context);

                // All partitions are the same
                return Enumerable.Range(0, Partitions).Select(x => context.CurrentTopologySnapshot);
            }
        }

        private class NonSerializableAffinityFunction : SimpleAffinityFunction
        {
            // No-op.
        }

        [Serializable]
        private class FailInGetPartitionAffinityFunction : IAffinityFunction
        {
            public int Partitions
            {
                get { return 5; }
            }

            public int GetPartition(object key)
            {
                throw new ArithmeticException("User error");
            }

            public void RemoveNode(Guid nodeId)
            {
                // No-op.
            }

            public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(AffinityFunctionContext context)
            {
                return Enumerable.Range(0, Partitions).Select(x => context.CurrentTopologySnapshot);
            }
        }

        [Serializable]
        private class FairAffinityFunctionInheritor : FairAffinityFunction
        {
            // No-op.
        }
    }
}

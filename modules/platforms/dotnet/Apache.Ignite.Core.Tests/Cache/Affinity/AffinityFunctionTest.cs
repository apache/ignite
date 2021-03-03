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

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
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
        private const string CacheNameRendezvous = "cacheRendezvous";

        /** */
        private const int PartitionCount = 10;

        /** */
        private const string BackupFilterAttrName = "DC";

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
                    },
                    new CacheConfiguration(CacheNameRendezvous)
                    {
                        AffinityFunction = new RendezvousAffinityFunctionEx {Bar = "test"}
                    }
                },
                UserAttributes = new Dictionary<string, object>{{BackupFilterAttrName, 1}}
            };

            _ignite = Ignition.Start(cfg);

            var cfg2 = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = "grid2",
                UserAttributes = new Dictionary<string, object>{{BackupFilterAttrName, 2}}
            };

            _ignite2 = Ignition.Start(cfg2);

            AffinityTopologyVersion waitingTop = new AffinityTopologyVersion(2, 1);

            Assert.True(_ignite.WaitTopology(waitingTop), "Failed to wait topology " + waitingTop);
            Assert.True(_ignite2.WaitTopology(waitingTop), "Failed to wait topology " + waitingTop);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            try
            {
                // Check that affinity handles are present:
                // TestDynamicCachePredefined, TestSimpleInheritance, TestSimpleInheritanceWithBackupFilter
                // do not produce extra handles, so "-3" here.
                TestUtils.AssertHandleRegistryHasItems(_ignite, _ignite.GetCacheNames().Count - 3, 0);
                TestUtils.AssertHandleRegistryHasItems(_ignite2, _ignite.GetCacheNames().Count - 3, 0);

                // Destroy all caches
                _ignite.GetCacheNames().ToList().ForEach(_ignite.DestroyCache);
                Assert.AreEqual(0, _ignite.GetCacheNames().Count);

                // Check that all affinity functions got released
                TestUtils.AssertHandleRegistryIsEmpty(1000, _ignite, _ignite2);
            }
            finally
            {
                Ignition.StopAll(true);
            }
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

            Assert.AreEqual(new AffinityTopologyVersion(2, 2), lastCtx.CurrentTopologyVersion);
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
        /// Tests the dynamic cache with predefined functions.
        /// </summary>
        [Test]
        public void TestDynamicCachePredefined()
        {
            var caches = new[]
            {
                new CacheConfiguration("rendezvousPredefined")
                {
                    AffinityFunction = new RendezvousAffinityFunction {Partitions = 1234}
                }
            }.Select(_ignite.CreateCache<int, int>);

            foreach (var cache in caches)
            {
                Assert.AreEqual(1234, cache.GetConfiguration().AffinityFunction.Partitions);

                cache[1] = 2;

                Assert.AreEqual(2, cache[1]);
            }
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
                IgniteInstanceName = "grid3",
            }))
            {
                expectedNodeId = ignite.GetCluster().GetLocalNode().Id;
                Assert.AreEqual(0, RemovedNodes.Count);
                VerifyCacheAffinity(ignite.GetCache<int, int>(CacheName));
            }

            // Called on both nodes
            TestUtils.WaitForCondition(() => RemovedNodes.Count == 6, 3000);
            Assert.GreaterOrEqual(RemovedNodes.Count, 6);
            Assert.AreEqual(expectedNodeId, RemovedNodes.Distinct().Single());
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
            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual("User error", ex.InnerException.Message);
        }

        /// <summary>
        /// Tests customized rendezvous affinity.
        /// </summary>
        [Test]
        public void TestInheritRendezvousAffinity()
        {
            Assert.Greater(RendezvousAffinityFunctionEx.AssignCount, 2);

            var caches = new[]
            {
                _ignite.GetCache<int, int>(CacheNameRendezvous),
                _ignite.CreateCache<int, int>(new CacheConfiguration(CacheNameRendezvous + "2")
                {
                    AffinityFunction = new RendezvousAffinityFunctionEx
                    {
                        Bar = "test",
                        AffinityBackupFilter = new ClusterNodeAttributeAffinityBackupFilter
                        {
                            AttributeNames = new[] {BackupFilterAttrName}
                        }
                    }
                })
            };

            foreach (var cache in caches)
            {
                var aff = _ignite.GetAffinity(cache.Name);

                Assert.AreEqual(PartitionCount, aff.Partitions);

                // Test from map
                Assert.AreEqual(3, aff.GetPartition(1));
                Assert.AreEqual(4, aff.GetPartition(2));

                // Test from base func
                Assert.AreEqual(2, aff.GetPartition(42));

                // Check config
                var func = (RendezvousAffinityFunctionEx)cache.GetConfiguration().AffinityFunction;
                Assert.AreEqual("test", func.Bar);

                if (cache.Name == CacheNameRendezvous)
                {
                    Assert.IsNull(func.AffinityBackupFilter);
                }
                else
                {
                    var filter = func.AffinityBackupFilter as ClusterNodeAttributeAffinityBackupFilter;
                    Assert.IsNotNull(filter);
                    CollectionAssert.AreEqual(new[]{BackupFilterAttrName}, filter.AttributeNames);
                }
            }
        }

        /// <summary>
        /// Tests the AffinityFunction with simple inheritance: none of the methods are overridden,
        /// so there are no callbacks, and user object is not passed over the wire.
        /// </summary>
        [Test]
        public void TestSimpleInheritance()
        {
           var cache = _ignite.CreateCache<int, int>(new CacheConfiguration("simpleInherit")
            {
                AffinityFunction = new SimpleOverride()
            });

            var aff = _ignite.GetAffinity(cache.Name);

            Assert.AreEqual(PartitionCount, aff.Partitions);
            Assert.AreEqual(3, aff.GetPartition(33));
            Assert.AreEqual(4, aff.GetPartition(34));
        }

        /// <summary>
        /// Tests the AffinityFunction with simple inheritance and a backup filter: none of the methods are overridden,
        /// so there are no callbacks, and user object is not passed over the wire.
        /// </summary>
        [Test]
        public void TestSimpleInheritanceWithBackupFilter()
        {
            var cache = _ignite.CreateCache<int, int>(new CacheConfiguration(TestUtils.TestName)
            {
                AffinityFunction = new SimpleOverride
                {
                    AffinityBackupFilter = new ClusterNodeAttributeAffinityBackupFilter
                    {
                        AttributeNames = new[] {BackupFilterAttrName}
                    }
                }
            });

            var aff = cache.GetConfiguration().AffinityFunction as RendezvousAffinityFunction;
            Assert.IsNotNull(aff);

            var filter = aff.AffinityBackupFilter as ClusterNodeAttributeAffinityBackupFilter;
            Assert.IsNotNull(filter);
            CollectionAssert.AreEqual(new[] {BackupFilterAttrName}, filter.AttributeNames);
        }

        /// <summary>
        /// Tests that custom backup filters are not allowed.
        /// </summary>
        [Test]
        public void TestCustomBackupFilterThrowsNotSupportedException()
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                AffinityFunction = new RendezvousAffinityFunction
                {
                    AffinityBackupFilter = new CustomBackupFilter()
                }
            };

            var ex = Assert.Throws<NotSupportedException>(() => _ignite.CreateCache<int, int>(cfg));

            var expectedMessage = string.Format(
                "Unsupported RendezvousAffinityFunction.AffinityBackupFilter: '{0}'. " +
                "Only predefined implementations are supported: 'ClusterNodeAttributeAffinityBackupFilter'",
                typeof(CustomBackupFilter).FullName);

            Assert.AreEqual(expectedMessage, ex.Message);
        }

        /// <summary>
        /// Tests that backup filter requires a non-empty attribute set.
        /// </summary>
        [Test]
        public void TestBackupFilterWithNullAttributesThrowsException([Values(true, false)] bool nullOrEmpty)
        {
            var cfg = new CacheConfiguration(TestUtils.TestName)
            {
                AffinityFunction = new RendezvousAffinityFunction
                {
                    AffinityBackupFilter = new ClusterNodeAttributeAffinityBackupFilter
                    {
                        AttributeNames = nullOrEmpty ? null : new List<string>()
                    }
                }
            };

            var ex = Assert.Throws<ArgumentException>(() => _ignite.CreateCache<int, int>(cfg));

            var expectedMessage =
                "'ClusterNodeAttributeAffinityBackupFilter.AttributeNames' argument should not be null or empty.";

            StringAssert.StartsWith(expectedMessage, ex.Message);
        }

        [Serializable]
        private class SimpleAffinityFunction : IAffinityFunction
        {
            #pragma warning disable 649  // field is never assigned
            // ReSharper disable once UnassignedReadonlyField
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
        private class RendezvousAffinityFunctionEx : RendezvousAffinityFunction
        {
            public static int AssignCount;

            private static readonly Dictionary<int, int> PartitionMap = new Dictionary<int, int> {{1, 3}, {2, 4}};

            public override int Partitions
            {
                get { return PartitionCount; }
                set { Assert.AreEqual(Partitions, value); }
            }

            public string Bar { get; set; }

            public override int GetPartition(object key)
            {
                int res;

                if (PartitionMap.TryGetValue((int)key, out res))
                    return res;

                return base.GetPartition(key);
            }

            public override void RemoveNode(Guid nodeId)
            {
                RemovedNodes.Add(nodeId);
            }

            public override IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(AffinityFunctionContext context)
            {
                var res = base.AssignPartitions(context).Reverse();

                Interlocked.Increment(ref AssignCount);

                return res;
            }
        }

        /// <summary>
        /// Override only properties, so this func won't be passed over the wire.
        /// </summary>
        private class SimpleOverride : RendezvousAffinityFunction
        {
            public override int Partitions
            {
                get { return PartitionCount; }
                set { throw new NotSupportedException(); }
            }

            public override bool ExcludeNeighbors { get; set; }
        }

        /// <summary>
        /// Custom backup filter.
        /// </summary>
        private class CustomBackupFilter : IAffinityBackupFilter
        {
            // No-op.
        }
    }
}

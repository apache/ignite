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
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
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
        private const string CacheName = "cache";

        /** */
        private const int PartitionCount = 10;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof (SimpleAffinityFunction)),
                CacheConfiguration = new[]
                {
                    new CacheConfiguration(CacheName)
                    {
                        AffinityFunction = new SimpleAffinityFunction()
                    }
                }
            };

            _ignite = Ignition.Start(cfg);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            // Check that affinity handles are present
            TestUtils.AssertHandleRegistryHasItems(_ignite, _ignite.GetCacheNames().Count, 0);

            // Destroy all caches
            _ignite.GetCacheNames().ToList().ForEach(_ignite.DestroyCache);
            Assert.AreEqual(0, _ignite.GetCacheNames().Count);

            // Check that all affinity functions got released
            TestUtils.AssertHandleRegistryIsEmpty(1000, _ignite);

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            VerifyCacheAffinity(_ignite.GetCache<int, int>(CacheName));
        }

        /// <summary>
        /// Tests the dynamic cache.
        /// </summary>
        [Test]
        public void TestDynamicCache()
        {
            VerifyCacheAffinity(_ignite.CreateCache<int, int>(new CacheConfiguration("dynCache")
            {
                AffinityFunction = new SimpleAffinityFunction()
            }));
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
        /// Tests Reset and RemoveNode methods.
        /// </summary>
        [Test]
        public void TestResetRemoveNode()
        {
            // Start another node and verify that Reset is called on start and RemoveNode on stop

            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = "myGrid",
                BinaryConfiguration = new BinaryConfiguration(typeof(SimpleAffinityFunction))
            }))
            {

            }

        }

        private class SimpleAffinityFunction : IAffinityFunction
        {
            #pragma warning disable 649  // field is never assigned
            [InstanceResource] public readonly IIgnite Ignite;

            public void Reset()
            {
                // No-op.
            }

            public int Partitions
            {
                get { return PartitionCount; }
            }

            public int GetPartition(object key)
            {
                Assert.IsNotNull(Ignite);

                return ((int) key) % Partitions;
            }

            public void RemoveNode(Guid nodeId)
            {
                // No-op.
            }

            public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(IAffinityFunctionContext context)
            {
                Assert.IsNotNull(Ignite);

                // All partitions are the same
                return Enumerable.Range(0, Partitions).Select(x => context.CurrentTopologySnapshot);
            }
        }
    }
}

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
        private const int Partitions = 10;

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

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            // Destroy all caches
            _ignite.GetCacheNames().ToList().ForEach(_ignite.DestroyCache);
            Assert.AreEqual(0, _ignite.GetCacheNames().Count);

            // Check that all affinity functions got released
            TestUtils.AssertHandleRegistryIsEmpty(1000, _ignite);

            Ignition.StopAll(true);
        }

        [Test]
        public void TestStaticCache()
        {
            var cache = _ignite.GetCache<int, int>(CacheName);
            var func = cache.GetConfiguration().AffinityFunction;
            Assert.IsNotNull(((SimpleAffinityFunction)func).Ignite);

            var aff = _ignite.GetAffinity(CacheName);
            Assert.AreEqual(Partitions, aff.Partitions);

            for (int i = 0; i < 100; i++)
                Assert.AreEqual(i % Partitions, aff.GetPartition(i));
        }

        [Test]
        public void TestDynamicCache()
        {
            
        }

        [Test]
        public void TestRemoveNode()
        {
            
        }

        [Test]
        public void TestLifetime()
        {
            // TODO: check handle removal on DestroyCache
        }

        private class SimpleAffinityFunction : IAffinityFunction
        {
            #pragma warning disable 649  // field is never assigned
            [InstanceResource] public readonly IIgnite Ignite;

            public void Reset()
            {
                // No-op.
            }

            public int PartitionCount
            {
                get { return Partitions; }
            }

            public int GetPartition(object key)
            {
                return ((int) key) % PartitionCount;
            }

            public void RemoveNode(Guid nodeId)
            {
                // No-op.
            }

            public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(IAffinityFunctionContext context)
            {
                // All partitions are the same
                return Enumerable.Range(0, PartitionCount).Select(x => context.CurrentTopologySnapshot);
            }
        }
    }
}

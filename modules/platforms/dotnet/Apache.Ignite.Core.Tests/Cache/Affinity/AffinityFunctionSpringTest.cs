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

// ReSharper disable UnusedAutoPropertyAccessor.Local
// ReSharper disable UnusedMember.Local
namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests AffinityFunction defined in Spring XML.
    /// </summary>
    public class AffinityFunctionSpringTest : SpringTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionSpringTest"/> class.
        /// </summary>
        public AffinityFunctionSpringTest() : base(6,
            Path.Combine("Config", "Cache", "Affinity", "affinity-function.xml"),
            Path.Combine("Config", "Cache", "Affinity", "affinity-function2.xml"))
        {
            // No-op.
        }

        /// <summary>
        /// Tests the static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            ValidateAffinityFunction(Grid.GetCache<long, int>("cache1"));
            ValidateAffinityFunction(Grid2.GetCache<long, int>("cache1"));
            ValidateAffinityFunction(Grid.GetCache<long, int>("cache2"));
            ValidateAffinityFunction(Grid2.GetCache<long, int>("cache2"));
        }

        /// <summary>
        /// Tests the dynamic cache.
        /// </summary>
        [Test]
        public void TestDynamicCache()
        {
            ValidateAffinityFunction(Grid.CreateCache<long, int>("dyn-cache-1"));
            ValidateAffinityFunction(Grid2.GetCache<long, int>("dyn-cache-1"));

            ValidateAffinityFunction(Grid2.CreateCache<long, int>("dyn-cache-2"));
            ValidateAffinityFunction(Grid.GetCache<long, int>("dyn-cache-2"));

            ValidateAffinityFunction(Grid.CreateCache<long, int>("dyn-cache2-1"));
            ValidateAffinityFunction(Grid2.GetCache<long, int>("dyn-cache2-1"));

            ValidateAffinityFunction(Grid2.CreateCache<long, int>("dyn-cache2-2"));
            ValidateAffinityFunction(Grid.GetCache<long, int>("dyn-cache2-2"));
        }

        /// <summary>
        /// Validates the affinity function.
        /// </summary>
        /// <param name="cache">The cache.</param>
        private static void ValidateAffinityFunction(ICache<long, int> cache)
        {
            var aff = cache.Ignite.GetAffinity(cache.Name);

            Assert.AreEqual(5, aff.Partitions);

            // Predefined map
            Assert.AreEqual(2, aff.GetPartition(1L));
            Assert.AreEqual(1, aff.GetPartition(2L));

            // Other keys
            Assert.AreEqual(1, aff.GetPartition(13L));
            Assert.AreEqual(3, aff.GetPartition(4L));
        }

        // ReSharper disable once UnusedType.Local (used from config)
        private class TestFunc : IAffinityFunction   // [Serializable] is not necessary
        {
            [InstanceResource]
            private readonly IIgnite _ignite = null;

            private int Property1 { get; set; }

            private string Property2 { get; set; }

            public int Partitions
            {
                get { return 5; }
            }

            public int GetPartition(object key)
            {
                Assert.IsNotNull(_ignite);
                Assert.AreEqual(1, Property1);
                Assert.AreEqual("1", Property2);

                var longKey = (long)key;
                int res;

                if (TestRendezvousFunc.PredefinedParts.TryGetValue(longKey, out res))
                    return res;

                return (int)(longKey * 2 % 5);
            }

            // ReSharper disable once UnusedParameter.Local
            public void RemoveNode(Guid nodeId)
            {
                // No-op.
            }

            public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(AffinityFunctionContext context)
            {
                return Enumerable.Range(0, Partitions).Select(x => context.CurrentTopologySnapshot);
            }
        }

        private class TestRendezvousFunc : RendezvousAffinityFunction   // [Serializable] is not necessary
        {
            public static readonly Dictionary<long, int> PredefinedParts = new Dictionary<long, int>
            {
                {1, 2},
                {2, 1}
            };

            [InstanceResource]
            private readonly IIgnite _ignite = null;

            private int Property1 { get; set; }

            private string Property2 { get; set; }

            public override int GetPartition(object key)
            {
                Assert.IsNotNull(_ignite);
                Assert.AreEqual(1, Property1);
                Assert.AreEqual("1", Property2);

                Assert.IsInstanceOf<long>(key);

                var basePart = base.GetPartition(key);
                Assert.Greater(basePart, -1);
                Assert.Less(basePart, Partitions);

                var longKey = (long)key;
                int res;

                if (PredefinedParts.TryGetValue(longKey, out res))
                    return res;

                return (int)(longKey * 2 % 5);
            }

            public override IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(AffinityFunctionContext context)
            {
                var baseRes = base.AssignPartitions(context).ToList();  // test base call

                Assert.AreEqual(Partitions, baseRes.Count);

                return baseRes;
            }
        }
    }
}
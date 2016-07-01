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
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests AffinityFunction defined in Spring XML.
    /// </summary>
    public class AffinityFunctionSpringTest : IgniteTestBase
    {
        /** */
        private IIgnite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionSpringTest"/> class.
        /// </summary>
        public AffinityFunctionSpringTest() : base(3, "config\\cache\\affinity\\affinity-function.xml")
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void TestSetUp()
        {
            base.TestSetUp();

            // Start another node without spring config
            if (Ignition.TryGetIgnite("grid2") == null)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration()) {GridName = "grid2"};
                _ignite = Ignition.Start(cfg);
            }
        }

        /// <summary>
        /// Tests the static cache.
        /// </summary>
        [Test]
        public void TestStaticCache()
        {
            ValidateAffinityFunction(Grid.GetCache<int, int>("cache1"));
            ValidateAffinityFunction(_ignite.GetCache<int, int>("cache1"));
        }

        /// <summary>
        /// Tests the dynamic cache.
        /// </summary>
        [Test]
        public void TestDynamicCache()
        {
            ValidateAffinityFunction(Grid.CreateCache<int, int>("dyn-cache-1"));
            ValidateAffinityFunction(_ignite.GetCache<int, int>("dyn-cache-1"));

            ValidateAffinityFunction(_ignite.CreateCache<int, int>("dyn-cache-2"));
            ValidateAffinityFunction(Grid.GetCache<int, int>("dyn-cache-2"));
        }

        /// <summary>
        /// Validates the affinity function.
        /// </summary>
        /// <param name="cache">The cache.</param>
        private static void ValidateAffinityFunction(ICache<int, int> cache)
        {
            Assert.IsNull(cache.GetConfiguration().AffinityFunction);

            var aff = cache.Ignite.GetAffinity(cache.Name);
            Assert.AreEqual(5, aff.Partitions);
            Assert.AreEqual(4, aff.GetPartition(2));
            Assert.AreEqual(3, aff.GetPartition(4));
        }

        [Serializable]
        private class TestFunc : IAffinityFunction
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

                return (int) key * 2 % 5;
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
    }
}

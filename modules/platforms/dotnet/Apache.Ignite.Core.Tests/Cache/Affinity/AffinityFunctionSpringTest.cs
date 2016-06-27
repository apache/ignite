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
namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests AffinityFunction defined in Spring XML.
    /// </summary>
    public class AffinityFunctionSpringTest : IgniteTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionSpringTest"/> class.
        /// </summary>
        public AffinityFunctionSpringTest() : base(1, "config\\cache\\affinity\\affinity-function.xml")
        {
            // No-op.
        }

        /// <summary>
        /// Tests the affinity.
        /// </summary>
        [Test]
        public void TestAffinity()
        {
            var cache = Grid.GetCache<int, int>("cache1");

            var func = (TestFunc) cache.GetConfiguration().AffinityFunction;

            Assert.AreEqual(1, func.Property1);
            Assert.AreEqual("1", func.Property2);

            var aff = Grid.GetAffinity(cache.Name);
            Assert.AreEqual(func.Partitions, aff.Partitions);
        }

        [Serializable]
        private class TestFunc : IAffinityFunction
        {
            [InstanceResource]
            private readonly IIgnite _ignite = null;

            public int Property1 { get; set; }

            public string Property2 { get; set; }

            public int Partitions
            {
                get { return 5; }
            }

            public int GetPartition(object key)
            {
                Assert.IsNotNull(_ignite);

                return 1;
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

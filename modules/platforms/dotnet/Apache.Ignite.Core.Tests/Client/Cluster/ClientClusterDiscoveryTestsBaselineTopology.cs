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

namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests client cluster discovery with baseline topology.
    /// </summary>
    [TestFixture]
    public class ClientClusterDiscoveryTestsBaselineTopology : ClientClusterDiscoveryTests
    {
        /** <inheritdoc /> */
        public override void FixtureSetUp()
        {
            base.FixtureSetUp();

            var cluster = Ignition.GetAll().First().GetCluster();

            cluster.SetBaselineAutoAdjustEnabledFlag(false);
            cluster.SetBaselineTopology(cluster.TopologyVersion);
        }

        /// <summary>
        /// Tests client discovery with changing baseline topology. 
        /// </summary>
        [Test]
        public void TestClientDiscoveryWithBaselineTopologyChange()
        {
            var cache = Client.GetOrCreateCache<int, int>(TestUtils.TestName);
            
            AssertClientConnectionCount(Client, 3);
            cache.Put(1, 1);

            // Start new node.
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            AssertClientConnectionCount(Client, 4);
            cache.Put(2, 2);
            
            // Add new node to baseline.
            var cluster = ignite.GetCluster();
            cluster.SetBaselineTopology(cluster.TopologyVersion);

            AssertClientConnectionCount(Client, 4);
            cache.Put(3, 3);

            // Stop node to remove from baseline (live node can't be removed from baseline).
            ignite.Dispose();
            
            cluster = Ignition.GetAll().First().GetCluster();
            cluster.SetBaselineTopology(cluster.TopologyVersion);

            AssertClientConnectionCount(Client, 3);
            cache.Put(4, 4);
        }
    }
}
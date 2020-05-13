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
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for client cluster discovery.
    /// </summary>
    public class ClientClusterDiscoveryTests : ClientClusterDiscoveryTestsBase
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTests() : this(false, false)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTests(bool noLocalhost, bool enableSsl) : base(noLocalhost, enableSsl)
        {
            // No-op.
        }

        /// <summary>
        /// Tests random topology changes.
        /// </summary>
        [Test]
        public void TestClientDiscoveryWithRandomTopologyChanges()
        {
            var nodes = new Stack<IIgnite>();

            using (var client = GetClient())
            {
                AssertClientConnectionCount(client, 3);

                for (int i = 0; i < 20; i++)
                {
                    if (nodes.Count == 0 || TestUtils.Random.Next(2) == 0)
                    {
                        nodes.Push(Ignition.Start(GetIgniteConfiguration()));
                    }
                    else
                    {
                        nodes.Pop().Dispose();
                    }
                    
                    AssertClientConnectionCount(client, 3 + nodes.Count);
                }
            }

            foreach (var node in nodes)
            {
                node.Dispose();
            }
        }

        /// <summary>
        /// Tests that originally known node can leave and client maintains connections to other cluster nodes.
        /// </summary>
        [Test]
        public void TestClientMaintainsConnectionWhenOriginalNodeLeaves()
        {
            // Client knows about single server node initially.
            var ignite = Ignition.Start(GetIgniteConfiguration());
            var cfg = GetClientConfiguration();
            cfg.Endpoints = new[] {IPAddress.Loopback + ":10803"};

            // Client starts and discovers other server nodes.
            var client = Ignition.StartClient(cfg);
            AssertClientConnectionCount(client, 4);
            
            // Original node leaves. Client is still connected.
            ignite.Dispose();
            AssertClientConnectionCount(client, 3);
            
            // Perform any operation to verify that client works.
            Assert.AreEqual(3, client.GetCluster().GetNodes().Count);
        }

        /// <summary>
        /// Tests that thin client discovery does not include thick client nodes.
        /// </summary>
        [Test]
        public void TestThinClientDoesNotDiscoverThickClientNodes()
        {
            var cfg = GetIgniteConfiguration();
            cfg.ClientMode = true;

            using (Ignition.Start(cfg))
            {
                var client = GetClient();
                AssertClientConnectionCount(client, 3);
            }
        }

        /// <summary>
        /// Tests that server nodes without client connector are ignored by thin client discovery.
        /// </summary>
        [Test]
        public void TestDiscoveryWithoutClientConnectorOnServer()
        {
            var cfg = GetIgniteConfiguration();
            cfg.ClientConnectorConfigurationEnabled = false;
            
            using (Ignition.Start(cfg))
            {
                var client = GetClient();
                AssertClientConnectionCount(client, 3);
            }
        }

        /// <summary>
        /// Tests that Partition Awareness feature works together with Cluster Discovery.
        /// </summary>
        [Test]
        public void TestPartitionAwarenessRoutesRequestsToNewlyJoinedNodes()
        {
            if (GetType() == typeof(ClientClusterDiscoveryTestsBaselineTopology))
            {
                // Fixed baseline means that rebalance to a new node won't happen.
                return;
            }
            
            var ignite = Ignition.GetAll().First();
            var cache = ignite.CreateCache<int, int>("c");
            
            using (var ignite2 = Ignition.Start(GetIgniteConfiguration()))
            {
                var client = GetClient();
                AssertClientConnectionCount(client, 4);

                var clientCache = client.GetCache<int, int>(cache.Name);
                var logger = (ListLogger) ignite2.Logger;
                var aff = ignite2.GetAffinity(cache.Name);
                var localNode = ignite2.GetCluster().GetLocalNode();

                TestUtils.WaitForTrueCondition(() => aff.GetAllPartitions(localNode).Length > 0, 5000);
                
                var key = TestUtils.GetPrimaryKey(ignite2, cache.Name);
                
                TestUtils.WaitForTrueCondition(() =>
                {
                    clientCache.Put(key, key);

                    var log = logger.Entries.LastOrDefault(
                        e => e.Message.Contains("client.cache.ClientCachePutRequest"));

                    return log != null;
                }, 3000);
            }
        }
    }
}
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
    using System;
    using System.Linq;
    using System.Net;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Basic class for client cluster discovery tests.
    /// </summary>
    public abstract class ClientClusterDiscoveryTestsBase : ClientTestBase
    {
        /** Flag indicating whether IgniteConfiguration.Localhost should be set. */
        private readonly bool _noLocalhost;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTestsBase() : this(false, false)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTestsBase(bool noLocalhost, bool enableSsl) : base(3, enableSsl)
        {
            _noLocalhost = noLocalhost;
        }

        /// <summary>
        /// Tests that client with one initial endpoint discovers all servers.
        /// </summary>
        [Test]
        public void TestClientWithOneEndpointDiscoversAllServers()
        {
            using (var client = GetClient())
            {
                AssertClientConnectionCount(client, 3);
            }
        }

        /// <summary>
        /// Tests that client discovers new servers automatically when they join the cluster, and removes
        /// disconnected servers.
        /// </summary>
        [Test]
        public void TestClientDiscoversJoinedServersAndRemovesDisconnected()
        {
            using (var client = GetClient())
            {
                AssertClientConnectionCount(client, 3);

                using (Ignition.Start(GetIgniteConfiguration()))
                {
                    AssertClientConnectionCount(client, 4);
                }

                AssertClientConnectionCount(client, 3);
            }
        }
        
        /** <inheritdoc /> */
        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration(base.GetClientConfiguration())
            {
                EnablePartitionAwareness = true
            };
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                Localhost = _noLocalhost ? null : "127.0.0.1",
                AutoGenerateIgniteInstanceName = true
            };
        }
        
        /// <summary>
        /// Asserts client connection count.
        /// </summary>
        protected static void AssertClientConnectionCount(IIgniteClient client, int count)
        {
            var res = TestUtils.WaitForCondition(() =>
            {
                // Perform any operation to cause topology update.
                try
                {
                    client.GetCacheNames();
                }
                catch (Exception)
                {
                    // Ignore.
                }

                return count == client.GetConnections().Count();
            }, 1000);

            if (!res)
            {
                Assert.Fail("Client connection count mismatch: expected {0}, but was {1}", 
                    count, client.GetConnections().Count());
            }

            var cluster = Ignition.GetAll().First().GetCluster();

            foreach (var connection in client.GetConnections())
            {
                var server = cluster.GetNode(connection.NodeId);
                Assert.IsNotNull(server);

                var remoteEndPoint = (IPEndPoint) connection.RemoteEndPoint;
                Assert.AreEqual(server.GetAttribute<int>("clientListenerPort"), remoteEndPoint.Port);

                var ipAddresses = server.Addresses
                    .Select(a => a.Split('%').First())  // Trim IPv6 scope.
                    .Select(IPAddress.Parse)
                    .ToArray();
                
                CollectionAssert.Contains(ipAddresses, remoteEndPoint.Address);

                var localEndPoint = (IPEndPoint) connection.LocalEndPoint;
                CollectionAssert.Contains(new[] {IPAddress.Loopback, IPAddress.IPv6Loopback}, localEndPoint.Address);
            }
        }
    }
}
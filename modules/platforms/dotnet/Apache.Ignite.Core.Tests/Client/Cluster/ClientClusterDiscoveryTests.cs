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
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for client cluster discovery: client connects to any node first, retrieves all server endpoints,
    /// and connects to all of them.
    /// </summary>
    public class ClientClusterDiscoveryTests : ClientTestBase
    {
        /** Flag indicating whether IgniteConfiguration.Localhost should be set. */
        private readonly bool _noLocalhost;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTests() : this(false)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTests(bool noLocalhost) : base(3)
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
                Assert.IsTrue(client.GetConfiguration().EnableDiscovery);
                
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

                var cfg = GetIgniteConfiguration();
                cfg.AutoGenerateIgniteInstanceName = true;

                using (Ignition.Start(cfg))
                {
                    AssertClientConnectionCount(client, 4);
                }

                AssertClientConnectionCount(client, 3);
            }
        }

        /// <summary>
        /// Tests that SSL + Discovery combination works.
        /// </summary>
        [Test]
        public void TestClientDiscoversAllServersWithSsl()
        {
            Assert.Fail("TODO");
        }

        /** <inheritdoc /> */
        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration(base.GetClientConfiguration())
            {
                EnableDiscovery = true,
                EnablePartitionAwareness = true
            };
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                Localhost = _noLocalhost ? null : "127.0.0.1"
            };
        }
        
        /// <summary>
        /// Asserts client connection count.
        /// </summary>
        private static void AssertClientConnectionCount(IIgniteClient client, int count)
        {
            TestUtils.WaitForTrueCondition(() =>
            {
                // Perform any operation to cause topology update.
                client.GetCacheNames();

                return count == client.GetConnections().Count();
            });
        }
    }
}
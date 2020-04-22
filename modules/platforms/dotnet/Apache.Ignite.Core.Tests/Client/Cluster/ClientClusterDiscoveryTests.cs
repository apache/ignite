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
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for client cluster discovery: client connects to any node first, retrieves all server endpoints,
    /// and connects to all of them.
    /// </summary>
    public class ClientClusterDiscoveryTests : ClientTestBase
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ClientClusterDiscoveryTests"/>.
        /// </summary>
        public ClientClusterDiscoveryTests() : base(3)
        {
            // No-op.
        }

        /// <summary>
        /// Tests that client with one initial endpoint discovers all servers.
        /// </summary>
        [Test]
        public void TestClientWithOneEndpointDiscoversAllServers()
        {
            var cfg = new IgniteClientConfiguration(GetClientConfiguration())
            {
                EnableDiscovery = true,
            };

            using (var client = Ignition.StartClient(cfg))
            {
                Assert.IsTrue(client.GetConfiguration().EnableDiscovery);
            }
        }
    }
}
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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests client connection: port ranges, version checks, etc.
    /// </summary>
    public class ClientConnectionTest
    {
        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that missing server yields connection refused error.
        /// </summary>
        [Test]
        public void TestNoServerConnectionRefused()
        {
            var ex = Assert.Throws<AggregateException>(() => StartClient());
            var socketEx = ex.InnerExceptions.OfType<SocketException>().First();
            Assert.AreEqual(SocketError.ConnectionRefused, socketEx.SocketErrorCode);
        }

        /// <summary>
        /// Tests that multiple clients can connect to one server.
        /// </summary>
        [Test]
        public void TestMultipleClients()
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var client1 = StartClient();
                var client2 = StartClient();
                var client3 = StartClient();

                client1.Dispose();
                client2.Dispose();
                client3.Dispose();
            }
        }

        /// <summary>
        /// Tests custom connector and client configuration.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestCustomConfig()
        {
            var servCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    Host = "localhost",
                    Port = 2000,
                    PortRange = 1
                }
            };

            var clientCfg = new IgniteClientConfiguration
            {
                Host = "localhost",
                Port = 2000
            };

            using (Ignition.Start(servCfg))
            using (Ignition.StartClient(clientCfg))
            {
                // No-op.
            }
        }

        /// <summary>
        /// Tests that default configuration throws.
        /// </summary>
        [Test]
        public void TestDefaultConfigThrows()
        {
            Assert.Throws<ArgumentNullException>(() => Ignition.StartClient(new IgniteClientConfiguration()));
        }

#if !NETCOREAPP2_0
        /// <summary>
        /// Tests the incorrect protocol version error.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestIncorrectProtocolVersionError()
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                // ReSharper disable once ObjectCreationAsStatement
                var ex = Assert.Throws<IgniteClientException>(() =>
                    new Impl.Client.ClientSocket(GetClientConfiguration(),
                    new Impl.Client.ClientProtocolVersion(-1, -1, -1)));

                Assert.AreEqual((int) Impl.Client.ClientStatus.Fail, ex.ErrorCode);

                Assert.AreEqual("Client handhsake failed: 'Unsupported version.'. " +
                                "Client version: -1.-1.-1. Server version: 1.0.0", ex.Message);
            }
        }
#endif

        /// <summary>
        /// Tests that connector can be disabled.
        /// </summary>
        [Test]
        public void TestDisabledConnector()
        {
            var servCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientConnectorConfigurationEnabled = false
            };

            var clientCfg = new IgniteClientConfiguration
            {
                Host = "localhost"
            };

            using (Ignition.Start(servCfg))
            {
                var ex = Assert.Throws<AggregateException>(() => Ignition.StartClient(clientCfg));
                Assert.AreEqual("Failed to establish Ignite thin client connection, " +
                                "examine inner exceptions for details.", ex.Message);
            }
        }

        /// <summary>
        /// Starts the client.
        /// </summary>
        private static IIgniteClient StartClient()
        {
            return Ignition.StartClient(GetClientConfiguration());
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        private static IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration { Host = IPAddress.Loopback.ToString() };
        }
    }
}

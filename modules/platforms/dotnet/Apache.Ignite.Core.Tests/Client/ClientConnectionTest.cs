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
    using System.Net.Sockets;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Client;
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
            var ex = Assert.Throws<AggregateException>(() => Ignition.GetClient());
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
                var client1 = Ignition.GetClient();
                var client2 = Ignition.GetClient();
                var client3 = Ignition.GetClient();

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
                SqlConnectorConfiguration = new SqlConnectorConfiguration
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
            using (Ignition.GetClient(clientCfg))
            {
                // No-op.
            }
        }

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
                var ex = Assert.Throws<IgniteException>(() => new ClientSocket(new IgniteClientConfiguration(),
                    new ClientProtocolVersion(-1, -1, -1)));

                Assert.AreEqual("Client handhsake failed: 'Unsupported version.'. " +
                                "Client version: -1.-1.-1. Server version: 2.1.5", ex.Message);
            }
        }
    }
}

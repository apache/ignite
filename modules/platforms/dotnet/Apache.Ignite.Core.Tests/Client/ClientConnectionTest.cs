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
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
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
        [TearDown]
        public void TearDown()
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
                    PortRange = 1,
                    SocketSendBufferSize = 100,
                    SocketReceiveBufferSize = 50
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

                Assert.AreEqual(ClientStatusCode.Fail, ex.StatusCode);

                Assert.AreEqual("Client handshake failed: 'Unsupported version.'. " +
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
                                "examine inner exceptions for details.", ex.Message.Substring(0, 88));
            }
        }

        /// <summary>
        /// Tests that we get a proper exception when server disconnects (node shutdown, network issues, etc).
        /// </summary>
        [Test]
        public void TestServerConnectionAborted()
        {
            var evt = new ManualResetEventSlim();
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            var putGetTask = Task.Factory.StartNew(() =>
            {
                using (var client = StartClient())
                {
                    var cache = client.GetOrCreateCache<int, int>("foo");
                    evt.Set();

                    for (var i = 0; i < 100000; i++)
                    {
                        cache[i] = i;
                        Assert.AreEqual(i, cache.GetAsync(i).Result);
                    }
                }
            });

            evt.Wait();
            ignite.Dispose();

            var ex = Assert.Throws<AggregateException>(() => putGetTask.Wait());
            var baseEx = ex.GetBaseException();
            var socketEx = baseEx as SocketException;

            if (socketEx != null)
            {
                Assert.AreEqual(SocketError.ConnectionAborted, socketEx.SocketErrorCode);
            }
            else
            {
                Assert.Fail("Unexpected exception: " + ex);
            }
        }

        /// <summary>
        /// Tests the operation timeout.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestOperationTimeout()
        {
            var data = Enumerable.Range(1, 500000).ToDictionary(x => x, x => x.ToString());

            Ignition.Start(TestUtils.GetTestConfiguration());

            var cfg = GetClientConfiguration();
            cfg.SocketTimeout = TimeSpan.FromMilliseconds(500);
            var client = Ignition.StartClient(cfg);
            var cache = client.CreateCache<int, string>("s");

            // Async.
            var task = cache.PutAllAsync(data);
            Assert.IsFalse(task.IsCompleted);
            var aex = Assert.Throws<AggregateException>(() => task.Wait());
            Assert.AreEqual(SocketError.TimedOut, ((SocketException) aex.GetBaseException()).SocketErrorCode);

            // Sync (reconnect for clean state).
            client = Ignition.StartClient(cfg);
            cache = client.GetCache<int, string>("s");
            var ex = Assert.Throws<SocketException>(() => cache.PutAll(data));
            Assert.AreEqual(SocketError.TimedOut, ex.SocketErrorCode);
        }

        /// <summary>
        /// Tests the client dispose while operations are in progress.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestClientDisposeWhileOperationsAreInProgress()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());

            var ops = new List<Task>();

            using (var client = StartClient())
            {
                var cache = client.GetOrCreateCache<int, int>("foo");
                for (var i = 0; i < 100000; i++)
                {
                    ops.Add(cache.PutAsync(i, i));
                }
                ops.First().Wait();
            }

            var completed = ops.Count(x => x.Status == TaskStatus.RanToCompletion);
            Assert.Greater(completed, 0, "Some tasks should have completed.");

            var failed = ops.Where(x => x.Status == TaskStatus.Faulted).ToArray();
            Assert.IsTrue(failed.Any(), "Some tasks should have failed.");

            foreach (var task in failed)
            {
                var ex = task.Exception;
                Assert.IsNotNull(ex);
                var baseEx = ex.GetBaseException();
                Assert.IsNotNull((object) (baseEx as SocketException) ?? baseEx as ObjectDisposedException, 
                    ex.ToString());
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

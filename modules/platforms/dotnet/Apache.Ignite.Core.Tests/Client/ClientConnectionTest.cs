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
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests client connection: port ranges, version checks, etc.
    /// </summary>
    public class ClientConnectionTest
    {
        /** Temp dir for WAL. */
        private readonly string _tempDir = TestUtils.GetTempDirectoryName();

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.ClearWorkDir();
        }

        /// <summary>
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);

            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, true);
            }

            TestUtils.ClearWorkDir();
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
        /// Tests that empty username or password are not allowed.
        /// </summary>
        [Test]
        public void TestAuthenticationEmptyCredentials()
        {
            using (Ignition.Start(SecureServerConfig()))
            {
                var cliCfg = GetSecureClientConfig();

                cliCfg.Password = null;
                var ex = Assert.Throws<IgniteClientException>(() => { Ignition.StartClient(cliCfg); });
                Assert.IsTrue(ex.Message.StartsWith("IgniteClientConfiguration.Password cannot be null"));

                cliCfg.Password = "";
                ex = Assert.Throws<IgniteClientException>(() => { Ignition.StartClient(cliCfg); });
                Assert.IsTrue(ex.Message.StartsWith("IgniteClientConfiguration.Password cannot be empty"));

                cliCfg.Password = "ignite";

                cliCfg.UserName = null;
                ex = Assert.Throws<IgniteClientException>(() => { Ignition.StartClient(cliCfg); });
                Assert.IsTrue(ex.Message.StartsWith("IgniteClientConfiguration.UserName cannot be null"));

                cliCfg.UserName = "";
                ex = Assert.Throws<IgniteClientException>(() => { Ignition.StartClient(cliCfg); });
                Assert.IsTrue(ex.Message.StartsWith("IgniteClientConfiguration.Username cannot be empty"));
            }
        }

        /// <summary>
        /// Test invalid username or password.
        /// </summary>
        [Test]
        public void TestAuthenticationInvalidCredentials()
        {
            using (Ignition.Start(SecureServerConfig()))
            {
                var cliCfg = GetSecureClientConfig();

                cliCfg.UserName = "invalid";

                var ex = Assert.Throws<IgniteClientException>(() => { Ignition.StartClient(cliCfg); });
                Assert.True(ex.StatusCode == ClientStatusCode.AuthenticationFailed);

                cliCfg.UserName = "ignite";
                cliCfg.Password = "invalid";

                ex = Assert.Throws<IgniteClientException>(() => { Ignition.StartClient(cliCfg); });
                Assert.True(ex.StatusCode == ClientStatusCode.AuthenticationFailed);
            }
        }

        /// <summary>
        /// Test authentication.
        /// </summary>
        [Test]
        public void TestAuthentication()
        {
            using (var srv = Ignition.Start(SecureServerConfig()))
            {
                srv.GetCluster().SetActive(true);

                using (var cli = Ignition.StartClient(GetSecureClientConfig()))
                {
                    CacheClientConfiguration ccfg = new CacheClientConfiguration
                    {
                        Name = "TestCache",
                        QueryEntities = new[]
                        {
                            new QueryEntity
                            {
                                KeyType = typeof(string),
                                ValueType = typeof(string),
                            },
                        },
                    };

                    ICacheClient<string, string> cache = cli.GetOrCreateCache<string, string>(ccfg);

                    cache.Put("key1", "val1");

                    cache.Query(new SqlFieldsQuery("CREATE USER \"my_User\" WITH PASSWORD 'my_Password'")).GetAll();
                }

                var cliCfg = GetSecureClientConfig();

                cliCfg.UserName = "my_User";
                cliCfg.Password = "my_Password";

                using (var cli = Ignition.StartClient(cliCfg))
                {
                    ICacheClient<string, string> cache = cli.GetCache<string, string>("TestCache");

                    string val = cache.Get("key1");

                    Assert.True(val == "val1");
                }
            }
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
                Endpoints = new[] {"localhost:2000"}
            };

            using (Ignition.Start(servCfg))
            using (var client = Ignition.StartClient(clientCfg))
            {
                Assert.AreNotEqual(clientCfg, client.GetConfiguration());
                Assert.AreNotEqual(client.GetConfiguration(), client.GetConfiguration());
                Assert.AreEqual(clientCfg.ToXml(), client.GetConfiguration().ToXml());
            }
        }

        /// <summary>
        /// Tests client config with EndPoints property.
        /// </summary>
        [Test]
        public void TestEndPoints()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                ignite.CreateCache<int, int>("foo");

                const int port = IgniteClientConfiguration.DefaultPort;

                // DnsEndPoint.
                var cfg = new IgniteClientConfiguration
                {
                    Endpoints = new[] { "localhost" }
                };

                using (var client = Ignition.StartClient(cfg))
                {
                    Assert.AreEqual("foo", client.GetCacheNames().Single());
                }

                // IPEndPoint.
                cfg = new IgniteClientConfiguration
                {
                    Endpoints = new[] { "127.0.0.1:" + port }
                };

                using (var client = Ignition.StartClient(cfg))
                {
                    Assert.AreEqual("foo", client.GetCacheNames().Single());
                }
            }
        }

        /// <summary>
        /// Tests that default configuration throws.
        /// </summary>
        [Test]
        public void TestDefaultConfigThrows()
        {
            Assert.Throws<IgniteClientException>(() => Ignition.StartClient(new IgniteClientConfiguration()));
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
                var ex = Assert.Throws<IgniteClientException>(() =>
                    new ClientSocket(GetClientConfiguration(),
                        new DnsEndPoint(
                            "localhost",
                            ClientConnectorConfiguration.DefaultPort,
                            AddressFamily.InterNetwork),
                        null,
                        null,
                        new ClientProtocolVersion(-1, -1, -1)));

                Assert.AreEqual(ClientStatusCode.Fail, ex.StatusCode);

                Assert.IsTrue(Regex.IsMatch(ex.Message, "Client handshake failed: 'Unsupported version.'. " +
                                "Client version: -1.-1.-1. Server version: [0-9]+.[0-9]+.[0-9]+"));
            }
        }

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
                Endpoints = new[] {"localhost"}
            };

            using (Ignition.Start(servCfg))
            {
                var ex = Assert.Throws<AggregateException>(() => Ignition.StartClient(clientCfg));
                Assert.AreEqual("Failed to establish Ignite thin client connection, " +
                                "examine inner exceptions for details.", ex.Message.Substring(0, 88));
            }

            // Disable only thin client.
            servCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    ThinClientEnabled = false
                }
            };

            using (Ignition.Start(servCfg))
            {
                var ex = Assert.Throws<IgniteClientException>(() => Ignition.StartClient(clientCfg));
                Assert.AreEqual("Client handshake failed: 'Thin client connection is not allowed, " +
                                "see ClientConnectorConfiguration.thinClientEnabled.'.",
                                ex.Message.Substring(0, 118));
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

            var putGetTask = TaskRunner.Run(() =>
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
            Assert.AreEqual(cfg.SocketTimeout, client.GetConfiguration().SocketTimeout);

            // Async.
            var task = cache.PutAllAsync(data);
            Assert.IsFalse(task.IsCompleted);
            var ex = Assert.Catch(() => task.Wait());
            Assert.AreEqual(SocketError.TimedOut, GetSocketException(ex).SocketErrorCode);

            // Sync (reconnect for clean state).
            Ignition.StopAll(true);
            Ignition.Start(TestUtils.GetTestConfiguration());
            client = Ignition.StartClient(cfg);
            cache = client.CreateCache<int, string>("s");
            ex = Assert.Catch(() => cache.PutAll(data));
            Assert.AreEqual(SocketError.TimedOut, GetSocketException(ex).SocketErrorCode);
        }

        /// <summary>
        /// Tests the client dispose while operations are in progress.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestClientDisposeWhileOperationsAreInProgress()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());

            const int count = 100000;
            var ops = new Task[count];

            using (var client = StartClient())
            {
                var cache = client.GetOrCreateCache<int, int>("foo");
                Parallel.For(0, count, new ParallelOptions {MaxDegreeOfParallelism = 16},
                    i =>
                    {
                        ops[i] = cache.PutAsync(i, i);
                    });
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
        /// Tests the <see cref="ClientConnectorConfiguration.IdleTimeout"/> property.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestIdleTimeout()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    IdleTimeout = TimeSpan.FromMilliseconds(100)
                }
            };

            var ignite = Ignition.Start(cfg);
            Assert.AreEqual(100, ignite.GetConfiguration().ClientConnectorConfiguration.IdleTimeout.TotalMilliseconds);

            using (var client = StartClient())
            {
                var cache = client.GetOrCreateCache<int, int>("foo");
                cache[1] = 1;
                Assert.AreEqual(1, cache[1]);

                Thread.Sleep(90);
                Assert.AreEqual(1, cache[1]);

                // Idle check frequency is 2 seconds.
                Thread.Sleep(4000);
                var ex = Assert.Catch(() => cache.Get(1));
                Assert.AreEqual(SocketError.ConnectionAborted, GetSocketException(ex).SocketErrorCode);
            }
        }

        /// <summary>
        /// Tests the protocol mismatch behavior: attempt to connect to an HTTP endpoint.
        /// </summary>
        [Test]
        public void TestProtocolMismatch()
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                // Connect to Ignite REST endpoint.
                var cfg = new IgniteClientConfiguration("127.0.0.1:11211");
                var ex = GetSocketException(Assert.Catch(() => Ignition.StartClient(cfg)));
                Assert.AreEqual(SocketError.ConnectionAborted, ex.SocketErrorCode);
            }
        }

        /// <summary>
        /// Tests reconnect logic with single server.
        /// </summary>
        [Test]
        public void TestReconnect()
        {
            // Connect client and check.
            Ignition.Start(TestUtils.GetTestConfiguration());
            var client = Ignition.StartClient(new IgniteClientConfiguration("127.0.0.1"));
            Assert.AreEqual(0, client.GetCacheNames().Count);

            var ep = client.RemoteEndPoint as IPEndPoint;
            Assert.IsNotNull(ep);
            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, ep.Port);
            Assert.AreEqual("127.0.0.1", ep.Address.ToString());

            ep = client.LocalEndPoint as IPEndPoint;
            Assert.IsNotNull(ep);
            Assert.AreNotEqual(IgniteClientConfiguration.DefaultPort, ep.Port);
            Assert.AreEqual("127.0.0.1", ep.Address.ToString());

            // Stop server.
            Ignition.StopAll(true);

            // First request fails, error is detected.
            var ex = Assert.Catch(() => client.GetCacheNames());
            Assert.IsNotNull(GetSocketException(ex));

            // Second request causes reconnect attempt which fails (server is stopped).
            var aex = Assert.Throws<AggregateException>(() => client.GetCacheNames());
            Assert.AreEqual("Failed to establish Ignite thin client connection, " +
                            "examine inner exceptions for details.", aex.Message.Substring(0, 88));

            // Start server, next operation succeeds.
            Ignition.Start(TestUtils.GetTestConfiguration());
            Assert.AreEqual(0, client.GetCacheNames().Count);
        }

        /// <summary>
        /// Tests disabled reconnect behavior.
        /// </summary>
        [Test]
        public void TestReconnectDisabled()
        {
            // Connect client and check.
            Ignition.Start(TestUtils.GetTestConfiguration());
            using (var client = Ignition.StartClient(new IgniteClientConfiguration("127.0.0.1")
            {
                ReconnectDisabled = true
            }))
            {
                Assert.AreEqual(0, client.GetCacheNames().Count);

                // Stop server.
                Ignition.StopAll(true);

                // Request fails, error is detected.
                var ex = Assert.Catch(() => client.GetCacheNames());
                Assert.IsNotNull(GetSocketException(ex));

                // Restart server, client does not reconnect.
                Ignition.Start(TestUtils.GetTestConfiguration());
                ex = Assert.Catch(() => client.GetCacheNames());
                Assert.IsNotNull(GetSocketException(ex));
            }
        }

        /// <summary>
        /// Tests reconnect logic with multiple servers.
        /// </summary>
        [Test]
        public void TestFailover()
        {
            // Start 3 nodes.
            Ignition.Start(TestUtils.GetTestConfiguration(name: "0"));
            Ignition.Start(TestUtils.GetTestConfiguration(name: "1"));
            Ignition.Start(TestUtils.GetTestConfiguration(name: "2"));

            // Connect client.
            var port = IgniteClientConfiguration.DefaultPort;
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[]
                {
                    "localhost",
                    string.Format("127.0.0.1:{0}..{1}", port + 1, port + 2)
                }
            };

            using (var client = Ignition.StartClient(cfg))
            {
                Assert.AreEqual(0, client.GetCacheNames().Count);

                // Stop target node.
                var nodeId = ((IPEndPoint) client.RemoteEndPoint).Port - port;
                Ignition.Stop(nodeId.ToString(), true);

                // Check failure.
                Assert.IsNotNull(GetSocketException(Assert.Catch(() => client.GetCacheNames())));

                // Check reconnect.
                Assert.AreEqual(0, client.GetCacheNames().Count);

                // Stop target node.
                nodeId = ((IPEndPoint) client.RemoteEndPoint).Port - port;
                Ignition.Stop(nodeId.ToString(), true);

                // Check failure.
                Assert.IsNotNull(GetSocketException(Assert.Catch(() => client.GetCacheNames())));

                // Check reconnect.
                Assert.AreEqual(0, client.GetCacheNames().Count);

                // Stop all nodes.
                Ignition.StopAll(true);
                Assert.IsNotNull(GetSocketException(Assert.Catch(() => client.GetCacheNames())));
                Assert.IsNotNull(GetSocketException(Assert.Catch(() => client.GetCacheNames())));
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
            return new IgniteClientConfiguration(IPAddress.Loopback.ToString());
        }

        /// <summary>
        /// Finds SocketException in the hierarchy.
        /// </summary>
        private static SocketException GetSocketException(Exception ex)
        {
            Assert.IsNotNull(ex);
            var origEx = ex;

            while (ex != null)
            {
                var socketEx = ex as SocketException;

                if (socketEx != null)
                {
                    return socketEx;
                }

                ex = ex.InnerException;
            }

            throw new Exception("SocketException not found.", origEx);
        }

        /// <summary>
        /// Create server configuration with enabled authentication.
        /// </summary>
        /// <returns>Server configuration.</returns>
        private IgniteConfiguration SecureServerConfig()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                AuthenticationEnabled = true,
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    StoragePath = Path.Combine(_tempDir, "Store"),
                    WalPath = Path.Combine(_tempDir, "WalStore"),
                    WalArchivePath = Path.Combine(_tempDir, "WalArchive"),
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "default",
                        PersistenceEnabled = true
                    }
                }
            };
        }

        /// <summary>
        /// Create client configuration with enabled authentication.
        /// </summary>
        /// <returns>Client configuration.</returns>
        private static IgniteClientConfiguration GetSecureClientConfig()
        {
            return new IgniteClientConfiguration("localhost")
            {
                UserName = "ignite",
                Password = "ignite"
            };
        }
    }
}

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
        /// Fixture tear down.
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
                var cliCfg = SecureClientConfig();

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
                var cliCfg = SecureClientConfig();

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

                using (var cli = Ignition.StartClient(SecureClientConfig()))
                {
                    CacheClientConfiguration ccfg = new CacheClientConfiguration()
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

                var cliCfg = SecureClientConfig();

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
                Host = "localhost",
                Port = 2000
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
        /// Tests that default configuration throws.
        /// </summary>
        [Test]
        public void TestDefaultConfigThrows()
        {
            Assert.Throws<ArgumentNullException>(() => Ignition.StartClient(new IgniteClientConfiguration()));
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
                Host = "localhost"
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
                var cfg = new IgniteClientConfiguration {Host = "127.0.0.1", Port = 11211 };
                var ex = Assert.Throws<SocketException>(() => Ignition.StartClient(cfg));
                Assert.AreEqual(SocketError.ConnectionAborted, ex.SocketErrorCode);
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
                DataStorageConfiguration = new DataStorageConfiguration()
                {
                    StoragePath = Path.Combine(_tempDir, "Store"),
                    WalPath = Path.Combine(_tempDir, "WalStore"),
                    WalArchivePath = Path.Combine(_tempDir, "WalArchive"),
                    DefaultDataRegionConfiguration = new DataRegionConfiguration()
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
        private static IgniteClientConfiguration SecureClientConfig()
        {
            return new IgniteClientConfiguration()
            {
                Host = "localhost",
                UserName = "ignite",
                Password = "ignite"
            };
        }
    }
}

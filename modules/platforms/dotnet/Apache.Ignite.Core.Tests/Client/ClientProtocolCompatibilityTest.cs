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
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client with old protocol versions.
    /// </summary>
    public class ClientProtocolCompatibilityTest : ClientTestBase
    {
        /// <summary>
        /// Tests that basic cache operations are supported on all protocols.
        /// </summary>
        [Test]
        public void TestCacheOperationsAreSupportedOnAllProtocols(
            [Values(0, 1, 2, 3, 4, 5, 6)] short minor)
        {
            var version = new ClientProtocolVersion(1, minor, 0);
            
            using (var client = GetClient(version, true))
            {
                var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                cache.Put(1, -1);
                cache.PutAll(Enumerable.Range(2, 10).ToDictionary(x => x, x => x));
                
                Assert.AreEqual(-1, cache.Get(1));
                Assert.AreEqual(11, cache.Query(new ScanQuery<int, int>()).GetAll().Count);
            }
        }

        /// <summary>
        /// Tests that cluster operations throw proper exception on older server versions.
        /// </summary>
        [Test]
        public void TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150(
            [Values(0, 1, 2, 3, 4)] short minor)
        {
            var version = new ClientProtocolVersion(1, minor, 0);
            
            using (var client = GetClient(version))
            {
                TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150(client, version.ToString());
            }
        }

        /// <summary>
        /// Tests that partition awareness disables automatically on older server versions.
        /// </summary>
        [Test]
        public void TestPartitionAwarenessDisablesAutomaticallyOnVersionsOlderThan140(
            [Values(0, 1, 2, 3)] short minor)
        {
            var version = new ClientProtocolVersion(1, minor, 0);
            
            using (var client = GetClient(version, true))
            {
                Assert.IsFalse(client.GetConfiguration().EnablePartitionAwareness);
                
                var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                cache.Put(1, 2);
                Assert.AreEqual(2, cache.Get(1));

                var log = GetLogs(client).Last();
                var expectedMessage = string.Format("Partition awareness has been disabled: server protocol version " +
                                                    "{0} is lower than required 1.4.0", version);
                
                Assert.AreEqual(expectedMessage, log.Message);
                Assert.AreEqual(LogLevel.Warn, log.Level);
                Assert.AreEqual(typeof(ClientFailoverSocket).Name, log.Category);
            }
        }

        /// <summary>
        /// Tests that client can connect to old server nodes and negotiate common protocol version.
        /// </summary>
        [Test]
        public void TestClientNewerThanServerReconnectsOnServerVersion()
        {
            // Use a non-existent version that is not supported by the server
            var version = new ClientProtocolVersion(short.MaxValue, short.MaxValue, short.MaxValue);

            using (var client = GetClient(version))
            {
                Assert.AreEqual(ClientSocket.CurrentProtocolVersion, client.Socket.CurrentProtocolVersion);

                var logs = GetLogs(client);

                var expectedMessage = "Handshake failed on 127.0.0.1:10800, " +
                                      "requested protocol version = 32767.32767.32767, server protocol version = , " +
                                      "status = Fail, message = Unsupported version.";

                var message = Regex.Replace(
                    logs[2].Message, @"server protocol version = \d\.\d\.\d", "server protocol version = ");

                Assert.AreEqual(expectedMessage, message);
            }
        }

        /// <summary>
        /// Tests that old client with new server can negotiate a protocol version.
        /// </summary>
        [Test]
        public void TestClientOlderThanServerConnectsOnClientVersion([Values(0, 1, 2, 3, 4, 5)] short minor)
        {
            var version = new ClientProtocolVersion(1, minor, 0);

            using (var client = GetClient(version))
            {
                Assert.AreEqual(version, client.Socket.CurrentProtocolVersion);

                var lastLog = GetLogs(client).Last();
                var expectedLog = string.Format(
                    "Handshake completed on 127.0.0.1:10800, protocol version = {0}", version);
                
                Assert.AreEqual(expectedLog, lastLog.Message);
                Assert.AreEqual(LogLevel.Debug, lastLog.Level);
                Assert.AreEqual(typeof(ClientSocket).Name, lastLog.Category);
            }
        }

        /// <summary>
        /// Asserts correct exception for cluster operations.
        /// </summary>
        public static void TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150(IIgniteClient client,
            string version)
        {
            var cluster = client.GetCluster();

            AssertNotSupportedOperation(() => cluster.IsActive(), version, "ClusterIsActive");
            AssertNotSupportedOperation(() => cluster.SetActive(true), version, "ClusterChangeState");
            AssertNotSupportedOperation(() => cluster.IsWalEnabled("c"), version, "ClusterGetWalState");
            AssertNotSupportedOperation(() => cluster.EnableWal("c"), version, "ClusterChangeWalState");
            AssertNotSupportedOperation(() => cluster.DisableWal("c"), version, "ClusterChangeWalState");
        }
        
        /// <summary>
        /// Asserts proper exception for non-supported operation.
        /// </summary>
        public static void AssertNotSupportedOperation(Action action, string version,
            string expectedOperationName)
        {
            var ex = Assert.Throws<IgniteClientException>(() => action());
            
            var expectedMessage = string.Format(
                "Operation {0} is not supported by protocol version {1}. " +
                "Minimum protocol version required is 1.5.0.",
                expectedOperationName, version);

            Assert.AreEqual(expectedMessage, ex.Message);
        }

        /// <summary>
        /// Gets the client with specified protocol version.
        /// </summary>
        private IgniteClient GetClient(ClientProtocolVersion version, bool enablePartitionAwareness = false)
        {
            var cfg = new IgniteClientConfiguration(GetClientConfiguration())
            {
                ProtocolVersion = version,
                EnablePartitionAwareness = enablePartitionAwareness
            };

            return (IgniteClient) Ignition.StartClient(cfg);
        }
    }
}
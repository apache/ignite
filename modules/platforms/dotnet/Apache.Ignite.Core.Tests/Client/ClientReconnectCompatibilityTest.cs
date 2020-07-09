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
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client compatibility with reconnect.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]    
    public class ClientReconnectCompatibilityTest
    {
        /// <summary>
        /// Tests that client reconnect to an old server node disables partition awareness.
        /// </summary>
        [Test]
        public void TestReconnectToOldNodeDisablesPartitionAwareness()
        {
            IIgniteClient client = null;
            var clientConfiguration = new IgniteClientConfiguration(JavaServer.GetClientConfiguration())
            {
                EnablePartitionAwareness = true,
                Logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace})
            };
            
            try
            {
                using (StartNewServer())
                {
                    client = Ignition.StartClient(clientConfiguration);
                    var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                    cache.Put(1, 42);
                    Assert.AreEqual(42, cache.Get(1));
                    Assert.IsTrue(client.GetConfiguration().EnablePartitionAwareness);
                }

                Assert.Catch(() => client.GetCacheNames());

                using (StartOldServer())
                {
                    var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                    cache.Put(1, 42);
                    Assert.AreEqual(42, cache.Get(1));
                    Assert.IsFalse(client.GetConfiguration().EnablePartitionAwareness);

                    var log = ((ListLogger) client.GetConfiguration().Logger).Entries
                        .FirstOrDefault(e => e.Message.StartsWith("Partition"));

                    Assert.IsNotNull(log);
                    Assert.AreEqual("Partition awareness has been disabled: " +
                                    "server protocol version 1.0.0 is lower than required 1.4.0", log.Message);
                }
            }
            finally
            {
                if (client != null)
                {
                    client.Dispose();
                }
            }
        }

        /// <summary>
        /// Starts new server node (partition awareness is supported).
        /// </summary>
        private static IDisposable StartNewServer()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    Port = JavaServer.ClientPort
                }
            };

            return Ignition.Start(cfg);
        }

        /// <summary>
        /// Starts old server node (partition awareness is not supported).
        /// </summary>
        private static IDisposable StartOldServer()
        {
            return JavaServer.Start(JavaServer.GroupIdIgnite, "2.4.0");
        }
    }
}
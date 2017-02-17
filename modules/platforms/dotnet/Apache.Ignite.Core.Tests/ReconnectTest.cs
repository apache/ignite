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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Client reconnect tests.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ReconnectTest
    {
        /** */
        private const string CacheName = "cache";

        /// <summary>
        /// Tests the cluster restart scenario, where client is alive, but all servers restart.
        /// </summary>
        [Test]
        public void TestClusterRestart()
        {
            var serverCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new[] {new CacheConfiguration(CacheName)}
            };

            var clientCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = "client",
                ClientMode = true
            };

            var server = Ignition.Start(serverCfg);

            Assert.AreEqual(1, server.GetCluster().GetNodes().Count);

            var client = Ignition.Start(clientCfg);

            Assert.AreEqual(2, client.GetCluster().GetNodes().Count);

            ClientReconnectEventArgs eventArgs = null;

            client.ClientReconnected += (sender, args) => { eventArgs = args; };

            var cache = client.GetCache<int, int>(CacheName);

            cache[1] = 1;

            Ignition.Stop(server.Name, true);

            var cacheEx = Assert.Throws<CacheException>(() => cache.Get(1));
            var ex = cacheEx.InnerException as ClientDisconnectedException;

            Assert.IsNotNull(ex);

            // Wait a bit for cluster restart detection.
            Thread.Sleep(1000);

            // Start the server and wait for reconnect.
            Ignition.Start(serverCfg);

            // Check reconnect task.
            Assert.IsTrue(ex.ClientReconnectTask.Result);
            
            // Wait a bit for notifications.
            Thread.Sleep(100);

            // Check the event args.
            Assert.IsNotNull(eventArgs);
            Assert.IsTrue(eventArgs.HasClusterRestarted);

            // Refresh the cache instance and check that it works.
            var cache1 = client.GetCache<int, int>(CacheName);
            Assert.AreEqual(0, cache1.GetSize());

            cache1[1] = 2;
            Assert.AreEqual(2, cache1[1]);

            // Check that old cache instance does not work.
            var cacheEx1 = Assert.Throws<InvalidOperationException>(() => cache.Get(1));
            Assert.AreEqual("Cache has been closed or destroyed: " + CacheName, cacheEx1.Message);
        }

        /// <summary>
        /// Tests the failed connection scenario, where servers are alive, but can't be contacted.
        /// </summary>
        [Test]
        public void TestFailedConnection()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\reconnect-test.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            var proc = StartServerProcess(cfg);

            Ignition.ClientMode = true;

            using (var ignite = Ignition.Start(cfg))
            {
                var reconnected = 0;
                var disconnected = 0;
                ignite.ClientDisconnected += (sender, args) => { disconnected++; };
                ignite.ClientReconnected += (sender, args) => { reconnected += args.HasClusterRestarted ? 10 : 1; };

                Assert.IsTrue(ignite.GetCluster().ClientReconnectTask.IsCompleted);

                var cache = ignite.CreateCache<int, int>(CacheName);

                cache[1] = 1;

                // Suspend external process to cause disconnect
                proc.Suspend();

                var ex = Assert.Throws<CacheException>(() => cache.Get(1));

                Assert.IsTrue(ex.ToString().Contains(
                    "javax.cache.CacheException: class org.apache.ignite.IgniteClientDisconnectedException: " +
                    "Operation has been cancelled (client node disconnected)"));

                var inner = (ClientDisconnectedException) ex.InnerException;

                Assert.IsNotNull(inner);

                var clientReconnectTask = inner.ClientReconnectTask;

                Assert.AreEqual(ignite.GetCluster().ClientReconnectTask, clientReconnectTask);
                Assert.AreEqual(1, disconnected);
                Assert.AreEqual(0, reconnected);

                // Resume process to reconnect
                proc.Resume();

                Assert.IsFalse(clientReconnectTask.Result);

                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(1, disconnected);

                Thread.Sleep(100);  // Wait for event handler
                Assert.AreEqual(1, reconnected);
            }
        }

        /// <summary>
        /// Starts the server process.
        /// </summary>
        private static IgniteProcess StartServerProcess(IgniteConfiguration cfg)
        {
            return new IgniteProcess(
                "-springConfigUrl=" + cfg.SpringConfigUrl, "-J-ea", "-J-Xcheck:jni", "-J-Xms512m", "-J-Xmx512m",
                "-J-DIGNITE_QUIET=false");
        }


        /// <summary>
        /// Test set up.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            Ignition.StopAll(true);
            IgniteProcess.KillAll();
        }

        /// <summary>
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
            IgniteProcess.KillAll();
            Ignition.ClientMode = false;
        }
    }
}

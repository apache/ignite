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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Client reconnect tests.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ReconnectTest
    {
        /// <summary>
        /// Tests the disconnected exception.
        /// </summary>
        [Test]
        public void TestDisconnectedException()
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

                var cache = ignite.CreateCache<int, int>("c");

                cache[1] = 1;

                // Suspend external process to cause disconnect
                proc.Suspend();

                var ex = Assert.Throws<CacheException>(() => cache.Get(1));

                Assert.IsTrue(ex.ToString().Contains(
                    "javax.cache.CacheException: class org.apache.ignite.IgniteClientDisconnectedException: " +
                    "Operation has been cancelled (client node disconnected)"));

                var inner = (ClientDisconnectedException) ex.InnerException;

                var clientReconnectTask = inner.ClientReconnectTask;

                Assert.AreEqual(ignite.GetCluster().ClientReconnectTask, clientReconnectTask);
                Assert.AreEqual(1, disconnected);
                Assert.AreEqual(0, reconnected);

                // Resume process to reconnect
                proc.Resume();

                clientReconnectTask.Wait();

                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(1, disconnected);
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
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            IgniteProcess.KillAll();
            Ignition.ClientMode = false;
        }
    }
}

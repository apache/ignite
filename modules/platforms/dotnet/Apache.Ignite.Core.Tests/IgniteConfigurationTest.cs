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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests code-based configuration.
    /// </summary>
    public class IgniteConfigurationTest
    {
        [Test]
        public void TestClientMode()
        {
            using (var ignite = Ignition.Start())
            using (var ignite2 = Ignition.Start(new IgniteConfiguration {GridName = "client", ClientMode = true}))
            {
                const string cacheName = "cache";

                ignite.CreateCache<int, int>(cacheName);

                Assert.AreEqual(2, ignite2.GetCluster().GetNodes().Count);
                Assert.AreEqual(1, ignite.GetCluster().ForCacheNodes(cacheName).GetNodes().Count);

            }
        }

        [Test]
        public void TestDefaultSpi()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoveryConfiguration =
                    new DiscoveryConfiguration
                    {
                        AckTimeout = TimeSpan.FromDays(2),
                        MaxAckTimeout = TimeSpan.MaxValue,
                        JoinTimeout = TimeSpan.MaxValue,
                        NetworkTimeout = TimeSpan.MaxValue,
                        SocketTimeout = TimeSpan.MaxValue
                    },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                cfg.GridName = "ignite2";
                using (var ignite2 = Ignition.Start(cfg))
                {
                    Assert.AreEqual(2, ignite.GetCluster().GetNodes().Count);
                    Assert.AreEqual(2, ignite2.GetCluster().GetNodes().Count);
                }
            }
        }

        [Test]
        public void TestInvalidTimeouts()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoveryConfiguration =
                    new DiscoveryConfiguration
                    {
                        AckTimeout = TimeSpan.FromMilliseconds(-5),
                        JoinTimeout = TimeSpan.MinValue,
                    },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
        }

        [Test]
        public void TestStaticIpFinder()
        {
            TestIpFinders(new StaticIpFinder
            {
                EndPoints = new[] {"127.0.0.1:47500"}
            }, new StaticIpFinder
            {
                EndPoints = new[] {"127.0.0.1:47501"}
            });
        }

        [Test]
        public void TestMulticastIpFinder()
        {
            TestIpFinders(
                new MulticastIpFinder {MulticastGroup = "228.111.111.222", MulticastPort = 54522},
                new MulticastIpFinder {MulticastGroup = "228.111.111.223", MulticastPort = 54522});
        }

        private static void TestIpFinders(IpFinder ipFinder, IpFinder ipFinder2)
        {
            var cfg = new IgniteConfiguration
            {
                DiscoveryConfiguration =
                    new DiscoveryConfiguration
                    {
                        IpFinder = ipFinder
                    },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                // Start with the same endpoint
                cfg.GridName = "ignite2";
                using (var ignite2 = Ignition.Start(cfg))
                {
                    Assert.AreEqual(2, ignite.GetCluster().GetNodes().Count);
                    Assert.AreEqual(2, ignite2.GetCluster().GetNodes().Count);
                }

                // Start with incompatible endpoint and check that there are 2 topologies
                cfg.DiscoveryConfiguration.IpFinder = ipFinder2;

                using (var ignite2 = Ignition.Start(cfg))
                {
                    Assert.AreEqual(1, ignite.GetCluster().GetNodes().Count);
                    Assert.AreEqual(1, ignite2.GetCluster().GetNodes().Count);
                }
            }
        }

    }
}

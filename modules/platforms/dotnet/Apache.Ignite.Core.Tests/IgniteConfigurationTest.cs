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
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Tests code-based configuration.
    /// </summary>
    public class IgniteConfigurationTest
    {
        [Test]
        public void TestAllConfigurationProperties()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoveryConfiguration = new DiscoveryConfiguration
                {
                    NetworkTimeout = TimeSpan.FromSeconds(1),
                    AckTimeout = TimeSpan.FromSeconds(2),
                    MaxAckTimeout = TimeSpan.FromSeconds(3),
                    SocketTimeout = TimeSpan.FromSeconds(4),
                    JoinTimeout = TimeSpan.FromSeconds(5),
                    IpFinder = new MulticastIpFinder
                    {
                        MulticastGroup = "228.111.111.222",
                        MulticastPort = 54522,
                        AddressRequestAttempts = 5,
                        EndPoints = new[] { "127.0.0.1:47500", "127.0.0.1:47501" },
                        TimeToLive = 25,
                        LocalAddress = "127.0.0.1",
                        ResponseTimeout = TimeSpan.FromSeconds(6)
                    }
                },
                GridName = "gridName1",
                IncludedEventTypes = EventType.SwapspaceAll,
                MetricsExpireTime = TimeSpan.FromMinutes(7),
                MetricsHistorySize = 125,
                MetricsLogFrequency = TimeSpan.FromMinutes(8),
                MetricsUpdateFrequency = TimeSpan.FromMinutes(9),
                NetworkSendRetryCount = 54,
                NetworkTimeout = TimeSpan.FromMinutes(10),
                NetworkSendRetryDelay = TimeSpan.FromMinutes(11),
                WorkDirectory = Path.GetTempPath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var resCfg = ignite.GetConfiguration();

                var disco = cfg.DiscoveryConfiguration;
                var resDisco = resCfg.DiscoveryConfiguration;

                Assert.AreEqual(disco.NetworkTimeout, resDisco.NetworkTimeout);
                Assert.AreEqual(disco.AckTimeout, resDisco.AckTimeout);
                Assert.AreEqual(disco.MaxAckTimeout, resDisco.MaxAckTimeout);
                Assert.AreEqual(disco.SocketTimeout, resDisco.SocketTimeout);
                Assert.AreEqual(disco.JoinTimeout, resDisco.JoinTimeout);

                var ip = (MulticastIpFinder) disco.IpFinder;
                var resIp = (MulticastIpFinder) resDisco.IpFinder;

                Assert.AreEqual(ip.MulticastGroup, resIp.MulticastGroup);
                Assert.AreEqual(ip.MulticastPort, resIp.MulticastPort);
                Assert.AreEqual(ip.AddressRequestAttempts, resIp.AddressRequestAttempts);

                // There can be extra IPv6 endpoints
                Assert.AreEqual(ip.EndPoints, resIp.EndPoints.Take(2).Select(x => x.Trim('/')).ToArray());

                Assert.AreEqual(ip.TimeToLive, resIp.TimeToLive);
                Assert.AreEqual(ip.LocalAddress, resIp.LocalAddress);
                Assert.AreEqual(ip.ResponseTimeout, resIp.ResponseTimeout);

                Assert.AreEqual(cfg.GridName, resCfg.GridName);
                Assert.AreEqual(cfg.IncludedEventTypes, resCfg.IncludedEventTypes);
                Assert.AreEqual(cfg.MetricsExpireTime, resCfg.MetricsExpireTime);
                Assert.AreEqual(cfg.MetricsHistorySize, resCfg.MetricsHistorySize);
                Assert.AreEqual(cfg.MetricsLogFrequency, resCfg.MetricsLogFrequency);
                Assert.AreEqual(cfg.MetricsUpdateFrequency, resCfg.MetricsUpdateFrequency);
                Assert.AreEqual(cfg.NetworkSendRetryCount, resCfg.NetworkSendRetryCount);
                Assert.AreEqual(cfg.NetworkTimeout, resCfg.NetworkTimeout);
                Assert.AreEqual(cfg.NetworkSendRetryDelay, resCfg.NetworkSendRetryDelay);
                Assert.AreEqual(cfg.WorkDirectory, resCfg.WorkDirectory);
                Assert.AreEqual(cfg.JvmClasspath, resCfg.JvmClasspath);
                Assert.AreEqual(cfg.JvmOptions, resCfg.JvmOptions);
                Assert.IsTrue(File.Exists(resCfg.JvmDllPath));
            }
        }

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

                Assert.AreEqual(false, ignite.GetConfiguration().ClientMode);
                Assert.AreEqual(true, ignite2.GetConfiguration().ClientMode);
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

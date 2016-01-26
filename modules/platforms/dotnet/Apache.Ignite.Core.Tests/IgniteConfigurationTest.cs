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

#pragma warning disable 618  // deprecated SpringConfigUrl
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Discovery;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Tests code-based configuration.
    /// </summary>
    public class IgniteConfigurationTest
    {
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void TestDefaultConfigurationProperties()
        {
            CheckDefaultProperties(new IgniteConfiguration());
        }

        [Test]
        public void TestDefaultValueAttributes()
        {
            CheckDefaultValueAttributes(new IgniteConfiguration());
            CheckDefaultValueAttributes(new TcpDiscoverySpi());
            CheckDefaultValueAttributes(new CacheConfiguration());
            CheckDefaultValueAttributes(new TcpDiscoveryMulticastIpFinder());
        }

        [Test]
        public void TestAllConfigurationProperties()
        {
            var cfg = new IgniteConfiguration(GetCustomConfig());

            using (var ignite = Ignition.Start(cfg))
            {
                var resCfg = ignite.GetConfiguration();

                var disco = (TcpDiscoverySpi) cfg.DiscoverySpi;
                var resDisco = (TcpDiscoverySpi) resCfg.DiscoverySpi;

                Assert.AreEqual(disco.NetworkTimeout, resDisco.NetworkTimeout);
                Assert.AreEqual(disco.AckTimeout, resDisco.AckTimeout);
                Assert.AreEqual(disco.MaxAckTimeout, resDisco.MaxAckTimeout);
                Assert.AreEqual(disco.SocketTimeout, resDisco.SocketTimeout);
                Assert.AreEqual(disco.JoinTimeout, resDisco.JoinTimeout);

                var ip = (TcpDiscoveryStaticIpFinder) disco.IpFinder;
                var resIp = (TcpDiscoveryStaticIpFinder) resDisco.IpFinder;

                // There can be extra IPv6 endpoints
                Assert.AreEqual(ip.EndPoints, resIp.EndPoints.Take(2).Select(x => x.Trim('/')).ToArray());

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
                Assert.AreEqual(cfg.LocalHost, resCfg.LocalHost);
            }
        }

        [Test]
        public void TestSpringXml()
        {
            // When Spring XML is used, all properties are ignored.
            var cfg = GetCustomConfig();

            cfg.SpringConfigUrl = "config\\marshaller-default.xml";

            using (var ignite = Ignition.Start(cfg))
            {
                var resCfg = ignite.GetConfiguration();

                CheckDefaultProperties(resCfg);
            }
        }

        [Test]
        public void TestClientMode()
        {
            using (var ignite = Ignition.Start(new IgniteConfiguration {LocalHost = "127.0.0.1"}))
            using (var ignite2 = Ignition.Start(new IgniteConfiguration
            {
                GridName = "client",
                ClientMode = true,
                LocalHost = "127.0.0.1"
            }))
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
                DiscoverySpi =
                    new TcpDiscoverySpi
                    {
                        AckTimeout = TimeSpan.FromDays(2),
                        MaxAckTimeout = TimeSpan.MaxValue,
                        JoinTimeout = TimeSpan.MaxValue,
                        NetworkTimeout = TimeSpan.MaxValue,
                        SocketTimeout = TimeSpan.MaxValue
                    },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                LocalHost = "127.0.0.1"
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
                DiscoverySpi =
                    new TcpDiscoverySpi
                    {
                        AckTimeout = TimeSpan.FromMilliseconds(-5),
                        JoinTimeout = TimeSpan.MinValue,
                    },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
            };

            Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
        }

        [Test]
        public void TestStaticIpFinder()
        {
            TestIpFinders(new TcpDiscoveryStaticIpFinder
            {
                EndPoints = new[] {"127.0.0.1:47500"}
            }, new TcpDiscoveryStaticIpFinder
            {
                EndPoints = new[] {"127.0.0.1:47501"}
            });
        }

        [Test]
        public void TestMulticastIpFinder()
        {
            TestIpFinders(
                new TcpDiscoveryMulticastIpFinder {MulticastGroup = "228.111.111.222", MulticastPort = 54522},
                new TcpDiscoveryMulticastIpFinder {MulticastGroup = "228.111.111.223", MulticastPort = 54522});
        }

        private static void TestIpFinders(TcpDiscoveryIpFinder ipFinder, TcpDiscoveryIpFinder ipFinder2)
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi =
                    new TcpDiscoverySpi
                    {
                        IpFinder = ipFinder
                    },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                LocalHost = "127.0.0.1"
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
                ((TcpDiscoverySpi) cfg.DiscoverySpi).IpFinder = ipFinder2;

                using (var ignite2 = Ignition.Start(cfg))
                {
                    Assert.AreEqual(1, ignite.GetCluster().GetNodes().Count);
                    Assert.AreEqual(1, ignite2.GetCluster().GetNodes().Count);
                }
            }
        }

        private static void CheckDefaultProperties(IgniteConfiguration cfg)
        {
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsExpireTime, cfg.MetricsExpireTime);
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsHistorySize, cfg.MetricsHistorySize);
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsLogFrequency, cfg.MetricsLogFrequency);
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsUpdateFrequency, cfg.MetricsUpdateFrequency);
            Assert.AreEqual(IgniteConfiguration.DefaultNetworkTimeout, cfg.NetworkTimeout);
            Assert.AreEqual(IgniteConfiguration.DefaultNetworkSendRetryCount, cfg.NetworkSendRetryCount);
            Assert.AreEqual(IgniteConfiguration.DefaultNetworkSendRetryDelay, cfg.NetworkSendRetryDelay);
        }

        private static void CheckDefaultValueAttributes(object obj)
        {
            var props = obj.GetType().GetProperties();

            foreach (var prop in props)
            {
                var attr = prop.GetCustomAttributes(true).OfType<DefaultValueAttribute>().FirstOrDefault();
                var propValue = prop.GetValue(obj, null);

                if (attr != null)
                    Assert.AreEqual(attr.Value, propValue);
                else if (prop.PropertyType.IsValueType)
                    Assert.AreEqual(Activator.CreateInstance(prop.PropertyType), propValue);
                else
                    Assert.IsNull(propValue);
            }
        }

        private static IgniteConfiguration GetCustomConfig()
        {
            return new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    NetworkTimeout = TimeSpan.FromSeconds(1),
                    AckTimeout = TimeSpan.FromSeconds(2),
                    MaxAckTimeout = TimeSpan.FromSeconds(3),
                    SocketTimeout = TimeSpan.FromSeconds(4),
                    JoinTimeout = TimeSpan.FromSeconds(5),
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        EndPoints = new[] { "127.0.0.1:47500", "127.0.0.1:47501" }
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
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath(),
                LocalHost = "127.0.0.1"
            };
        }
    }
}

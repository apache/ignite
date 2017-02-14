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
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.SwapSpace.File;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests code-based configuration.
    /// </summary>
    public class IgniteConfigurationTest
    {
        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the default configuration properties.
        /// </summary>
        [Test]
        public void TestDefaultConfigurationProperties()
        {
            CheckDefaultProperties(new IgniteConfiguration());
        }

        /// <summary>
        /// Tests the default value attributes.
        /// </summary>
        [Test]
        public void TestDefaultValueAttributes()
        {
            CheckDefaultValueAttributes(new IgniteConfiguration());
            CheckDefaultValueAttributes(new BinaryConfiguration());
            CheckDefaultValueAttributes(new TcpDiscoverySpi());
            CheckDefaultValueAttributes(new CacheConfiguration());
            CheckDefaultValueAttributes(new TcpDiscoveryMulticastIpFinder());
            CheckDefaultValueAttributes(new TcpCommunicationSpi());
            CheckDefaultValueAttributes(new RendezvousAffinityFunction());
            CheckDefaultValueAttributes(new FairAffinityFunction());
            CheckDefaultValueAttributes(new NearCacheConfiguration());
            CheckDefaultValueAttributes(new FifoEvictionPolicy());
            CheckDefaultValueAttributes(new LruEvictionPolicy());
            CheckDefaultValueAttributes(new AtomicConfiguration());
            CheckDefaultValueAttributes(new TransactionConfiguration());
            CheckDefaultValueAttributes(new FileSwapSpaceSpi());
        }

        /// <summary>
        /// Tests all configuration properties.
        /// </summary>
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

                Assert.AreEqual(disco.LocalAddress, resDisco.LocalAddress);
                Assert.AreEqual(disco.LocalPort, resDisco.LocalPort);
                Assert.AreEqual(disco.LocalPortRange, resDisco.LocalPortRange);
                Assert.AreEqual(disco.MaxMissedClientHeartbeats, resDisco.MaxMissedClientHeartbeats);
                Assert.AreEqual(disco.MaxMissedHeartbeats, resDisco.MaxMissedHeartbeats);
                Assert.AreEqual(disco.ReconnectCount, resDisco.ReconnectCount);
                Assert.AreEqual(disco.StatisticsPrintFrequency, resDisco.StatisticsPrintFrequency);
                Assert.AreEqual(disco.ThreadPriority, resDisco.ThreadPriority);
                Assert.AreEqual(disco.TopologyHistorySize, resDisco.TopologyHistorySize);

                var ip = (TcpDiscoveryStaticIpFinder) disco.IpFinder;
                var resIp = (TcpDiscoveryStaticIpFinder) resDisco.IpFinder;

                // There can be extra IPv6 endpoints
                Assert.AreEqual(ip.Endpoints, resIp.Endpoints.Take(2).Select(x => x.Trim('/')).ToArray());

                Assert.AreEqual(cfg.GridName, resCfg.GridName);
                Assert.AreEqual(cfg.IncludedEventTypes, resCfg.IncludedEventTypes);
                Assert.AreEqual(cfg.MetricsExpireTime, resCfg.MetricsExpireTime);
                Assert.AreEqual(cfg.MetricsHistorySize, resCfg.MetricsHistorySize);
                Assert.AreEqual(cfg.MetricsLogFrequency, resCfg.MetricsLogFrequency);
                Assert.AreEqual(cfg.MetricsUpdateFrequency, resCfg.MetricsUpdateFrequency);
                Assert.AreEqual(cfg.NetworkSendRetryCount, resCfg.NetworkSendRetryCount);
                Assert.AreEqual(cfg.NetworkTimeout, resCfg.NetworkTimeout);
                Assert.AreEqual(cfg.NetworkSendRetryDelay, resCfg.NetworkSendRetryDelay);
                Assert.AreEqual(cfg.WorkDirectory.Trim('\\'), resCfg.WorkDirectory.Trim('\\'));
                Assert.AreEqual(cfg.JvmClasspath, resCfg.JvmClasspath);
                Assert.AreEqual(cfg.JvmOptions, resCfg.JvmOptions);
                Assert.IsTrue(File.Exists(resCfg.JvmDllPath));
                Assert.AreEqual(cfg.Localhost, resCfg.Localhost);
                Assert.AreEqual(cfg.IsDaemon, resCfg.IsDaemon);
                Assert.AreEqual(cfg.IsLateAffinityAssignment, resCfg.IsLateAffinityAssignment);
                Assert.AreEqual(cfg.UserAttributes, resCfg.UserAttributes);

                var atm = cfg.AtomicConfiguration;
                var resAtm = resCfg.AtomicConfiguration;
                Assert.AreEqual(atm.AtomicSequenceReserveSize, resAtm.AtomicSequenceReserveSize);
                Assert.AreEqual(atm.Backups, resAtm.Backups);
                Assert.AreEqual(atm.CacheMode, resAtm.CacheMode);

                var tx = cfg.TransactionConfiguration;
                var resTx = resCfg.TransactionConfiguration;
                Assert.AreEqual(tx.DefaultTimeout, resTx.DefaultTimeout);
                Assert.AreEqual(tx.DefaultTransactionConcurrency, resTx.DefaultTransactionConcurrency);
                Assert.AreEqual(tx.DefaultTransactionIsolation, resTx.DefaultTransactionIsolation);
                Assert.AreEqual(tx.PessimisticTransactionLogLinger, resTx.PessimisticTransactionLogLinger);
                Assert.AreEqual(tx.PessimisticTransactionLogSize, resTx.PessimisticTransactionLogSize);

                var com = (TcpCommunicationSpi) cfg.CommunicationSpi;
                var resCom = (TcpCommunicationSpi) resCfg.CommunicationSpi;
                Assert.AreEqual(com.AckSendThreshold, resCom.AckSendThreshold);
                Assert.AreEqual(com.ConnectTimeout, resCom.ConnectTimeout);
                Assert.AreEqual(com.DirectBuffer, resCom.DirectBuffer);
                Assert.AreEqual(com.DirectSendBuffer, resCom.DirectSendBuffer);
                Assert.AreEqual(com.IdleConnectionTimeout, resCom.IdleConnectionTimeout);
                Assert.AreEqual(com.LocalAddress, resCom.LocalAddress);
                Assert.AreEqual(com.LocalPort, resCom.LocalPort);
                Assert.AreEqual(com.LocalPortRange, resCom.LocalPortRange);
                Assert.AreEqual(com.MaxConnectTimeout, resCom.MaxConnectTimeout);
                Assert.AreEqual(com.MessageQueueLimit, resCom.MessageQueueLimit);
                Assert.AreEqual(com.ReconnectCount, resCom.ReconnectCount);
                Assert.AreEqual(com.SelectorsCount, resCom.SelectorsCount);
                Assert.AreEqual(com.SlowClientQueueLimit, resCom.SlowClientQueueLimit);
                Assert.AreEqual(com.SocketReceiveBufferSize, resCom.SocketReceiveBufferSize);
                Assert.AreEqual(com.SocketSendBufferSize, resCom.SocketSendBufferSize);
                Assert.AreEqual(com.TcpNoDelay, resCom.TcpNoDelay);
                Assert.AreEqual(com.UnacknowledgedMessagesBufferSize, resCom.UnacknowledgedMessagesBufferSize);

                Assert.AreEqual(cfg.FailureDetectionTimeout, resCfg.FailureDetectionTimeout);

                var swap = (FileSwapSpaceSpi) cfg.SwapSpaceSpi;
                var resSwap = (FileSwapSpaceSpi) resCfg.SwapSpaceSpi;
                Assert.AreEqual(swap.MaximumSparsity, resSwap.MaximumSparsity);
                Assert.AreEqual(swap.BaseDirectory, resSwap.BaseDirectory);
                Assert.AreEqual(swap.MaximumWriteQueueSize, resSwap.MaximumWriteQueueSize);
                Assert.AreEqual(swap.ReadStripesNumber, resSwap.ReadStripesNumber);
                Assert.AreEqual(swap.WriteBufferSize, resSwap.WriteBufferSize);

                var binCfg = cfg.BinaryConfiguration;
                Assert.IsFalse(binCfg.CompactFooter);

                var typ = binCfg.TypeConfigurations.Single();
                Assert.AreEqual("myType", typ.TypeName);
                Assert.IsTrue(typ.IsEnum);
                Assert.AreEqual("affKey", typ.AffinityKeyFieldName);
                Assert.AreEqual(false, typ.KeepDeserialized);

                CollectionAssert.AreEqual(new[] {"fld1", "fld2"}, 
                    ((BinaryFieldEqualityComparer)typ.EqualityComparer).FieldNames);
            }
        }

        /// <summary>
        /// Tests the spring XML.
        /// </summary>
        [Test]
        public void TestSpringXml()
        {
            // When Spring XML is used, .NET overrides Spring.
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"config\spring-test.xml",
                NetworkSendRetryDelay = TimeSpan.FromSeconds(45),
                MetricsHistorySize = 57
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var resCfg = ignite.GetConfiguration();

                Assert.AreEqual(45, resCfg.NetworkSendRetryDelay.TotalSeconds);  // .NET overrides XML
                Assert.AreEqual(2999, resCfg.NetworkTimeout.TotalMilliseconds);  // Not set in .NET -> comes from XML
                Assert.AreEqual(57, resCfg.MetricsHistorySize);  // Only set in .NET

                var disco = resCfg.DiscoverySpi as TcpDiscoverySpi;
                Assert.IsNotNull(disco);
                Assert.AreEqual(TimeSpan.FromMilliseconds(300), disco.SocketTimeout);
            }
        }

        /// <summary>
        /// Tests the client mode.
        /// </summary>
        [Test]
        public void TestClientMode()
        {
            using (var ignite = Ignition.Start(new IgniteConfiguration
            {
                Localhost = "127.0.0.1",
                DiscoverySpi = TestUtils.GetStaticDiscovery()
            }))
            using (var ignite2 = Ignition.Start(new IgniteConfiguration
            {
                Localhost = "127.0.0.1",
                DiscoverySpi = TestUtils.GetStaticDiscovery(),
                GridName = "client",
                ClientMode = true
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

        /// <summary>
        /// Tests the default spi.
        /// </summary>
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
                Localhost = "127.0.0.1"
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

        /// <summary>
        /// Tests the invalid timeouts.
        /// </summary>
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

        /// <summary>
        /// Tests the static ip finder.
        /// </summary>
        [Test]
        public void TestStaticIpFinder()
        {
            TestIpFinders(new TcpDiscoveryStaticIpFinder
            {
                Endpoints = new[] {"127.0.0.1:47500"}
            }, new TcpDiscoveryStaticIpFinder
            {
                Endpoints = new[] {"127.0.0.1:47501"}
            });
        }

        /// <summary>
        /// Tests the multicast ip finder.
        /// </summary>
        [Test]
        public void TestMulticastIpFinder()
        {
            TestIpFinders(
                new TcpDiscoveryMulticastIpFinder {MulticastGroup = "228.111.111.222", MulticastPort = 54522},
                new TcpDiscoveryMulticastIpFinder {MulticastGroup = "228.111.111.223", MulticastPort = 54522});
        }

        /// <summary>
        /// Tests the work directory.
        /// </summary>
        [Test]
        public void TestWorkDirectory()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                WorkDirectory = IgniteUtils.GetTempDirectoryName()
            };

            using (Ignition.Start(cfg))
            {
                var marshDir = Path.Combine(cfg.WorkDirectory, "marshaller");

                Assert.IsTrue(Directory.Exists(marshDir));
            }

            Directory.Delete(cfg.WorkDirectory, true);
        }

        /// <summary>
        /// Tests the ip finders.
        /// </summary>
        /// <param name="ipFinder">The ip finder.</param>
        /// <param name="ipFinder2">The ip finder2.</param>
        private static void TestIpFinders(TcpDiscoveryIpFinderBase ipFinder, TcpDiscoveryIpFinderBase ipFinder2)
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
                Localhost = "127.0.0.1"
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

        /// <summary>
        /// Checks the default properties.
        /// </summary>
        /// <param name="cfg">The CFG.</param>
        private static void CheckDefaultProperties(IgniteConfiguration cfg)
        {
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsExpireTime, cfg.MetricsExpireTime);
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsHistorySize, cfg.MetricsHistorySize);
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsLogFrequency, cfg.MetricsLogFrequency);
            Assert.AreEqual(IgniteConfiguration.DefaultMetricsUpdateFrequency, cfg.MetricsUpdateFrequency);
            Assert.AreEqual(IgniteConfiguration.DefaultNetworkTimeout, cfg.NetworkTimeout);
            Assert.AreEqual(IgniteConfiguration.DefaultNetworkSendRetryCount, cfg.NetworkSendRetryCount);
            Assert.AreEqual(IgniteConfiguration.DefaultNetworkSendRetryDelay, cfg.NetworkSendRetryDelay);
            Assert.AreEqual(IgniteConfiguration.DefaultFailureDetectionTimeout, cfg.FailureDetectionTimeout);
        }

        /// <summary>
        /// Checks the default value attributes.
        /// </summary>
        /// <param name="obj">The object.</param>
        private static void CheckDefaultValueAttributes(object obj)
        {
            var props = obj.GetType().GetProperties();

            foreach (var prop in props.Where(p => p.Name != "SelectorsCount" && p.Name != "ReadStripesNumber"))
            {
                var attr = prop.GetCustomAttributes(true).OfType<DefaultValueAttribute>().FirstOrDefault();
                var propValue = prop.GetValue(obj, null);

                if (attr != null)
                    Assert.AreEqual(attr.Value, propValue, string.Format("{0}.{1}", obj.GetType(), prop.Name));
                else if (prop.PropertyType.IsValueType)
                    Assert.AreEqual(Activator.CreateInstance(prop.PropertyType), propValue);
                else
                    Assert.IsNull(propValue);
            }
        }

        /// <summary>
        /// Gets the custom configuration.
        /// </summary>
        private static IgniteConfiguration GetCustomConfig()
        {
            // CacheConfiguration is not tested here - see CacheConfigurationTest
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
                        Endpoints = new[] { "127.0.0.1:49900", "127.0.0.1:49901" }
                    },
                    ClientReconnectDisabled = true,
                    ForceServerMode = true,
                    HeartbeatFrequency = TimeSpan.FromSeconds(3),
                    IpFinderCleanFrequency = TimeSpan.FromMinutes(7),
                    LocalAddress = "127.0.0.1",
                    LocalPort = 49900,
                    LocalPortRange = 13,
                    MaxMissedClientHeartbeats = 9,
                    MaxMissedHeartbeats = 7,
                    ReconnectCount = 11,
                    StatisticsPrintFrequency = TimeSpan.FromSeconds(20),
                    ThreadPriority = 6,
                    TopologyHistorySize = 1234567
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
                Localhost = "127.0.0.1",
                IsDaemon = true,
                IsLateAffinityAssignment = false,
                UserAttributes = Enumerable.Range(1, 10).ToDictionary(x => x.ToString(), x => (object) x),
                AtomicConfiguration = new AtomicConfiguration
                {
                    CacheMode = CacheMode.Replicated,
                    Backups = 2,
                    AtomicSequenceReserveSize = 200
                },
                TransactionConfiguration = new TransactionConfiguration
                {
                    DefaultTransactionConcurrency = TransactionConcurrency.Optimistic,
                    DefaultTimeout = TimeSpan.FromSeconds(25),
                    DefaultTransactionIsolation = TransactionIsolation.Serializable,
                    PessimisticTransactionLogLinger = TimeSpan.FromHours(1),
                    PessimisticTransactionLogSize = 240
                },
                CommunicationSpi = new TcpCommunicationSpi
                {
                    LocalPort = 47501,
                    MaxConnectTimeout = TimeSpan.FromSeconds(34),
                    MessageQueueLimit = 15,
                    ConnectTimeout = TimeSpan.FromSeconds(17),
                    IdleConnectionTimeout = TimeSpan.FromSeconds(19),
                    SelectorsCount = 8,
                    ReconnectCount = 33,
                    SocketReceiveBufferSize = 512,
                    AckSendThreshold = 99,
                    DirectBuffer = false,
                    DirectSendBuffer = true,
                    LocalPortRange = 45,
                    LocalAddress = "127.0.0.1",
                    TcpNoDelay = false,
                    SlowClientQueueLimit = 98,
                    SocketSendBufferSize = 2045,
                    UnacknowledgedMessagesBufferSize = 3450
                },
                FailureDetectionTimeout = TimeSpan.FromSeconds(3.5),
                SwapSpaceSpi = new FileSwapSpaceSpi
                {
                    ReadStripesNumber = 64,
                    MaximumWriteQueueSize = 8,
                    WriteBufferSize = 9,
                    BaseDirectory = Path.GetTempPath(),
                    MaximumSparsity = 11.22f
                },
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = false,
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration
                        {
                            TypeName = "myType",
                            IsEnum = true,
                            AffinityKeyFieldName = "affKey",
                            KeepDeserialized = false,
                            EqualityComparer = new BinaryFieldEqualityComparer("fld1", "fld2")
                        }
                    }
                }
            };
        }
    }
}

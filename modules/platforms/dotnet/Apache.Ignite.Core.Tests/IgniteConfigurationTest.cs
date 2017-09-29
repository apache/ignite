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

#pragma warning disable 618  // Ignore obsolete, we still need to test them.
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Tests.Plugin;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests code-based configuration.
    /// </summary>
    public class IgniteConfigurationTest
    {
        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureTearDown()
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
            CheckDefaultProperties(new PersistentStoreConfiguration());
            CheckDefaultProperties(new ClientConnectorConfiguration());
            CheckDefaultProperties(new SqlConnectorConfiguration());
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
            CheckDefaultValueAttributes(new NearCacheConfiguration());
            CheckDefaultValueAttributes(new FifoEvictionPolicy());
            CheckDefaultValueAttributes(new LruEvictionPolicy());
            CheckDefaultValueAttributes(new AtomicConfiguration());
            CheckDefaultValueAttributes(new TransactionConfiguration());
            CheckDefaultValueAttributes(new MemoryEventStorageSpi());
            CheckDefaultValueAttributes(new MemoryConfiguration());
            CheckDefaultValueAttributes(new MemoryPolicyConfiguration());
            CheckDefaultValueAttributes(new SqlConnectorConfiguration());
            CheckDefaultValueAttributes(new ClientConnectorConfiguration());
            CheckDefaultValueAttributes(new PersistentStoreConfiguration());
            CheckDefaultValueAttributes(new IgniteClientConfiguration());
            CheckDefaultValueAttributes(new QueryIndex());
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
                Assert.AreEqual(disco.ReconnectCount, resDisco.ReconnectCount);
                Assert.AreEqual(disco.StatisticsPrintFrequency, resDisco.StatisticsPrintFrequency);
                Assert.AreEqual(disco.ThreadPriority, resDisco.ThreadPriority);
                Assert.AreEqual(disco.TopologyHistorySize, resDisco.TopologyHistorySize);

                var ip = (TcpDiscoveryStaticIpFinder) disco.IpFinder;
                var resIp = (TcpDiscoveryStaticIpFinder) resDisco.IpFinder;

                // There can be extra IPv6 endpoints
                Assert.AreEqual(ip.Endpoints, resIp.Endpoints.Take(2).Select(x => x.Trim('/')).ToArray());

                Assert.AreEqual(cfg.IgniteInstanceName, resCfg.IgniteInstanceName);
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
                Assert.AreEqual(IgniteConfiguration.DefaultIsLateAffinityAssignment, resCfg.IsLateAffinityAssignment);
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
                Assert.AreEqual(cfg.ClientFailureDetectionTimeout, resCfg.ClientFailureDetectionTimeout);
                Assert.AreEqual(cfg.LongQueryWarningTimeout, resCfg.LongQueryWarningTimeout);

                Assert.AreEqual(cfg.PublicThreadPoolSize, resCfg.PublicThreadPoolSize);
                Assert.AreEqual(cfg.StripedThreadPoolSize, resCfg.StripedThreadPoolSize);
                Assert.AreEqual(cfg.ServiceThreadPoolSize, resCfg.ServiceThreadPoolSize);
                Assert.AreEqual(cfg.SystemThreadPoolSize, resCfg.SystemThreadPoolSize);
                Assert.AreEqual(cfg.AsyncCallbackThreadPoolSize, resCfg.AsyncCallbackThreadPoolSize);
                Assert.AreEqual(cfg.ManagementThreadPoolSize, resCfg.ManagementThreadPoolSize);
                Assert.AreEqual(cfg.DataStreamerThreadPoolSize, resCfg.DataStreamerThreadPoolSize);
                Assert.AreEqual(cfg.UtilityCacheThreadPoolSize, resCfg.UtilityCacheThreadPoolSize);
                Assert.AreEqual(cfg.QueryThreadPoolSize, resCfg.QueryThreadPoolSize);

                Assert.AreEqual(cfg.ConsistentId, resCfg.ConsistentId);

                var binCfg = cfg.BinaryConfiguration;
                Assert.IsFalse(binCfg.CompactFooter);

                var typ = binCfg.TypeConfigurations.Single();
                Assert.AreEqual("myType", typ.TypeName);
                Assert.IsTrue(typ.IsEnum);
                Assert.AreEqual("affKey", typ.AffinityKeyFieldName);
                Assert.AreEqual(false, typ.KeepDeserialized);

                Assert.IsNotNull(resCfg.PluginConfigurations);
                Assert.AreEqual(cfg.PluginConfigurations, resCfg.PluginConfigurations);

                var eventCfg = cfg.EventStorageSpi as MemoryEventStorageSpi;
                var resEventCfg = resCfg.EventStorageSpi as MemoryEventStorageSpi;
                Assert.IsNotNull(eventCfg);
                Assert.IsNotNull(resEventCfg);
                Assert.AreEqual(eventCfg.ExpirationTimeout, resEventCfg.ExpirationTimeout);
                Assert.AreEqual(eventCfg.MaxEventCount, resEventCfg.MaxEventCount);

                var memCfg = cfg.MemoryConfiguration;
                var resMemCfg = resCfg.MemoryConfiguration;
                Assert.IsNotNull(memCfg);
                Assert.IsNotNull(resMemCfg);
                Assert.AreEqual(memCfg.PageSize, resMemCfg.PageSize);
                Assert.AreEqual(memCfg.ConcurrencyLevel, resMemCfg.ConcurrencyLevel);
                Assert.AreEqual(memCfg.DefaultMemoryPolicyName, resMemCfg.DefaultMemoryPolicyName);
                Assert.AreEqual(memCfg.SystemCacheInitialSize, resMemCfg.SystemCacheInitialSize);
                Assert.AreEqual(memCfg.SystemCacheMaxSize, resMemCfg.SystemCacheMaxSize);
                Assert.IsNotNull(memCfg.MemoryPolicies);
                Assert.IsNotNull(resMemCfg.MemoryPolicies);
                Assert.AreEqual(2, memCfg.MemoryPolicies.Count);
                Assert.AreEqual(2, resMemCfg.MemoryPolicies.Count);

                for (var i = 0; i < memCfg.MemoryPolicies.Count; i++)
                {
                    var plc = memCfg.MemoryPolicies.Skip(i).First();
                    var resPlc = resMemCfg.MemoryPolicies.Skip(i).First();

                    Assert.AreEqual(plc.PageEvictionMode, resPlc.PageEvictionMode);
                    Assert.AreEqual(plc.MaxSize, resPlc.MaxSize);
                    Assert.AreEqual(plc.EmptyPagesPoolSize, resPlc.EmptyPagesPoolSize);
                    Assert.AreEqual(plc.EvictionThreshold, resPlc.EvictionThreshold);
                    Assert.AreEqual(plc.Name, resPlc.Name);
                    Assert.AreEqual(plc.SwapFilePath, resPlc.SwapFilePath);
                }

                var sql = cfg.SqlConnectorConfiguration;
                var resSql = resCfg.SqlConnectorConfiguration;

                Assert.AreEqual(sql.Host, resSql.Host);
                Assert.AreEqual(sql.Port, resSql.Port);
                Assert.AreEqual(sql.PortRange, resSql.PortRange);
                Assert.AreEqual(sql.MaxOpenCursorsPerConnection, resSql.MaxOpenCursorsPerConnection);
                Assert.AreEqual(sql.SocketReceiveBufferSize, resSql.SocketReceiveBufferSize);
                Assert.AreEqual(sql.SocketSendBufferSize, resSql.SocketSendBufferSize);
                Assert.AreEqual(sql.TcpNoDelay, resSql.TcpNoDelay);
                Assert.AreEqual(sql.ThreadPoolSize, resSql.ThreadPoolSize);

                var pers = cfg.PersistentStoreConfiguration;
                var resPers = resCfg.PersistentStoreConfiguration;

                Assert.AreEqual(pers.AlwaysWriteFullPages, resPers.AlwaysWriteFullPages);
                Assert.AreEqual(pers.CheckpointingFrequency, resPers.CheckpointingFrequency);
                Assert.AreEqual(pers.CheckpointingPageBufferSize, resPers.CheckpointingPageBufferSize);
                Assert.AreEqual(pers.CheckpointingThreads, resPers.CheckpointingThreads);
                Assert.AreEqual(pers.LockWaitTime, resPers.LockWaitTime);
                Assert.AreEqual(pers.PersistentStorePath, resPers.PersistentStorePath);
                Assert.AreEqual(pers.TlbSize, resPers.TlbSize);
                Assert.AreEqual(pers.WalArchivePath, resPers.WalArchivePath);
                Assert.AreEqual(pers.WalFlushFrequency, resPers.WalFlushFrequency);
                Assert.AreEqual(pers.WalFsyncDelayNanos, resPers.WalFsyncDelayNanos);
                Assert.AreEqual(pers.WalHistorySize, resPers.WalHistorySize);
                Assert.AreEqual(pers.WalMode, resPers.WalMode);
                Assert.AreEqual(pers.WalRecordIteratorBufferSize, resPers.WalRecordIteratorBufferSize);
                Assert.AreEqual(pers.WalSegments, resPers.WalSegments);
                Assert.AreEqual(pers.WalSegmentSize, resPers.WalSegmentSize);
                Assert.AreEqual(pers.WalStorePath, resPers.WalStorePath);
                Assert.AreEqual(pers.MetricsEnabled, resPers.MetricsEnabled);
                Assert.AreEqual(pers.RateTimeInterval, resPers.RateTimeInterval);
                Assert.AreEqual(pers.SubIntervals, resPers.SubIntervals);
                Assert.AreEqual(pers.CheckpointWriteOrder, resPers.CheckpointWriteOrder);
                Assert.AreEqual(pers.WriteThrottlingEnabled, resPers.WriteThrottlingEnabled);
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

                // Check memory configuration defaults.
                var mem = resCfg.MemoryConfiguration;

                Assert.IsNotNull(mem);
                Assert.AreEqual("dfltPlc", mem.DefaultMemoryPolicyName);
                Assert.AreEqual(MemoryConfiguration.DefaultPageSize, mem.PageSize);
                Assert.AreEqual(MemoryConfiguration.DefaultSystemCacheInitialSize, mem.SystemCacheInitialSize);
                Assert.AreEqual(MemoryConfiguration.DefaultSystemCacheMaxSize, mem.SystemCacheMaxSize);

                var plc = mem.MemoryPolicies.Single();
                Assert.AreEqual("dfltPlc", plc.Name);
                Assert.AreEqual(MemoryPolicyConfiguration.DefaultEmptyPagesPoolSize, plc.EmptyPagesPoolSize);
                Assert.AreEqual(MemoryPolicyConfiguration.DefaultEvictionThreshold, plc.EvictionThreshold);
                Assert.AreEqual(MemoryPolicyConfiguration.DefaultInitialSize, plc.InitialSize);
                Assert.AreEqual(MemoryPolicyConfiguration.DefaultMaxSize, plc.MaxSize);
                Assert.AreEqual(MemoryPolicyConfiguration.DefaultSubIntervals, plc.SubIntervals);
                Assert.AreEqual(MemoryPolicyConfiguration.DefaultRateTimeInterval, plc.RateTimeInterval);

                // Check PersistentStoreConfiguration defaults.
                CheckDefaultProperties(resCfg.PersistentStoreConfiguration);

                // Connector defaults.
                CheckDefaultProperties(resCfg.ClientConnectorConfiguration);
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
                IgniteInstanceName = "client",
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
                cfg.IgniteInstanceName = "ignite2";
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
        /// Tests the consistent id.
        /// </summary>
        [Test]
        [NUnit.Framework.Category(TestUtils.CategoryIntensive)]
        public void TestConsistentId()
        {
            var ids = new object[]
            {
                null, new MyConsistentId {Data = "foo"}, "str", 1, 1.1, DateTime.Now, Guid.NewGuid()
            };

            var cfg = TestUtils.GetTestConfiguration();

            foreach (var id in ids)
            {
                cfg.ConsistentId = id;

                using (var ignite = Ignition.Start(cfg))
                {
                    Assert.AreEqual(id, ignite.GetConfiguration().ConsistentId);
                }
            }
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
                cfg.IgniteInstanceName = "ignite2";
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
            Assert.AreEqual(IgniteConfiguration.DefaultClientFailureDetectionTimeout,
                cfg.ClientFailureDetectionTimeout);
            Assert.AreEqual(IgniteConfiguration.DefaultLongQueryWarningTimeout, cfg.LongQueryWarningTimeout);
            Assert.AreEqual(IgniteConfiguration.DefaultIsLateAffinityAssignment, cfg.IsLateAffinityAssignment);
            Assert.AreEqual(IgniteConfiguration.DefaultIsActiveOnStart, cfg.IsActiveOnStart);
            Assert.AreEqual(IgniteConfiguration.DefaultClientConnectorConfigurationEnabled, 
                cfg.ClientConnectorConfigurationEnabled);

            // Thread pools.
            Assert.AreEqual(IgniteConfiguration.DefaultManagementThreadPoolSize, cfg.ManagementThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.PublicThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.StripedThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.ServiceThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.SystemThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.AsyncCallbackThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.DataStreamerThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.UtilityCacheThreadPoolSize);
            Assert.AreEqual(IgniteConfiguration.DefaultThreadPoolSize, cfg.QueryThreadPoolSize);
        }

        /// <summary>
        /// Checks the default properties.
        /// </summary>
        /// <param name="cfg">Config.</param>
        private static void CheckDefaultProperties(PersistentStoreConfiguration cfg)
        {
            Assert.AreEqual(PersistentStoreConfiguration.DefaultTlbSize, cfg.TlbSize);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultCheckpointingFrequency, cfg.CheckpointingFrequency);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultCheckpointingThreads, cfg.CheckpointingThreads);
            Assert.AreEqual(default(long), cfg.CheckpointingPageBufferSize);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultLockWaitTime, cfg.LockWaitTime);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalFlushFrequency, cfg.WalFlushFrequency);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalFsyncDelayNanos, cfg.WalFsyncDelayNanos);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalHistorySize, cfg.WalHistorySize);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalRecordIteratorBufferSize,
                cfg.WalRecordIteratorBufferSize);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalSegmentSize, cfg.WalSegmentSize);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalSegments, cfg.WalSegments);
            Assert.AreEqual(WalMode.Default, cfg.WalMode);
            Assert.IsFalse(cfg.MetricsEnabled);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultSubIntervals, cfg.SubIntervals);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultRateTimeInterval, cfg.RateTimeInterval);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalStorePath, cfg.WalStorePath);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWalArchivePath, cfg.WalArchivePath);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultCheckpointWriteOrder, cfg.CheckpointWriteOrder);
            Assert.AreEqual(PersistentStoreConfiguration.DefaultWriteThrottlingEnabled, cfg.WriteThrottlingEnabled);
        }

        /// <summary>
        /// Checks the default properties.
        /// </summary>
        /// <param name="cfg">Config.</param>
        private static void CheckDefaultProperties(ClientConnectorConfiguration cfg)
        {
            Assert.AreEqual(ClientConnectorConfiguration.DefaultPort, cfg.Port);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultPortRange, cfg.PortRange);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultMaxOpenCursorsPerConnection,
                cfg.MaxOpenCursorsPerConnection);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultSocketBufferSize, cfg.SocketReceiveBufferSize);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultSocketBufferSize, cfg.SocketSendBufferSize);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultTcpNoDelay, cfg.TcpNoDelay);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultThreadPoolSize, cfg.ThreadPoolSize);
        }

        /// <summary>
        /// Checks the default properties.
        /// </summary>
        /// <param name="cfg">Config.</param>
        private static void CheckDefaultProperties(SqlConnectorConfiguration cfg)
        {
            Assert.AreEqual(ClientConnectorConfiguration.DefaultPort, cfg.Port);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultPortRange, cfg.PortRange);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultMaxOpenCursorsPerConnection,
                cfg.MaxOpenCursorsPerConnection);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultSocketBufferSize, cfg.SocketReceiveBufferSize);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultSocketBufferSize, cfg.SocketSendBufferSize);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultTcpNoDelay, cfg.TcpNoDelay);
            Assert.AreEqual(ClientConnectorConfiguration.DefaultThreadPoolSize, cfg.ThreadPoolSize);
        }

        /// <summary>
        /// Checks the default value attributes.
        /// </summary>
        /// <param name="obj">The object.</param>
        private static void CheckDefaultValueAttributes(object obj)
        {
            var props = obj.GetType().GetProperties();

            foreach (var prop in props.Where(p => p.Name != "SelectorsCount" && p.Name != "ReadStripesNumber" &&
                                                  !p.Name.Contains("ThreadPoolSize") &&
                                                  !(p.Name == "MaxSize" &&
                                                    p.DeclaringType == typeof(MemoryPolicyConfiguration))))
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
                        Endpoints = new[] {"127.0.0.1:49900", "127.0.0.1:49901"}
                    },
                    ClientReconnectDisabled = true,
                    ForceServerMode = true,
                    IpFinderCleanFrequency = TimeSpan.FromMinutes(7),
                    LocalAddress = "127.0.0.1",
                    LocalPort = 49900,
                    LocalPortRange = 13,
                    ReconnectCount = 11,
                    StatisticsPrintFrequency = TimeSpan.FromSeconds(20),
                    ThreadPriority = 6,
                    TopologyHistorySize = 1234567
                },
                IgniteInstanceName = "gridName1",
                IncludedEventTypes = EventType.DiscoveryAll,
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
                IsDaemon = false,
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
                ClientFailureDetectionTimeout = TimeSpan.FromMinutes(12.3),
                LongQueryWarningTimeout = TimeSpan.FromMinutes(1.23),
                IsActiveOnStart = true,
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
                            KeepDeserialized = false
                        }
                    }
                },
                // Skip cache check because with persistence the grid is not active by default.
                PluginConfigurations = new[] {new TestIgnitePluginConfiguration {SkipCacheCheck = true}},
                EventStorageSpi = new MemoryEventStorageSpi
                {
                    ExpirationTimeout = TimeSpan.FromSeconds(5),
                    MaxEventCount = 10
                },
                MemoryConfiguration = new MemoryConfiguration
                {
                    ConcurrencyLevel = 3,
                    DefaultMemoryPolicyName = "myDefaultPlc",
                    PageSize = 2048,
                    SystemCacheInitialSize = 13 * 1024 * 1024,
                    SystemCacheMaxSize = 15 * 1024 * 1024,
                    MemoryPolicies = new[]
                    {
                        new MemoryPolicyConfiguration
                        {
                            Name = "myDefaultPlc",
                            PageEvictionMode = DataPageEvictionMode.Disabled,
                            InitialSize = 340 * 1024 * 1024,
                            MaxSize = 345 * 1024 * 1024,
                            EvictionThreshold = 0.88,
                            EmptyPagesPoolSize = 77,
                            SwapFilePath = "myPath1",
                            RateTimeInterval = TimeSpan.FromSeconds(35),
                            SubIntervals = 7
                        },
                        new MemoryPolicyConfiguration
                        {
                            Name = "customPlc",
                            PageEvictionMode = DataPageEvictionMode.Disabled,
                            MaxSize = 456 * 1024 * 1024,
                            EvictionThreshold = 0.77,
                            EmptyPagesPoolSize = 66,
                            SwapFilePath = "somePath2",
                            MetricsEnabled = true
                        }
                    }
                },
                PublicThreadPoolSize = 3,
                StripedThreadPoolSize = 5,
                ServiceThreadPoolSize = 6,
                SystemThreadPoolSize = 7,
                AsyncCallbackThreadPoolSize = 8,
                ManagementThreadPoolSize = 9,
                DataStreamerThreadPoolSize = 10,
                UtilityCacheThreadPoolSize = 11,
                QueryThreadPoolSize = 12,
                SqlConnectorConfiguration = new SqlConnectorConfiguration
                {
                    Host = "127.0.0.2",
                    Port = 1081,
                    PortRange = 3,
                    SocketReceiveBufferSize = 2048,
                    MaxOpenCursorsPerConnection = 5,
                    ThreadPoolSize = 4,
                    TcpNoDelay = false,
                    SocketSendBufferSize = 4096
                },
                PersistentStoreConfiguration = new PersistentStoreConfiguration
                {
                    AlwaysWriteFullPages = true,
                    CheckpointingFrequency = TimeSpan.FromSeconds(25),
                    CheckpointingPageBufferSize = 28 * 1024 * 1024,
                    CheckpointingThreads = 2,
                    LockWaitTime = TimeSpan.FromSeconds(5),
                    PersistentStorePath = Path.GetTempPath(),
                    TlbSize = 64 * 1024,
                    WalArchivePath = Path.GetTempPath(),
                    WalFlushFrequency = TimeSpan.FromSeconds(3),
                    WalFsyncDelayNanos = 3,
                    WalHistorySize = 10,
                    WalMode = WalMode.LogOnly,
                    WalRecordIteratorBufferSize = 32 * 1024 * 1024,
                    WalSegments = 6,
                    WalSegmentSize = 5 * 1024 * 1024,
                    WalStorePath = Path.GetTempPath(),
                    MetricsEnabled = true,
                    SubIntervals = 7,
                    RateTimeInterval = TimeSpan.FromSeconds(9),
                    CheckpointWriteOrder = CheckpointWriteOrder.Random,
                    WriteThrottlingEnabled = true
                },
                ConsistentId = new MyConsistentId {Data = "abc"}
            };
        }

        private class MyConsistentId
        {
            public string Data { get; set; }

            private bool Equals(MyConsistentId other)
            {
                return string.Equals(Data, other.Data);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((MyConsistentId) obj);
            }

            public override int GetHashCode()
            {
                return (Data != null ? Data.GetHashCode() : 0);
            }

            public static bool operator ==(MyConsistentId left, MyConsistentId right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(MyConsistentId left, MyConsistentId right)
            {
                return !Equals(left, right);
            }
        }
    }
}

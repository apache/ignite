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

// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable MemberCanBePrivate.Global
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Xml;
    using System.Xml.Schema;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Tests.Binary;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IgniteConfiguration"/> serialization.
    /// </summary>
    public class IgniteConfigurationSerializerTest
    {
        /// <summary>
        /// Tests the predefined XML.
        /// </summary>
        [Test]
        public void TestPredefinedXml()
        {
            var xml = @"<igniteConfig workDirectory='c:' JvmMaxMemoryMb='1024' MetricsLogFrequency='0:0:10' isDaemon='true' isLateAffinityAssignment='false' springConfigUrl='c:\myconfig.xml'>
                            <localhost>127.1.1.1</localhost>
                            <binaryConfiguration compactFooter='false'>
                                <defaultNameMapper type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+NameMapper, Apache.Ignite.Core.Tests' bar='testBar' />
                                <types>
                                    <string>Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+FooClass, Apache.Ignite.Core.Tests</string>
                                </types>
                            </binaryConfiguration>
                            <discoverySpi type='TcpDiscoverySpi' joinTimeout='0:1:0' localAddress='192.168.1.1' localPort='6655'>
                                <ipFinder type='TcpDiscoveryMulticastIpFinder' addressRequestAttempts='7' />
                            </discoverySpi>
                            <communicationSpi type='TcpCommunicationSpi' ackSendThreshold='33' idleConnectionTimeout='0:1:2' />
                            <jvmOptions><string>-Xms1g</string><string>-Xmx4g</string></jvmOptions>
                            <lifecycleBeans>
                                <iLifecycleBean type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+LifecycleBean, Apache.Ignite.Core.Tests' foo='15' />
                            </lifecycleBeans>
                            <cacheConfiguration>
                                <cacheConfiguration cacheMode='Replicated' readThrough='true' writeThrough='true'>
                                    <queryEntities>    
                                        <queryEntity keyType='System.Int32' valueType='System.String'>    
                                            <fields>
                                                <queryField name='length' fieldType='System.Int32' />
                                            </fields>
                                            <aliases>
                                                <queryAlias fullName='somefield.field' alias='shortField' />
                                            </aliases>
                                            <indexes>
                                                <queryIndex name='idx' indexType='Geospatial'>
                                                    <fields>
                                                        <queryIndexField name='indexFld' isDescending='true' />
                                                    </fields>
                                                </queryIndex>
                                            </indexes>
                                        </queryEntity>
                                    </queryEntities>
                                    <evictionPolicy type='LruEvictionPolicy' batchSize='1' maxSize='2' maxMemorySize='3' />
                                    <nearConfiguration nearStartSize='7'>
                                        <evictionPolicy type='FifoEvictionPolicy' batchSize='10' maxSize='20' maxMemorySize='30' />
                                    </nearConfiguration>
                                    <affinityFunction type='RendezvousAffinityFunction' partitions='99' excludeNeighbors='true' />
                                </cacheConfiguration>
                                <cacheConfiguration name='secondCache' />
                            </cacheConfiguration>
                            <includedEventTypes>
                                <int>42</int>
                                <int>TaskFailed</int>
                                <int>JobFinished</int>
                            </includedEventTypes>
                            <userAttributes><pair key='myNode' value='true' /></userAttributes>
                            <atomicConfiguration backups='2' cacheMode='Local' atomicSequenceReserveSize='250' />
                            <transactionConfiguration defaultTransactionConcurrency='Optimistic' defaultTransactionIsolation='RepeatableRead' defaultTimeout='0:1:2' pessimisticTransactionLogSize='15' pessimisticTransactionLogLinger='0:0:33' />
                        </igniteConfig>";
            var reader = XmlReader.Create(new StringReader(xml));

            var cfg = IgniteConfigurationXmlSerializer.Deserialize(reader);

            Assert.AreEqual("c:", cfg.WorkDirectory);
            Assert.AreEqual("127.1.1.1", cfg.Localhost);
            Assert.IsTrue(cfg.IsDaemon);
            Assert.IsFalse(cfg.IsLateAffinityAssignment);
            Assert.AreEqual(1024, cfg.JvmMaxMemoryMb);
            Assert.AreEqual(TimeSpan.FromSeconds(10), cfg.MetricsLogFrequency);
            Assert.AreEqual(TimeSpan.FromMinutes(1), ((TcpDiscoverySpi)cfg.DiscoverySpi).JoinTimeout);
            Assert.AreEqual("192.168.1.1", ((TcpDiscoverySpi)cfg.DiscoverySpi).LocalAddress);
            Assert.AreEqual(6655, ((TcpDiscoverySpi)cfg.DiscoverySpi).LocalPort);
            Assert.AreEqual(7,
                ((TcpDiscoveryMulticastIpFinder) ((TcpDiscoverySpi) cfg.DiscoverySpi).IpFinder).AddressRequestAttempts);
            Assert.AreEqual(new[] { "-Xms1g", "-Xmx4g" }, cfg.JvmOptions);
            Assert.AreEqual(15, ((LifecycleBean) cfg.LifecycleBeans.Single()).Foo);
            Assert.AreEqual("testBar", ((NameMapper) cfg.BinaryConfiguration.DefaultNameMapper).Bar);
            Assert.AreEqual(
                "Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+FooClass, Apache.Ignite.Core.Tests",
                cfg.BinaryConfiguration.Types.Single());
            Assert.IsFalse(cfg.BinaryConfiguration.CompactFooter);
            Assert.AreEqual(new[] {42, EventType.TaskFailed, EventType.JobFinished}, cfg.IncludedEventTypes);
            Assert.AreEqual(@"c:\myconfig.xml", cfg.SpringConfigUrl);

            Assert.AreEqual("secondCache", cfg.CacheConfiguration.Last().Name);

            var cacheCfg = cfg.CacheConfiguration.First();

            Assert.AreEqual(CacheMode.Replicated, cacheCfg.CacheMode);
            Assert.IsTrue(cacheCfg.ReadThrough);
            Assert.IsTrue(cacheCfg.WriteThrough);

            var queryEntity = cacheCfg.QueryEntities.Single();
            Assert.AreEqual(typeof(int), queryEntity.KeyType);
            Assert.AreEqual(typeof(string), queryEntity.ValueType);
            Assert.AreEqual("length", queryEntity.Fields.Single().Name);
            Assert.AreEqual(typeof(int), queryEntity.Fields.Single().FieldType);
            Assert.AreEqual("somefield.field", queryEntity.Aliases.Single().FullName);
            Assert.AreEqual("shortField", queryEntity.Aliases.Single().Alias);
            Assert.AreEqual(QueryIndexType.Geospatial, queryEntity.Indexes.Single().IndexType);
            Assert.AreEqual("indexFld", queryEntity.Indexes.Single().Fields.Single().Name);
            Assert.AreEqual(true, queryEntity.Indexes.Single().Fields.Single().IsDescending);

            var nearCfg = cacheCfg.NearConfiguration;
            Assert.IsNotNull(nearCfg);
            Assert.AreEqual(7, nearCfg.NearStartSize);

            var plc = nearCfg.EvictionPolicy as FifoEvictionPolicy;
            Assert.IsNotNull(plc);
            Assert.AreEqual(10, plc.BatchSize);
            Assert.AreEqual(20, plc.MaxSize);
            Assert.AreEqual(30, plc.MaxMemorySize);

            var plc2 = cacheCfg.EvictionPolicy as LruEvictionPolicy;
            Assert.IsNotNull(plc2);
            Assert.AreEqual(1, plc2.BatchSize);
            Assert.AreEqual(2, plc2.MaxSize);
            Assert.AreEqual(3, plc2.MaxMemorySize);

            var af = cacheCfg.AffinityFunction as RendezvousAffinityFunction;
            Assert.IsNotNull(af);
            Assert.AreEqual(99, af.Partitions);
            Assert.IsTrue(af.ExcludeNeighbors);

            Assert.AreEqual(new Dictionary<string, object> {{"myNode", "true"}}, cfg.UserAttributes);

            var atomicCfg = cfg.AtomicConfiguration;
            Assert.AreEqual(2, atomicCfg.Backups);
            Assert.AreEqual(CacheMode.Local, atomicCfg.CacheMode);
            Assert.AreEqual(250, atomicCfg.AtomicSequenceReserveSize);

            var tx = cfg.TransactionConfiguration;
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.DefaultTransactionConcurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.DefaultTransactionIsolation);
            Assert.AreEqual(new TimeSpan(0,1,2), tx.DefaultTimeout);
            Assert.AreEqual(15, tx.PessimisticTransactionLogSize);
            Assert.AreEqual(TimeSpan.FromSeconds(33), tx.PessimisticTransactionLogLinger);

            var comm = cfg.CommunicationSpi as TcpCommunicationSpi;
            Assert.IsNotNull(comm);
            Assert.AreEqual(33, comm.AckSendThreshold);
            Assert.AreEqual(new TimeSpan(0, 1, 2), comm.IdleConnectionTimeout);
        }

        /// <summary>
        /// Tests the serialize deserialize.
        /// </summary>
        [Test]
        public void TestSerializeDeserialize()
        {
            // Test custom
            CheckSerializeDeserialize(GetTestConfig());

            // Test custom with different culture to make sure numbers are serialized properly
            RunWithCustomCulture(() => CheckSerializeDeserialize(GetTestConfig()));
            
            // Test default
            CheckSerializeDeserialize(new IgniteConfiguration());
        }

        /// <summary>
        /// Tests the schema validation.
        /// </summary>
        [Test]
        public void TestSchemaValidation()
        {
            CheckSchemaValidation();

            RunWithCustomCulture(CheckSchemaValidation);

            // Check invalid xml
            const string invalidXml =
                @"<igniteConfiguration xmlns='http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection'>
                    <binaryConfiguration /><binaryConfiguration />
                  </igniteConfiguration>";

            Assert.Throws<XmlSchemaValidationException>(() => CheckSchemaValidation(invalidXml));
        }

        /// <summary>
        /// Checks the schema validation.
        /// </summary>
        private static void CheckSchemaValidation()
        {
            var sb = new StringBuilder();

            using (var xmlWriter = XmlWriter.Create(sb))
            {
                IgniteConfigurationXmlSerializer.Serialize(GetTestConfig(), xmlWriter, "igniteConfiguration");
            }

            CheckSchemaValidation(sb.ToString());
        }

        /// <summary>
        /// Checks the schema validation.
        /// </summary>
        /// <param name="xml">The XML.</param>
        private static void CheckSchemaValidation(string xml)
        {
            var document = new XmlDocument();

            document.Schemas.Add("http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection", 
                XmlReader.Create("IgniteConfigurationSection.xsd"));

            document.Load(new StringReader(xml));

            document.Validate(null);
        }

        /// <summary>
        /// Checks the serialize deserialize.
        /// </summary>
        /// <param name="cfg">The config.</param>
        private static void CheckSerializeDeserialize(IgniteConfiguration cfg)
        {
            var resCfg = SerializeDeserialize(cfg);

            AssertReflectionEqual(cfg, resCfg);
        }

        /// <summary>
        /// Serializes and deserializes a config.
        /// </summary>
        private static IgniteConfiguration SerializeDeserialize(IgniteConfiguration cfg)
        {
            var sb = new StringBuilder();

            using (var xmlWriter = XmlWriter.Create(sb))
            {
                IgniteConfigurationXmlSerializer.Serialize(cfg, xmlWriter, "igniteConfig");
            }

            var xml = sb.ToString();

            using (var xmlReader = XmlReader.Create(new StringReader(xml)))
            {
                xmlReader.MoveToContent();
                return IgniteConfigurationXmlSerializer.Deserialize(xmlReader);
            }
        }

        /// <summary>
        /// Asserts equality with reflection.
        /// </summary>
        private static void AssertReflectionEqual(object x, object y)
        {
            var type = x.GetType();

            Assert.AreEqual(type, y.GetType());

            if (type.IsValueType || type == typeof (string) || type.IsSubclassOf(typeof (Type)))
            {
                Assert.AreEqual(x, y);
                return;
            }

            var props = type.GetProperties().Where(p => p.GetIndexParameters().Length == 0);

            foreach (var propInfo in props)
            {
                var propType = propInfo.PropertyType;

                var xVal = propInfo.GetValue(x, null);
                var yVal = propInfo.GetValue(y, null);

                if (xVal == null || yVal == null)
                {
                    Assert.IsNull(xVal);
                    Assert.IsNull(yVal);
                }
                else if (propType != typeof(string) && propType.IsGenericType 
                    && propType.GetGenericTypeDefinition() == typeof (ICollection<>))
                {
                    var xCol = ((IEnumerable) xVal).OfType<object>().ToList();
                    var yCol = ((IEnumerable) yVal).OfType<object>().ToList();

                    Assert.AreEqual(xCol.Count, yCol.Count);

                    for (int i = 0; i < xCol.Count; i++)
                        AssertReflectionEqual(xCol[i], yCol[i]);
                }
                else
                {
                    AssertReflectionEqual(xVal, yVal);
                }
            }
        }

        /// <summary>
        /// Gets the test configuration.
        /// </summary>
        private static IgniteConfiguration GetTestConfig()
        {
            return new IgniteConfiguration
            {
                GridName = "gridName",
                JvmOptions = new[] {"1", "2"},
                Localhost = "localhost11",
                JvmClasspath = "classpath",
                Assemblies = new[] {"asm1", "asm2", "asm3"},
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration
                        {
                            IsEnum = true,
                            KeepDeserialized = true,
                            AffinityKeyFieldName = "affKeyFieldName",
                            TypeName = "typeName",
                            IdMapper = new IdMapper(),
                            NameMapper = new NameMapper(),
                            Serializer = new TestSerializer()
                        },
                        new BinaryTypeConfiguration
                        {
                            IsEnum = false,
                            KeepDeserialized = false,
                            AffinityKeyFieldName = "affKeyFieldName",
                            TypeName = "typeName2",
                            Serializer = new BinaryReflectiveSerializer()
                        }
                    },
                    Types = new[] {typeof (string).FullName},
                    DefaultIdMapper = new IdMapper(),
                    DefaultKeepDeserialized = true,
                    DefaultNameMapper = new NameMapper(),
                    DefaultSerializer = new TestSerializer()
                },
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("cacheName")
                    {
                        AtomicWriteOrderMode = CacheAtomicWriteOrderMode.Primary,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        Backups = 15,
                        CacheMode = CacheMode.Replicated,
                        CacheStoreFactory = new TestCacheStoreFactory(),
                        CopyOnRead = true,
                        EagerTtl = true,
                        EnableSwap = true,
                        EvictSynchronized = true,
                        EvictSynchronizedConcurrencyLevel = 13,
                        EvictSynchronizedKeyBufferSize = 14,
                        EvictSynchronizedTimeout = TimeSpan.FromMinutes(3),
                        Invalidate = true,
                        KeepBinaryInStore = true,
                        LoadPreviousValue = true,
                        LockTimeout = TimeSpan.FromSeconds(56),
                        LongQueryWarningTimeout = TimeSpan.FromSeconds(99),
                        MaxConcurrentAsyncOperations = 24,
                        MaxEvictionOverflowRatio = 5.6F,
                        MemoryMode = CacheMemoryMode.OffheapValues,
                        OffHeapMaxMemory = 567,
                        QueryEntities = new[]
                        {
                            new QueryEntity
                            {
                                Fields = new[]
                                {
                                    new QueryField("field", typeof (int))
                                },
                                Indexes = new[]
                                {
                                    new QueryIndex("field") {IndexType = QueryIndexType.FullText}
                                },
                                Aliases = new[]
                                {
                                    new QueryAlias("field.field", "fld")
                                },
                                KeyType = typeof (string),
                                ValueType = typeof (long)
                            },
                        },
                        ReadFromBackup = true,
                        RebalanceBatchSize = 33,
                        RebalanceDelay = TimeSpan.MaxValue,
                        RebalanceMode = CacheRebalanceMode.Sync,
                        RebalanceThrottle = TimeSpan.FromHours(44),
                        RebalanceTimeout = TimeSpan.FromMinutes(8),
                        SqlEscapeAll = true,
                        SqlOnheapRowCacheSize = 679,
                        StartSize = 1023,
                        WriteBehindBatchSize = 45,
                        WriteBehindEnabled = true,
                        WriteBehindFlushFrequency = TimeSpan.FromSeconds(5),
                        WriteBehindFlushSize = 66,
                        WriteBehindFlushThreadCount = 2,
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullAsync,
                        NearConfiguration = new NearCacheConfiguration
                        {
                            NearStartSize = 5,
                            EvictionPolicy = new FifoEvictionPolicy
                            {
                                BatchSize = 19, MaxMemorySize = 1024, MaxSize = 555
                            }
                        },
                        EvictionPolicy = new LruEvictionPolicy
                        {
                            BatchSize = 18, MaxMemorySize = 1023, MaxSize = 554
                        },
                        AffinityFunction = new FairAffinityFunction
                        {
                            ExcludeNeighbors = true,
                            Partitions = 48
                        }
                    }
                },
                ClientMode = true,
                DiscoverySpi = new TcpDiscoverySpi
                {
                    NetworkTimeout = TimeSpan.FromSeconds(1),
                    SocketTimeout = TimeSpan.FromSeconds(2),
                    AckTimeout = TimeSpan.FromSeconds(3),
                    JoinTimeout = TimeSpan.FromSeconds(4),
                    MaxAckTimeout = TimeSpan.FromSeconds(5),
                    IpFinder = new TcpDiscoveryMulticastIpFinder
                    {
                        TimeToLive = 110,
                        MulticastGroup = "multicastGroup",
                        AddressRequestAttempts = 10,
                        MulticastPort = 987,
                        ResponseTimeout = TimeSpan.FromDays(1),
                        LocalAddress = "127.0.0.2",
                        Endpoints = new[] {"", "abc"}
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
                IgniteHome = "igniteHome",
                IncludedEventTypes = EventType.CacheQueryAll,
                JvmDllPath = @"c:\jvm",
                JvmInitialMemoryMb = 1024,
                JvmMaxMemoryMb = 2048,
                LifecycleBeans = new[] {new LifecycleBean(), new LifecycleBean()},
                MetricsExpireTime = TimeSpan.FromSeconds(15),
                MetricsHistorySize = 45,
                MetricsLogFrequency = TimeSpan.FromDays(2),
                MetricsUpdateFrequency = TimeSpan.MinValue,
                NetworkSendRetryCount = 7,
                NetworkSendRetryDelay = TimeSpan.FromSeconds(98),
                NetworkTimeout = TimeSpan.FromMinutes(4),
                SuppressWarnings = true,
                WorkDirectory = @"c:\work",
                IsDaemon = true,
                UserAttributes = Enumerable.Range(1, 10).ToDictionary(x => x.ToString(), x => (object) x),
                AtomicConfiguration = new AtomicConfiguration
                {
                    CacheMode = CacheMode.Replicated,
                    AtomicSequenceReserveSize = 200,
                    Backups = 2
                },
                TransactionConfiguration = new TransactionConfiguration
                {
                    PessimisticTransactionLogSize = 23,
                    DefaultTransactionIsolation = TransactionIsolation.ReadCommitted,
                    DefaultTimeout = TimeSpan.FromDays(2),
                    DefaultTransactionConcurrency = TransactionConcurrency.Optimistic,
                    PessimisticTransactionLogLinger = TimeSpan.FromHours(3)
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
                }
            };
        }

        /// <summary>
        /// Runs the with custom culture.
        /// </summary>
        /// <param name="action">The action.</param>
        private static void RunWithCustomCulture(Action action)
        {
            RunWithCulture(action, CultureInfo.InvariantCulture);
            RunWithCulture(action, CultureInfo.GetCultureInfo("ru-RU"));
        }

        /// <summary>
        /// Runs the with culture.
        /// </summary>
        /// <param name="action">The action.</param>
        /// <param name="cultureInfo">The culture information.</param>
        private static void RunWithCulture(Action action, CultureInfo cultureInfo)
        {
            var oldCulture = Thread.CurrentThread.CurrentCulture;

            try
            {
                Thread.CurrentThread.CurrentCulture = cultureInfo;

                action();
            }
            finally
            {
                Thread.CurrentThread.CurrentCulture = oldCulture;
            }
        }

        /// <summary>
        /// Test bean.
        /// </summary>
        public class LifecycleBean : ILifecycleBean
        {
            /// <summary>
            /// Gets or sets the foo.
            /// </summary>
            /// <value>
            /// The foo.
            /// </value>
            public int Foo { get; set; }

            /// <summary>
            /// This method is called when lifecycle event occurs.
            /// </summary>
            /// <param name="evt">Lifecycle event.</param>
            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Test mapper.
        /// </summary>
        public class NameMapper : IBinaryNameMapper
        {
            /// <summary>
            /// Gets or sets the bar.
            /// </summary>
            /// <value>
            /// The bar.
            /// </value>
            public string Bar { get; set; }

            /// <summary>
            /// Gets the type name.
            /// </summary>
            /// <param name="name">The name.</param>
            /// <returns>
            /// Type name.
            /// </returns>
            public string GetTypeName(string name)
            {
                return name;
            }

            /// <summary>
            /// Gets the field name.
            /// </summary>
            /// <param name="name">The name.</param>
            /// <returns>
            /// Field name.
            /// </returns>
            public string GetFieldName(string name)
            {
                return name;
            }
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        public class TestSerializer : IBinarySerializer
        {
            /// <summary>
            /// Write portalbe object.
            /// </summary>
            /// <param name="obj">Object.</param>
            /// <param name="writer">Poratble writer.</param>
            public void WriteBinary(object obj, IBinaryWriter writer)
            {
                // No-op.
            }

            /// <summary>
            /// Read binary object.
            /// </summary>
            /// <param name="obj">Instantiated empty object.</param>
            /// <param name="reader">Poratble reader.</param>
            public void ReadBinary(object obj, IBinaryReader reader)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Test class.
        /// </summary>
        public class FooClass
        {
            // No-op.
        }

        /// <summary>
        /// Test factory.
        /// </summary>
        public class TestCacheStoreFactory : IFactory<ICacheStore>
        {
            /// <summary>
            /// Creates an instance of the cache store.
            /// </summary>
            /// <returns>
            /// New instance of the cache store.
            /// </returns>
            public ICacheStore CreateInstance()
            {
                return null;
            }
        }
    }
}

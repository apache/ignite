﻿/*
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
    using System.Configuration;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Xml;
    using System.Xml.Linq;
    using System.Xml.Schema;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Plugin.Cache;
    using Apache.Ignite.Core.Tests.Binary;
    using Apache.Ignite.Core.Tests.Plugin;
    using Apache.Ignite.Core.Transactions;
    using Apache.Ignite.NLog;
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
            var xml = @"<igniteConfig workDirectory='c:' JvmMaxMemoryMb='1024' MetricsLogFrequency='0:0:10' isDaemon='true' isLateAffinityAssignment='false' springConfigUrl='c:\myconfig.xml' autoGenerateIgniteInstanceName='true' peerAssemblyLoadingMode='CurrentAppDomain'>
                            <localhost>127.1.1.1</localhost>
                            <binaryConfiguration compactFooter='false' keepDeserialized='true'>
                                <nameMapper type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+NameMapper' bar='testBar' />
                                <idMapper type='Apache.Ignite.Core.Tests.Binary.IdMapper' />
                                <types>
                                    <string>Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+FooClass, Apache.Ignite.Core.Tests</string>
                                </types>
                                <typeConfigurations>
                                    <binaryTypeConfiguration affinityKeyFieldName='affKeyFieldName' isEnum='true' keepDeserialized='True' typeName='typeName'>
                                        <idMapper type='Apache.Ignite.Core.Tests.Binary.IdMapper, Apache.Ignite.Core.Tests' />
                                        <nameMapper type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+NameMapper, Apache.Ignite.Core.Tests' />
                                        <serializer type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+TestSerializer, Apache.Ignite.Core.Tests' />
                                    </binaryTypeConfiguration>
                                </typeConfigurations>
                            </binaryConfiguration>
                            <discoverySpi type='TcpDiscoverySpi' joinTimeout='0:1:0' localAddress='192.168.1.1' localPort='6655'>
                                <ipFinder type='TcpDiscoveryMulticastIpFinder' addressRequestAttempts='7' />
                            </discoverySpi>
                            <communicationSpi type='TcpCommunicationSpi' ackSendThreshold='33' idleConnectionTimeout='0:1:2' />
                            <jvmOptions><string>-Xms1g</string><string>-Xmx4g</string></jvmOptions>
                            <lifecycleHandlers>
                                <iLifecycleHandler type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+LifecycleBean' foo='15' />
                            </lifecycleHandlers>
                            <cacheConfiguration>
                                <cacheConfiguration cacheMode='Replicated' readThrough='true' writeThrough='true' enableStatistics='true' writeBehindCoalescing='false' partitionLossPolicy='ReadWriteAll'>
                                    <queryEntities>    
                                        <queryEntity keyType='System.Int32' valueType='System.String' tableName='myTable'>
                                            <fields>
                                                <queryField name='length' fieldType='System.Int32' isKeyField='true' />
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
                                    <expiryPolicyFactory type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+MyPolicyFactory, Apache.Ignite.Core.Tests' />
                                    <pluginConfigurations><iCachePluginConfiguration type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+MyPluginConfiguration, Apache.Ignite.Core.Tests' /></pluginConfigurations>
                                </cacheConfiguration>
                                <cacheConfiguration name='secondCache' />
                            </cacheConfiguration>
                            <includedEventTypes>
                                <int>42</int>
                                <int>TaskFailed</int>
                                <int>JobFinished</int>
                            </includedEventTypes>
                            <userAttributes>
                                <pair key='myNode' value='true' />
                                <pair key='foo'><value type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+FooClass, Apache.Ignite.Core.Tests'><bar>Baz</bar></value></pair>
                            </userAttributes>
                            <atomicConfiguration backups='2' cacheMode='Local' atomicSequenceReserveSize='250' />
                            <transactionConfiguration defaultTransactionConcurrency='Optimistic' defaultTransactionIsolation='RepeatableRead' defaultTimeout='0:1:2' pessimisticTransactionLogSize='15' pessimisticTransactionLogLinger='0:0:33' />
                            <logger type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+TestLogger, Apache.Ignite.Core.Tests' />
                            <pluginConfigurations>
                                <iPluginConfiguration type='Apache.Ignite.Core.Tests.Plugin.TestIgnitePluginConfiguration, Apache.Ignite.Core.Tests' />
                            </pluginConfigurations>
                            <eventStorageSpi type='MemoryEventStorageSpi' expirationTimeout='00:00:23.45' maxEventCount='129' />
                            <memoryConfiguration concurrencyLevel='3' defaultMemoryPolicyName='dfPlc' pageSize='45' systemCacheInitialSize='67' systemCacheMaxSize='68'>
                                <memoryPolicies>
                                    <memoryPolicyConfiguration emptyPagesPoolSize='1' evictionThreshold='0.2' name='dfPlc' pageEvictionMode='RandomLru' initialSize='89' maxSize='98' swapFilePath='abc' metricsEnabled='true' rateTimeInterval='0:1:2' subIntervals='9' />
                                </memoryPolicies>
                            </memoryConfiguration>
                        </igniteConfig>";

            var cfg = IgniteConfiguration.FromXml(xml);

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
            Assert.AreEqual(15, ((LifecycleBean) cfg.LifecycleHandlers.Single()).Foo);
            Assert.AreEqual("testBar", ((NameMapper) cfg.BinaryConfiguration.NameMapper).Bar);
            Assert.AreEqual(
                "Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+FooClass, Apache.Ignite.Core.Tests",
                cfg.BinaryConfiguration.Types.Single());
            Assert.IsFalse(cfg.BinaryConfiguration.CompactFooter);
            Assert.AreEqual(new[] {42, EventType.TaskFailed, EventType.JobFinished}, cfg.IncludedEventTypes);
            Assert.AreEqual(@"c:\myconfig.xml", cfg.SpringConfigUrl);
            Assert.IsTrue(cfg.AutoGenerateIgniteInstanceName);

            Assert.AreEqual("secondCache", cfg.CacheConfiguration.Last().Name);

            var cacheCfg = cfg.CacheConfiguration.First();

            Assert.AreEqual(CacheMode.Replicated, cacheCfg.CacheMode);
            Assert.IsTrue(cacheCfg.ReadThrough);
            Assert.IsTrue(cacheCfg.WriteThrough);
            Assert.IsInstanceOf<MyPolicyFactory>(cacheCfg.ExpiryPolicyFactory);
            Assert.IsTrue(cacheCfg.EnableStatistics);
            Assert.IsFalse(cacheCfg.WriteBehindCoalescing);
            Assert.AreEqual(PartitionLossPolicy.ReadWriteAll, cacheCfg.PartitionLossPolicy);

            var queryEntity = cacheCfg.QueryEntities.Single();
            Assert.AreEqual(typeof(int), queryEntity.KeyType);
            Assert.AreEqual(typeof(string), queryEntity.ValueType);
            Assert.AreEqual("myTable", queryEntity.TableName);
            Assert.AreEqual("length", queryEntity.Fields.Single().Name);
            Assert.AreEqual(typeof(int), queryEntity.Fields.Single().FieldType);
            Assert.IsTrue(queryEntity.Fields.Single().IsKeyField);
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

            Assert.AreEqual(new Dictionary<string, object>
            {
                {"myNode", "true"},
                {"foo", new FooClass {Bar = "Baz"}}
            }, cfg.UserAttributes);

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

            Assert.IsInstanceOf<TestLogger>(cfg.Logger);

            var binType = cfg.BinaryConfiguration.TypeConfigurations.Single();
            Assert.AreEqual("typeName", binType.TypeName);
            Assert.AreEqual("affKeyFieldName", binType.AffinityKeyFieldName);
            Assert.IsTrue(binType.IsEnum);
            Assert.AreEqual(true, binType.KeepDeserialized);
            Assert.IsInstanceOf<IdMapper>(binType.IdMapper);
            Assert.IsInstanceOf<NameMapper>(binType.NameMapper);
            Assert.IsInstanceOf<TestSerializer>(binType.Serializer);

            var plugins = cfg.PluginConfigurations;
            Assert.IsNotNull(plugins);
            Assert.IsNotNull(plugins.Cast<TestIgnitePluginConfiguration>().SingleOrDefault());

            Assert.IsNotNull(cacheCfg.PluginConfigurations.Cast<MyPluginConfiguration>().SingleOrDefault());

            var eventStorage = cfg.EventStorageSpi as MemoryEventStorageSpi;
            Assert.IsNotNull(eventStorage);
            Assert.AreEqual(23.45, eventStorage.ExpirationTimeout.TotalSeconds);
            Assert.AreEqual(129, eventStorage.MaxEventCount);

            var memCfg = cfg.MemoryConfiguration;
            Assert.IsNotNull(memCfg);
            Assert.AreEqual(3, memCfg.ConcurrencyLevel);
            Assert.AreEqual("dfPlc", memCfg.DefaultMemoryPolicyName);
            Assert.AreEqual(45, memCfg.PageSize);
            Assert.AreEqual(67, memCfg.SystemCacheInitialSize);
            Assert.AreEqual(68, memCfg.SystemCacheMaxSize);

            var memPlc = memCfg.MemoryPolicies.Single();
            Assert.AreEqual(1, memPlc.EmptyPagesPoolSize);
            Assert.AreEqual(0.2, memPlc.EvictionThreshold);
            Assert.AreEqual("dfPlc", memPlc.Name);
            Assert.AreEqual(DataPageEvictionMode.RandomLru, memPlc.PageEvictionMode);
            Assert.AreEqual("abc", memPlc.SwapFilePath);
            Assert.AreEqual(89, memPlc.InitialSize);
            Assert.AreEqual(98, memPlc.MaxSize);
            Assert.IsTrue(memPlc.MetricsEnabled);
            Assert.AreEqual(9, memPlc.SubIntervals);
            Assert.AreEqual(TimeSpan.FromSeconds(62), memPlc.RateTimeInterval);

            Assert.AreEqual(PeerAssemblyLoadingMode.CurrentAppDomain, cfg.PeerAssemblyLoadingMode);
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
        /// Tests that all properties are present in the schema.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        public void TestAllPropertiesArePresentInSchema()
        {
            var schema = XDocument.Load("IgniteConfigurationSection.xsd")
                    .Root.Elements()
                    .Single(x => x.Attribute("name").Value == "igniteConfiguration");

            var type = typeof(IgniteConfiguration);

            CheckPropertyIsPresentInSchema(type, schema);
        }

        /// <summary>
        /// Checks the property is present in schema.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private static void CheckPropertyIsPresentInSchema(Type type, XElement schema)
        {
            Func<string, string> toLowerCamel = x => char.ToLowerInvariant(x[0]) + x.Substring(1);

            foreach (var prop in type.GetProperties())
            {
                if (!prop.CanWrite)
                    continue;  // Read-only properties are not configured in XML.

                if (prop.GetCustomAttributes(typeof(ObsoleteAttribute), true).Any())
                    continue;  // Skip deprecated.

                var propType = prop.PropertyType;

                var isCollection = propType.IsGenericType &&
                                   propType.GetGenericTypeDefinition() == typeof(ICollection<>);

                if (isCollection)
                    propType = propType.GetGenericArguments().First();

                var propName = toLowerCamel(prop.Name);

                Assert.IsTrue(schema.Descendants().Select(x => x.Attribute("name"))
                    .Any(x => x != null && x.Value == propName),
                    "Property is missing in XML schema: " + propName);

                var isComplexProp = propType.Namespace != null && propType.Namespace.StartsWith("Apache.Ignite.Core");

                if (isComplexProp)
                    CheckPropertyIsPresentInSchema(propType, schema);
            }
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
        /// Tests the XML conversion.
        /// </summary>
        [Test]
        public void TestToXml()
        {
            // Empty config
            Assert.AreEqual("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<igniteConfiguration " +
                            "xmlns=\"http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection\" />",
                new IgniteConfiguration().ToXml());

            // Some properties
            var cfg = new IgniteConfiguration
            {
                IgniteInstanceName = "myGrid",
                ClientMode = true,
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("myCache")
                    {
                        CacheMode = CacheMode.Replicated,
                        QueryEntities = new[]
                        {
                            new QueryEntity(typeof(int)),
                            new QueryEntity(typeof(int), typeof(string))
                        }
                    }
                },
                IncludedEventTypes = new[]
                {
                    EventType.CacheEntryCreated,
                    EventType.CacheNodesLeft
                }
            };

            Assert.AreEqual(FixLineEndings(@"<?xml version=""1.0"" encoding=""utf-16""?>
<igniteConfiguration clientMode=""true"" igniteInstanceName=""myGrid"" xmlns=""http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection"">
  <cacheConfiguration>
    <cacheConfiguration cacheMode=""Replicated"" name=""myCache"">
      <queryEntities>
        <queryEntity valueType=""System.Int32"" valueTypeName=""java.lang.Integer"" />
        <queryEntity keyType=""System.Int32"" keyTypeName=""java.lang.Integer"" valueType=""System.String"" valueTypeName=""java.lang.String"" />
      </queryEntities>
    </cacheConfiguration>
  </cacheConfiguration>
  <includedEventTypes>
    <int>CacheEntryCreated</int>
    <int>CacheNodesLeft</int>
  </includedEventTypes>
</igniteConfiguration>"), cfg.ToXml());

            // Custom section name and indent
            var sb = new StringBuilder();

            var settings = new XmlWriterSettings
            {
                Indent = true,
                IndentChars = " "
            };

            using (var xmlWriter = XmlWriter.Create(sb, settings))
            {
                cfg.ToXml(xmlWriter, "igCfg");
            }

            Assert.AreEqual(FixLineEndings(@"<?xml version=""1.0"" encoding=""utf-16""?>
<igCfg clientMode=""true"" igniteInstanceName=""myGrid"" xmlns=""http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection"">
 <cacheConfiguration>
  <cacheConfiguration cacheMode=""Replicated"" name=""myCache"">
   <queryEntities>
    <queryEntity valueType=""System.Int32"" valueTypeName=""java.lang.Integer"" />
    <queryEntity keyType=""System.Int32"" keyTypeName=""java.lang.Integer"" valueType=""System.String"" valueTypeName=""java.lang.String"" />
   </queryEntities>
  </cacheConfiguration>
 </cacheConfiguration>
 <includedEventTypes>
  <int>CacheEntryCreated</int>
  <int>CacheNodesLeft</int>
 </includedEventTypes>
</igCfg>"), sb.ToString());
        }

        /// <summary>
        /// Tests the deserialization.
        /// </summary>
        [Test]
        public void TestFromXml()
        {
            // Empty section.
            var cfg = IgniteConfiguration.FromXml("<x />");
            AssertReflectionEqual(new IgniteConfiguration(), cfg);

            // Empty section with XML header.
            cfg = IgniteConfiguration.FromXml("<?xml version=\"1.0\" encoding=\"utf-16\"?><x />");
            AssertReflectionEqual(new IgniteConfiguration(), cfg);

            // Simple test.
            cfg = IgniteConfiguration.FromXml(@"<igCfg igniteInstanceName=""myGrid"" clientMode=""true"" />");
            AssertReflectionEqual(new IgniteConfiguration {IgniteInstanceName = "myGrid", ClientMode = true}, cfg);

            // Invalid xml.
            var ex = Assert.Throws<ConfigurationErrorsException>(() =>
                IgniteConfiguration.FromXml(@"<igCfg foo=""bar"" />"));

            Assert.AreEqual("Invalid IgniteConfiguration attribute 'foo=bar', there is no such property " +
                            "on 'Apache.Ignite.Core.IgniteConfiguration'", ex.Message);

            // Xml reader.
            using (var xmlReader = XmlReader.Create(
                new StringReader(@"<igCfg igniteInstanceName=""myGrid"" clientMode=""true"" />")))
            {
                cfg = IgniteConfiguration.FromXml(xmlReader);
            }
            AssertReflectionEqual(new IgniteConfiguration { IgniteInstanceName = "myGrid", ClientMode = true }, cfg);
        }

        /// <summary>
        /// Ensures windows-style \r\n line endings in a string literal.
        /// Git settings may cause string literals in both styles.
        /// </summary>
        private static string FixLineEndings(string s)
        {
            return s.Split('\n').Select(x => x.TrimEnd('\r'))
                .Aggregate((acc, x) => string.Format("{0}\r\n{1}", acc, x));
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
            var xml = cfg.ToXml();

            return IgniteConfiguration.FromXml(xml);
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
                else if (propType != typeof(string) && propType.IsGenericType &&
                         (propType.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                          propType.GetGenericTypeDefinition() == typeof(IDictionary<,>) ))
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
                IgniteInstanceName = "gridName",
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
                    IdMapper = new IdMapper(),
                    KeepDeserialized = true,
                    NameMapper = new NameMapper(),
                    Serializer = new TestSerializer()
                },
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("cacheName")
                    {
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        Backups = 15,
                        CacheMode = CacheMode.Replicated,
                        CacheStoreFactory = new TestCacheStoreFactory(),
                        CopyOnRead = false,
                        EagerTtl = false,
                        Invalidate = true,
                        KeepBinaryInStore = true,
                        LoadPreviousValue = true,
                        LockTimeout = TimeSpan.FromSeconds(56),
                        LongQueryWarningTimeout = TimeSpan.FromSeconds(99),
                        MaxConcurrentAsyncOperations = 24,
                        QueryEntities = new[]
                        {
                            new QueryEntity
                            {
                                Fields = new[]
                                {
                                    new QueryField("field", typeof (int)) { IsKeyField = true }
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
                                ValueType = typeof (long),
                                TableName = "table-1",
                                KeyFieldName = "k",
                                ValueFieldName = "v"
                            },
                        },
                        ReadFromBackup = false,
                        RebalanceBatchSize = 33,
                        RebalanceDelay = TimeSpan.MaxValue,
                        RebalanceMode = CacheRebalanceMode.Sync,
                        RebalanceThrottle = TimeSpan.FromHours(44),
                        RebalanceTimeout = TimeSpan.FromMinutes(8),
                        SqlEscapeAll = true,
                        WriteBehindBatchSize = 45,
                        WriteBehindEnabled = true,
                        WriteBehindFlushFrequency = TimeSpan.FromSeconds(55),
                        WriteBehindFlushSize = 66,
                        WriteBehindFlushThreadCount = 2,
                        WriteBehindCoalescing = false,
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
                        AffinityFunction = new RendezvousAffinityFunction
                        {
                            ExcludeNeighbors = true,
                            Partitions = 48
                        },
                        ExpiryPolicyFactory = new MyPolicyFactory(),
                        EnableStatistics = true,
                        PluginConfigurations = new[]
                        {
                            new MyPluginConfiguration()
                        },
                        MemoryPolicyName = "somePolicy",
                        PartitionLossPolicy = PartitionLossPolicy.ReadOnlyAll
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
                    IpFinderCleanFrequency = TimeSpan.FromMinutes(7),
                    LocalAddress = "127.0.0.1",
                    LocalPort = 49900,
                    LocalPortRange = 13,
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
                LifecycleHandlers = new[] {new LifecycleBean(), new LifecycleBean()},
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
                UserAttributes = Enumerable.Range(1, 10).ToDictionary(x => x.ToString(),
                    x => x%2 == 0 ? (object) x : new FooClass {Bar = x.ToString()}),
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
                },
                IsLateAffinityAssignment = false,
                SpringConfigUrl = "test",
                Logger = new IgniteNLogLogger(),
                FailureDetectionTimeout = TimeSpan.FromMinutes(2),
                ClientFailureDetectionTimeout = TimeSpan.FromMinutes(3),
                PluginConfigurations = new[] {new TestIgnitePluginConfiguration() },
                EventStorageSpi = new MemoryEventStorageSpi
                {
                    ExpirationTimeout = TimeSpan.FromMilliseconds(12345),
                    MaxEventCount = 257
                },
                MemoryConfiguration = new MemoryConfiguration
                {
                    ConcurrencyLevel = 3,
                    DefaultMemoryPolicyName = "somePolicy",
                    PageSize = 4,
                    SystemCacheInitialSize = 5,
                    SystemCacheMaxSize = 6,
                    MemoryPolicies = new[]
                    {
                        new MemoryPolicyConfiguration
                        {
                            Name = "myDefaultPlc",
                            PageEvictionMode = DataPageEvictionMode.Random2Lru,
                            InitialSize = 245 * 1024 * 1024,
                            MaxSize = 345 * 1024 * 1024,
                            EvictionThreshold = 0.88,
                            EmptyPagesPoolSize = 77,
                            SwapFilePath = "myPath1",
                            RateTimeInterval = TimeSpan.FromSeconds(22),
                            SubIntervals = 99
                        },
                        new MemoryPolicyConfiguration
                        {
                            Name = "customPlc",
                            PageEvictionMode = DataPageEvictionMode.RandomLru,
                            EvictionThreshold = 0.77,
                            EmptyPagesPoolSize = 66,
                            SwapFilePath = "somePath2",
                            MetricsEnabled = true
                        }
                    }
                },
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain
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
        public class LifecycleBean : ILifecycleHandler
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
            /** <inheritdoc /> */
            public void WriteBinary(object obj, IBinaryWriter writer)
            {
                // No-op.
            }

            /** <inheritdoc /> */
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
            public string Bar { get; set; }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return string.Equals(Bar, ((FooClass) obj).Bar);
            }

            public override int GetHashCode()
            {
                return Bar != null ? Bar.GetHashCode() : 0;
            }

            public static bool operator ==(FooClass left, FooClass right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(FooClass left, FooClass right)
            {
                return !Equals(left, right);
            }
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

        /// <summary>
        /// Test logger.
        /// </summary>
        public class TestLogger : ILogger
        {
            /** <inheritdoc /> */
            public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
                string nativeErrorInfo, Exception ex)
            {
                throw new NotImplementedException();
            }

            /** <inheritdoc /> */
            public bool IsEnabled(LogLevel level)
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Test factory.
        /// </summary>
        public class MyPolicyFactory : IFactory<IExpiryPolicy>
        {
            /** <inheritdoc /> */
            public IExpiryPolicy CreateInstance()
            {
                throw new NotImplementedException();
            }
        }

        public class MyPluginConfiguration : ICachePluginConfiguration
        {
            int? ICachePluginConfiguration.CachePluginConfigurationClosureFactoryId
            {
                get { return 0; }
            }

            void ICachePluginConfiguration.WriteBinary(IBinaryRawWriter writer)
            {
                throw new NotImplementedException();
            }
        }
    }
}

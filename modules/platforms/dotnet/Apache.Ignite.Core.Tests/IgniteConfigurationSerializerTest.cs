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
#pragma warning disable 618
namespace Apache.Ignite.Core.Tests
{
    using System;
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
    using Apache.Ignite.Core.Ssl;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Failure;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Plugin.Cache;
    using Apache.Ignite.Core.Tests.Binary;
    using Apache.Ignite.Core.Tests.Plugin;
    using Apache.Ignite.Core.Transactions;
    using Apache.Ignite.NLog;
    using NUnit.Framework;
    using CheckpointWriteOrder = Apache.Ignite.Core.PersistentStore.CheckpointWriteOrder;
    using DataPageEvictionMode = Apache.Ignite.Core.Cache.Configuration.DataPageEvictionMode;
    using WalMode = Apache.Ignite.Core.PersistentStore.WalMode;

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
            var xml = File.ReadAllText("Config\\full-config.xml");

            var cfg = IgniteConfiguration.FromXml(xml);

            Assert.AreEqual("c:", cfg.WorkDirectory);
            Assert.AreEqual("127.1.1.1", cfg.Localhost);
            Assert.IsTrue(cfg.IsDaemon);
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
            Assert.AreEqual(new TimeSpan(1, 2, 3), cfg.LongQueryWarningTimeout);
            Assert.IsFalse(cfg.IsActiveOnStart);
            Assert.IsTrue(cfg.AuthenticationEnabled);

            Assert.IsNotNull(cfg.SqlSchemas);
            Assert.AreEqual(2, cfg.SqlSchemas.Count);
            Assert.IsTrue(cfg.SqlSchemas.Contains("SCHEMA_1"));
            Assert.IsTrue(cfg.SqlSchemas.Contains("schema_2"));

            Assert.AreEqual("someId012", cfg.ConsistentId);
            Assert.IsFalse(cfg.RedirectJavaConsoleOutput);

            Assert.AreEqual("secondCache", cfg.CacheConfiguration.Last().Name);

            var cacheCfg = cfg.CacheConfiguration.First();

            Assert.AreEqual(CacheMode.Replicated, cacheCfg.CacheMode);
            Assert.IsTrue(cacheCfg.ReadThrough);
            Assert.IsTrue(cacheCfg.WriteThrough);
            Assert.IsInstanceOf<MyPolicyFactory>(cacheCfg.ExpiryPolicyFactory);
            Assert.IsTrue(cacheCfg.EnableStatistics);
            Assert.IsFalse(cacheCfg.WriteBehindCoalescing);
            Assert.AreEqual(PartitionLossPolicy.ReadWriteAll, cacheCfg.PartitionLossPolicy);
            Assert.AreEqual("fooGroup", cacheCfg.GroupName);
            
            Assert.AreEqual("bar", cacheCfg.KeyConfiguration.Single().AffinityKeyFieldName);
            Assert.AreEqual("foo", cacheCfg.KeyConfiguration.Single().TypeName);

            Assert.IsTrue(cacheCfg.OnheapCacheEnabled);
            Assert.AreEqual(8, cacheCfg.StoreConcurrentLoadAllThreshold);
            Assert.AreEqual(9, cacheCfg.RebalanceOrder);
            Assert.AreEqual(10, cacheCfg.RebalanceBatchesPrefetchCount);
            Assert.AreEqual(11, cacheCfg.MaxQueryIteratorsCount);
            Assert.AreEqual(12, cacheCfg.QueryDetailMetricsSize);
            Assert.AreEqual(13, cacheCfg.QueryParallelism);
            Assert.AreEqual("mySchema", cacheCfg.SqlSchema);

            var queryEntity = cacheCfg.QueryEntities.Single();
            Assert.AreEqual(typeof(int), queryEntity.KeyType);
            Assert.AreEqual(typeof(string), queryEntity.ValueType);
            Assert.AreEqual("myTable", queryEntity.TableName);
            Assert.AreEqual("length", queryEntity.Fields.Single().Name);
            Assert.AreEqual(typeof(int), queryEntity.Fields.Single().FieldType);
            Assert.IsTrue(queryEntity.Fields.Single().IsKeyField);
            Assert.IsTrue(queryEntity.Fields.Single().NotNull);
            Assert.AreEqual(3.456d, (double)queryEntity.Fields.Single().DefaultValue);
            Assert.AreEqual("somefield.field", queryEntity.Aliases.Single().FullName);
            Assert.AreEqual("shortField", queryEntity.Aliases.Single().Alias);

            var queryIndex = queryEntity.Indexes.Single();
            Assert.AreEqual(QueryIndexType.Geospatial, queryIndex.IndexType);
            Assert.AreEqual("indexFld", queryIndex.Fields.Single().Name);
            Assert.AreEqual(true, queryIndex.Fields.Single().IsDescending);
            Assert.AreEqual(123, queryIndex.InlineSize);

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

            var sql = cfg.SqlConnectorConfiguration;
            Assert.IsNotNull(sql);
            Assert.AreEqual("bar", sql.Host);
            Assert.AreEqual(10, sql.Port);
            Assert.AreEqual(11, sql.PortRange);
            Assert.AreEqual(12, sql.SocketSendBufferSize);
            Assert.AreEqual(13, sql.SocketReceiveBufferSize);
            Assert.IsTrue(sql.TcpNoDelay);
            Assert.AreEqual(14, sql.MaxOpenCursorsPerConnection);
            Assert.AreEqual(15, sql.ThreadPoolSize);

            var client = cfg.ClientConnectorConfiguration;
            Assert.IsNotNull(client);
            Assert.AreEqual("bar", client.Host);
            Assert.AreEqual(10, client.Port);
            Assert.AreEqual(11, client.PortRange);
            Assert.AreEqual(12, client.SocketSendBufferSize);
            Assert.AreEqual(13, client.SocketReceiveBufferSize);
            Assert.IsTrue(client.TcpNoDelay);
            Assert.AreEqual(14, client.MaxOpenCursorsPerConnection);
            Assert.AreEqual(15, client.ThreadPoolSize);
            Assert.AreEqual(19, client.IdleTimeout.TotalSeconds);

            var pers = cfg.PersistentStoreConfiguration;

            Assert.AreEqual(true, pers.AlwaysWriteFullPages);
            Assert.AreEqual(TimeSpan.FromSeconds(1), pers.CheckpointingFrequency);
            Assert.AreEqual(2, pers.CheckpointingPageBufferSize);
            Assert.AreEqual(3, pers.CheckpointingThreads);
            Assert.AreEqual(TimeSpan.FromSeconds(4), pers.LockWaitTime);
            Assert.AreEqual("foo", pers.PersistentStorePath);
            Assert.AreEqual(5, pers.TlbSize);
            Assert.AreEqual("bar", pers.WalArchivePath);
            Assert.AreEqual(TimeSpan.FromSeconds(6), pers.WalFlushFrequency);
            Assert.AreEqual(7, pers.WalFsyncDelayNanos);
            Assert.AreEqual(8, pers.WalHistorySize);
            Assert.AreEqual(WalMode.None, pers.WalMode);
            Assert.AreEqual(9, pers.WalRecordIteratorBufferSize);
            Assert.AreEqual(10, pers.WalSegments);
            Assert.AreEqual(11, pers.WalSegmentSize);
            Assert.AreEqual("baz", pers.WalStorePath);
            Assert.IsTrue(pers.MetricsEnabled);
            Assert.AreEqual(3, pers.SubIntervals);
            Assert.AreEqual(TimeSpan.FromSeconds(6), pers.RateTimeInterval);
            Assert.AreEqual(CheckpointWriteOrder.Random, pers.CheckpointWriteOrder);
            Assert.IsTrue(pers.WriteThrottlingEnabled);

            var listeners = cfg.LocalEventListeners;
            Assert.AreEqual(2, listeners.Count);

            var rebalListener = (LocalEventListener<CacheRebalancingEvent>) listeners.First();
            Assert.AreEqual(new[] {EventType.CacheObjectPut, 81}, rebalListener.EventTypes);
            Assert.AreEqual("Apache.Ignite.Core.Tests.EventsTestLocalListeners+Listener`1" +
                            "[Apache.Ignite.Core.Events.CacheRebalancingEvent]",
                rebalListener.Listener.GetType().ToString());

            var ds = cfg.DataStorageConfiguration;
            Assert.IsFalse(ds.AlwaysWriteFullPages);
            Assert.AreEqual(TimeSpan.FromSeconds(1), ds.CheckpointFrequency);
            Assert.AreEqual(3, ds.CheckpointThreads);
            Assert.AreEqual(4, ds.ConcurrencyLevel);
            Assert.AreEqual(TimeSpan.FromSeconds(5), ds.LockWaitTime);
            Assert.IsTrue(ds.MetricsEnabled);
            Assert.AreEqual(6, ds.PageSize);
            Assert.AreEqual("cde", ds.StoragePath);
            Assert.AreEqual(TimeSpan.FromSeconds(7), ds.MetricsRateTimeInterval);
            Assert.AreEqual(8, ds.MetricsSubIntervalCount);
            Assert.AreEqual(9, ds.SystemRegionInitialSize);
            Assert.AreEqual(10, ds.SystemRegionMaxSize);
            Assert.AreEqual(11, ds.WalThreadLocalBufferSize);
            Assert.AreEqual("abc", ds.WalArchivePath);
            Assert.AreEqual(TimeSpan.FromSeconds(12), ds.WalFlushFrequency);
            Assert.AreEqual(13, ds.WalFsyncDelayNanos);
            Assert.AreEqual(14, ds.WalHistorySize);
            Assert.AreEqual(Core.Configuration.WalMode.Background, ds.WalMode);
            Assert.AreEqual(15, ds.WalRecordIteratorBufferSize);
            Assert.AreEqual(16, ds.WalSegments);
            Assert.AreEqual(17, ds.WalSegmentSize);
            Assert.AreEqual("wal-store", ds.WalPath);
            Assert.AreEqual(TimeSpan.FromSeconds(18), ds.WalAutoArchiveAfterInactivity);
            Assert.IsTrue(ds.WriteThrottlingEnabled);

            var dr = ds.DataRegionConfigurations.Single();
            Assert.AreEqual(1, dr.EmptyPagesPoolSize);
            Assert.AreEqual(2, dr.EvictionThreshold);
            Assert.AreEqual(3, dr.InitialSize);
            Assert.AreEqual(4, dr.MaxSize);
            Assert.AreEqual("reg2", dr.Name);
            Assert.AreEqual(Core.Configuration.DataPageEvictionMode.RandomLru, dr.PageEvictionMode);
            Assert.AreEqual(TimeSpan.FromSeconds(1), dr.MetricsRateTimeInterval);
            Assert.AreEqual(5, dr.MetricsSubIntervalCount);
            Assert.AreEqual("swap", dr.SwapPath);
            Assert.IsTrue(dr.MetricsEnabled);
            Assert.AreEqual(7, dr.CheckpointPageBufferSize);

            dr = ds.DefaultDataRegionConfiguration;
            Assert.AreEqual(2, dr.EmptyPagesPoolSize);
            Assert.AreEqual(3, dr.EvictionThreshold);
            Assert.AreEqual(4, dr.InitialSize);
            Assert.AreEqual(5, dr.MaxSize);
            Assert.AreEqual("reg1", dr.Name);
            Assert.AreEqual(Core.Configuration.DataPageEvictionMode.Disabled, dr.PageEvictionMode);
            Assert.AreEqual(TimeSpan.FromSeconds(3), dr.MetricsRateTimeInterval);
            Assert.AreEqual(6, dr.MetricsSubIntervalCount);
            Assert.AreEqual("swap2", dr.SwapPath);
            Assert.IsFalse(dr.MetricsEnabled);

            Assert.IsInstanceOf<SslContextFactory>(cfg.SslContextFactory);
            
            Assert.IsInstanceOf<StopNodeOrHaltFailureHandler>(cfg.FailureHandler);

            var failureHandler = (StopNodeOrHaltFailureHandler)cfg.FailureHandler;
            
            Assert.IsTrue(failureHandler.TryStop);  
            Assert.AreEqual(TimeSpan.Parse("0:1:0"), failureHandler.Timeout);
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
        public void TestAllPropertiesArePresentInSchema()
        {
            CheckAllPropertiesArePresentInSchema("IgniteConfigurationSection.xsd", "igniteConfiguration", 
                typeof(IgniteConfiguration));
        }

        /// <summary>
        /// Checks that all properties are present in schema.
        /// </summary>
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        public static void CheckAllPropertiesArePresentInSchema(string xsd, string sectionName, Type type)
        {
            var schema = XDocument.Load(xsd)
                .Root.Elements()
                .Single(x => x.Attribute("name").Value == sectionName);


            CheckPropertyIsPresentInSchema(type, schema);
        }

        /// <summary>
        /// Checks the property is present in schema.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
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
            AssertExtensions.ReflectionEqual(new IgniteConfiguration(), cfg);

            // Empty section with XML header.
            cfg = IgniteConfiguration.FromXml("<?xml version=\"1.0\" encoding=\"utf-16\"?><x />");
            AssertExtensions.ReflectionEqual(new IgniteConfiguration(), cfg);

            // Simple test.
            cfg = IgniteConfiguration.FromXml(@"<igCfg igniteInstanceName=""myGrid"" clientMode=""true"" />");
            AssertExtensions.ReflectionEqual(new IgniteConfiguration {IgniteInstanceName = "myGrid", ClientMode = true}, cfg);

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
            AssertExtensions.ReflectionEqual(new IgniteConfiguration { IgniteInstanceName = "myGrid", ClientMode = true }, cfg);
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
            CheckSchemaValidation(GetTestConfig().ToXml());
        }

        /// <summary>
        /// Checks the schema validation.
        /// </summary>
        private static void CheckSchemaValidation(string xml)
        {
            var xmlns = "http://ignite.apache.org/schema/dotnet/IgniteConfigurationSection";
            var schemaFile = "IgniteConfigurationSection.xsd";

            CheckSchemaValidation(xml, xmlns, schemaFile);
        }

        /// <summary>
        /// Checks the schema validation.
        /// </summary>
        public static void CheckSchemaValidation(string xml, string xmlns, string schemaFile)
        {
            var document = new XmlDocument();
            document.Schemas.Add(xmlns, XmlReader.Create(schemaFile));
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

            AssertExtensions.ReflectionEqual(cfg, resCfg);
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
                    Types = new[] {typeof(string).FullName},
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
                        MaxConcurrentAsyncOperations = 24,
                        QueryEntities = new[]
                        {
                            new QueryEntity
                            {
                                Fields = new[]
                                {
                                    new QueryField("field", typeof(int))
                                    {
                                        IsKeyField = true,
                                        NotNull = true,
                                        DefaultValue = "foo"
                                    }
                                },
                                Indexes = new[]
                                {
                                    new QueryIndex("field")
                                    {
                                        IndexType = QueryIndexType.FullText,
                                        InlineSize = 32
                                    }
                                },
                                Aliases = new[]
                                {
                                    new QueryAlias("field.field", "fld")
                                },
                                KeyType = typeof(string),
                                ValueType = typeof(long),
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
                                BatchSize = 19,
                                MaxMemorySize = 1024,
                                MaxSize = 555
                            }
                        },
                        EvictionPolicy = new LruEvictionPolicy
                        {
                            BatchSize = 18,
                            MaxMemorySize = 1023,
                            MaxSize = 554
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
                        PartitionLossPolicy = PartitionLossPolicy.ReadOnlyAll,
                        GroupName = "abc",
                        SqlIndexMaxInlineSize = 24,
                        KeyConfiguration = new[]
                        {
                            new CacheKeyConfiguration
                            {
                                AffinityKeyFieldName = "abc",
                                TypeName = "def"
                            }, 
                        },
                        OnheapCacheEnabled = true,
                        StoreConcurrentLoadAllThreshold = 7,
                        RebalanceOrder = 3,
                        RebalanceBatchesPrefetchCount = 4,
                        MaxQueryIteratorsCount = 512,
                        QueryDetailMetricsSize = 100,
                        QueryParallelism = 16,
                        SqlSchema = "foo"
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
                    x => x % 2 == 0 ? (object) x : new FooClass {Bar = x.ToString()}),
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
                SpringConfigUrl = "test",
                Logger = new IgniteNLogLogger(),
                FailureDetectionTimeout = TimeSpan.FromMinutes(2),
                ClientFailureDetectionTimeout = TimeSpan.FromMinutes(3),
                LongQueryWarningTimeout = TimeSpan.FromDays(4),
                PluginConfigurations = new[] {new TestIgnitePluginConfiguration()},
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
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain,
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    Host = "foo",
                    Port = 2,
                    PortRange = 3,
                    MaxOpenCursorsPerConnection = 4,
                    SocketReceiveBufferSize = 5,
                    SocketSendBufferSize = 6,
                    TcpNoDelay = false,
                    ThinClientEnabled = false,
                    OdbcEnabled = false,
                    JdbcEnabled = false,
                    ThreadPoolSize = 7,
                    IdleTimeout = TimeSpan.FromMinutes(5)
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
                    WalMode = WalMode.Background,
                    WalRecordIteratorBufferSize = 32 * 1024 * 1024,
                    WalSegments = 6,
                    WalSegmentSize = 5 * 1024 * 1024,
                    WalStorePath = Path.GetTempPath(),
                    SubIntervals = 25,
                    MetricsEnabled = true,
                    RateTimeInterval = TimeSpan.FromDays(1),
                    CheckpointWriteOrder = CheckpointWriteOrder.Random,
                    WriteThrottlingEnabled = true
                },
                IsActiveOnStart = false,
                ConsistentId = "myId123",
                LocalEventListeners = new[]
                {
                    new LocalEventListener<IEvent>
                    {
                        EventTypes = new[] {1, 2},
                        Listener = new MyEventListener()
                    }
                },
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    AlwaysWriteFullPages = true,
                    CheckpointFrequency = TimeSpan.FromSeconds(25),
                    CheckpointThreads = 2,
                    LockWaitTime = TimeSpan.FromSeconds(5),
                    StoragePath = Path.GetTempPath(),
                    WalThreadLocalBufferSize = 64 * 1024,
                    WalArchivePath = Path.GetTempPath(),
                    WalFlushFrequency = TimeSpan.FromSeconds(3),
                    WalFsyncDelayNanos = 3,
                    WalHistorySize = 10,
                    WalMode = Core.Configuration.WalMode.None,
                    WalRecordIteratorBufferSize = 32 * 1024 * 1024,
                    WalSegments = 6,
                    WalSegmentSize = 5 * 1024 * 1024,
                    WalPath = Path.GetTempPath(),
                    MetricsEnabled = true,
                    MetricsSubIntervalCount = 7,
                    MetricsRateTimeInterval = TimeSpan.FromSeconds(9),
                    CheckpointWriteOrder = Core.Configuration.CheckpointWriteOrder.Sequential,
                    WriteThrottlingEnabled = true,
                    SystemRegionInitialSize = 64 * 1024 * 1024,
                    SystemRegionMaxSize = 128 * 1024 * 1024,
                    ConcurrencyLevel = 1,
                    PageSize = 5 * 1024,
                    WalAutoArchiveAfterInactivity = TimeSpan.FromSeconds(19),
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "reg1",
                        EmptyPagesPoolSize = 50,
                        EvictionThreshold = 0.8,
                        InitialSize = 100 * 1024 * 1024,
                        MaxSize = 150 * 1024 * 1024,
                        MetricsEnabled = true,
                        PageEvictionMode = Core.Configuration.DataPageEvictionMode.RandomLru,
                        PersistenceEnabled = false,
                        MetricsRateTimeInterval = TimeSpan.FromMinutes(2),
                        MetricsSubIntervalCount = 6,
                        SwapPath = Path.GetTempPath(),
                        CheckpointPageBufferSize = 7
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "reg2",
                            EmptyPagesPoolSize = 51,
                            EvictionThreshold = 0.7,
                            InitialSize = 101 * 1024 * 1024,
                            MaxSize = 151 * 1024 * 1024,
                            MetricsEnabled = false,
                            PageEvictionMode = Core.Configuration.DataPageEvictionMode.RandomLru,
                            PersistenceEnabled = false,
                            MetricsRateTimeInterval = TimeSpan.FromMinutes(3),
                            MetricsSubIntervalCount = 7,
                            SwapPath = Path.GetTempPath()
                        }
                    }
                },
                SslContextFactory = new SslContextFactory(),
                FailureHandler = new StopNodeOrHaltFailureHandler()
                {
                    TryStop = false,
                    Timeout = TimeSpan.FromSeconds(10)
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

        public class MyEventListener : IEventListener<IEvent>
        {
            public bool Invoke(IEvent evt)
            {
                throw new NotImplementedException();
            }
        }
    }
}

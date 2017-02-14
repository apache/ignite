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

 namespace Apache.Ignite.Core
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Xml;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Communication;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Discovery;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.SwapSpace;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.SwapSpace;
    using Apache.Ignite.Core.Transactions;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Grid configuration.
    /// </summary>
    public class IgniteConfiguration
    {
        /// <summary>
        /// Default initial JVM memory in megabytes.
        /// </summary>
        public const int DefaultJvmInitMem = -1;

        /// <summary>
        /// Default maximum JVM memory in megabytes.
        /// </summary>
        public const int DefaultJvmMaxMem = -1;

        /// <summary>
        /// Default metrics expire time.
        /// </summary>
        public static readonly TimeSpan DefaultMetricsExpireTime = TimeSpan.MaxValue;

        /// <summary>
        /// Default metrics history size.
        /// </summary>
        public const int DefaultMetricsHistorySize = 10000;

        /// <summary>
        /// Default metrics log frequency.
        /// </summary>
        public static readonly TimeSpan DefaultMetricsLogFrequency = TimeSpan.FromMilliseconds(60000);

        /// <summary>
        /// Default metrics update frequency.
        /// </summary>
        public static readonly TimeSpan DefaultMetricsUpdateFrequency = TimeSpan.FromMilliseconds(2000);

        /// <summary>
        /// Default network timeout.
        /// </summary>
        public static readonly TimeSpan DefaultNetworkTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default network retry delay.
        /// </summary>
        public static readonly TimeSpan DefaultNetworkSendRetryDelay = TimeSpan.FromMilliseconds(1000);

        /// <summary>
        /// Default failure detection timeout.
        /// </summary>
        public static readonly TimeSpan DefaultFailureDetectionTimeout = TimeSpan.FromSeconds(10);

        /** */
        private TimeSpan? _metricsExpireTime;

        /** */
        private int? _metricsHistorySize;

        /** */
        private TimeSpan? _metricsLogFrequency;

        /** */
        private TimeSpan? _metricsUpdateFrequency;

        /** */
        private int? _networkSendRetryCount;

        /** */
        private TimeSpan? _networkSendRetryDelay;

        /** */
        private TimeSpan? _networkTimeout;

        /** */
        private bool? _isDaemon;

        /** */
        private bool? _isLateAffinityAssignment;

        /** */
        private bool? _clientMode;

        /** */
        private TimeSpan? _failureDetectionTimeout;

        /// <summary>
        /// Default network retry count.
        /// </summary>
        public const int DefaultNetworkSendRetryCount = 3;

        /// <summary>
        /// Default late affinity assignment mode.
        /// </summary>
        public const bool DefaultIsLateAffinityAssignment = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class.
        /// </summary>
        public IgniteConfiguration()
        {
            JvmInitialMemoryMb = DefaultJvmInitMem;
            JvmMaxMemoryMb = DefaultJvmMaxMem;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class.
        /// </summary>
        /// <param name="configuration">The configuration to copy.</param>
        public IgniteConfiguration(IgniteConfiguration configuration)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var marsh = new Marshaller(configuration.BinaryConfiguration);

                configuration.Write(marsh.StartMarshal(stream));

                stream.SynchronizeOutput();

                stream.Seek(0, SeekOrigin.Begin);

                ReadCore(marsh.StartUnmarshal(stream));
            }

            CopyLocalProperties(configuration);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class from a reader.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        internal IgniteConfiguration(BinaryReader binaryReader)
        {
            Read(binaryReader);
        }

        /// <summary>
        /// Writes this instance to a writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(BinaryWriter writer)
        {
            Debug.Assert(writer != null);

            // Simple properties
            writer.WriteBooleanNullable(_clientMode);
            writer.WriteIntArray(IncludedEventTypes == null ? null : IncludedEventTypes.ToArray());

            writer.WriteTimeSpanAsLongNullable(_metricsExpireTime);
            writer.WriteIntNullable(_metricsHistorySize);
            writer.WriteTimeSpanAsLongNullable(_metricsLogFrequency);
            writer.WriteTimeSpanAsLongNullable(_metricsUpdateFrequency);
            writer.WriteIntNullable(_networkSendRetryCount);
            writer.WriteTimeSpanAsLongNullable(_networkSendRetryDelay);
            writer.WriteTimeSpanAsLongNullable(_networkTimeout);
            writer.WriteString(WorkDirectory);
            writer.WriteString(Localhost);
            writer.WriteBooleanNullable(_isDaemon);
            writer.WriteBooleanNullable(_isLateAffinityAssignment);
            writer.WriteTimeSpanAsLongNullable(_failureDetectionTimeout);

            // Cache config
            var caches = CacheConfiguration;

            if (caches == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(caches.Count);

                foreach (var cache in caches)
                    cache.Write(writer);
            }

            // Discovery config
            var disco = DiscoverySpi;

            if (disco != null)
            {
                writer.WriteBoolean(true);

                var tcpDisco = disco as TcpDiscoverySpi;

                if (tcpDisco == null)
                    throw new InvalidOperationException("Unsupported discovery SPI: " + disco.GetType());

                tcpDisco.Write(writer);
            }
            else
                writer.WriteBoolean(false);

            // Communication config
            var comm = CommunicationSpi;

            if (comm != null)
            {
                writer.WriteBoolean(true);

                var tcpComm = comm as TcpCommunicationSpi;

                if (tcpComm == null)
                    throw new InvalidOperationException("Unsupported communication SPI: " + comm.GetType());

                tcpComm.Write(writer);
            }
            else
                writer.WriteBoolean(false);

            // Binary config
            if (BinaryConfiguration != null)
            {
                writer.WriteBoolean(true);

                if (BinaryConfiguration.CompactFooterInternal != null)
                {
                    writer.WriteBoolean(true);
                    writer.WriteBoolean(BinaryConfiguration.CompactFooter);
                }
                else
                {
                    writer.WriteBoolean(false);
                }

                // Send only descriptors with non-null EqualityComparer to preserve old behavior where
                // remote nodes can have no BinaryConfiguration.
                var types = writer.Marshaller.GetUserTypeDescriptors().Where(x => x.EqualityComparer != null).ToList();

                writer.WriteInt(types.Count);

                foreach (var type in types)
                {
                    writer.WriteString(BinaryUtils.SimpleTypeName(type.TypeName));
                    writer.WriteBoolean(type.IsEnum);
                    BinaryEqualityComparerSerializer.Write(writer, type.EqualityComparer);
                }
            }
            else
            {
                writer.WriteBoolean(false);
            }

            // User attributes
            var attrs = UserAttributes;

            if (attrs == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(attrs.Count);

                foreach (var pair in attrs)
                {
                    writer.WriteString(pair.Key);
                    writer.Write(pair.Value);
                }
            }

            // Atomic
            if (AtomicConfiguration != null)
            {
                writer.WriteBoolean(true);

                writer.WriteInt(AtomicConfiguration.AtomicSequenceReserveSize);
                writer.WriteInt(AtomicConfiguration.Backups);
                writer.WriteInt((int) AtomicConfiguration.CacheMode);
            }
            else
                writer.WriteBoolean(false);

            // Tx
            if (TransactionConfiguration != null)
            {
                writer.WriteBoolean(true);

                writer.WriteInt(TransactionConfiguration.PessimisticTransactionLogSize);
                writer.WriteInt((int) TransactionConfiguration.DefaultTransactionConcurrency);
                writer.WriteInt((int) TransactionConfiguration.DefaultTransactionIsolation);
                writer.WriteLong((long) TransactionConfiguration.DefaultTimeout.TotalMilliseconds);
                writer.WriteInt((int) TransactionConfiguration.PessimisticTransactionLogLinger.TotalMilliseconds);
            }
            else
                writer.WriteBoolean(false);

            // Swap space
            SwapSpaceSerializer.Write(writer, SwapSpaceSpi);
        }

        /// <summary>
        /// Validates this instance and outputs information to the log, if necessary.
        /// </summary>
        internal void Validate(ILogger log)
        {
            Debug.Assert(log != null);

            var ccfg = CacheConfiguration;
            if (ccfg != null)
            {
                foreach (var cfg in ccfg)
                    cfg.Validate(log);
            }
        }

        /// <summary>
        /// Reads data from specified reader into current instance.
        /// </summary>
        /// <param name="r">The binary reader.</param>
        private void ReadCore(BinaryReader r)
        {
            // Simple properties
            _clientMode = r.ReadBooleanNullable();
            IncludedEventTypes = r.ReadIntArray();
            _metricsExpireTime = r.ReadTimeSpanNullable();
            _metricsHistorySize = r.ReadIntNullable();
            _metricsLogFrequency = r.ReadTimeSpanNullable();
            _metricsUpdateFrequency = r.ReadTimeSpanNullable();
            _networkSendRetryCount = r.ReadIntNullable();
            _networkSendRetryDelay = r.ReadTimeSpanNullable();
            _networkTimeout = r.ReadTimeSpanNullable();
            WorkDirectory = r.ReadString();
            Localhost = r.ReadString();
            _isDaemon = r.ReadBooleanNullable();
            _isLateAffinityAssignment = r.ReadBooleanNullable();
            _failureDetectionTimeout = r.ReadTimeSpanNullable();

            // Cache config
            var cacheCfgCount = r.ReadInt();
            CacheConfiguration = new List<CacheConfiguration>(cacheCfgCount);
            for (int i = 0; i < cacheCfgCount; i++)
                CacheConfiguration.Add(new CacheConfiguration(r));

            // Discovery config
            DiscoverySpi = r.ReadBoolean() ? new TcpDiscoverySpi(r) : null;

            // Communication config
            CommunicationSpi = r.ReadBoolean() ? new TcpCommunicationSpi(r) : null;

            // Binary config
            if (r.ReadBoolean())
            {
                BinaryConfiguration = BinaryConfiguration ?? new BinaryConfiguration();

                if (r.ReadBoolean())
                    BinaryConfiguration.CompactFooter = r.ReadBoolean();

                var typeCount = r.ReadInt();

                if (typeCount > 0)
                {
                    var types = new List<BinaryTypeConfiguration>(typeCount);

                    for (var i = 0; i < typeCount; i++)
                    {
                        types.Add(new BinaryTypeConfiguration
                        {
                            TypeName = r.ReadString(),
                            IsEnum = r.ReadBoolean(),
                            EqualityComparer = BinaryEqualityComparerSerializer.Read(r)
                        });
                    }

                    BinaryConfiguration.TypeConfigurations = types;
                }
            }

            // User attributes
            UserAttributes = Enumerable.Range(0, r.ReadInt())
                .ToDictionary(x => r.ReadString(), x => r.ReadObject<object>());

            // Atomic
            if (r.ReadBoolean())
            {
                AtomicConfiguration = new AtomicConfiguration
                {
                    AtomicSequenceReserveSize = r.ReadInt(),
                    Backups = r.ReadInt(),
                    CacheMode = (CacheMode) r.ReadInt()
                };
            }

            // Tx
            if (r.ReadBoolean())
            {
                TransactionConfiguration = new TransactionConfiguration
                {
                    PessimisticTransactionLogSize = r.ReadInt(),
                    DefaultTransactionConcurrency = (TransactionConcurrency) r.ReadInt(),
                    DefaultTransactionIsolation = (TransactionIsolation) r.ReadInt(),
                    DefaultTimeout = TimeSpan.FromMilliseconds(r.ReadLong()),
                    PessimisticTransactionLogLinger = TimeSpan.FromMilliseconds(r.ReadInt())
                };
            }

            // Swap
            SwapSpaceSpi = SwapSpaceSerializer.Read(r);
        }

        /// <summary>
        /// Reads data from specified reader into current instance.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        private void Read(BinaryReader binaryReader)
        {
            ReadCore(binaryReader);

            CopyLocalProperties(binaryReader.Marshaller.Ignite.Configuration);

            // Misc
            IgniteHome = binaryReader.ReadString();

            JvmInitialMemoryMb = (int) (binaryReader.ReadLong()/1024/2014);
            JvmMaxMemoryMb = (int) (binaryReader.ReadLong()/1024/2014);

            // Local data (not from reader)
            JvmDllPath = Process.GetCurrentProcess().Modules.OfType<ProcessModule>()
                .Single(x => string.Equals(x.ModuleName, IgniteUtils.FileJvmDll, StringComparison.OrdinalIgnoreCase))
                .FileName;
        }

        /// <summary>
        /// Copies the local properties (properties that are not written in Write method).
        /// </summary>
        private void CopyLocalProperties(IgniteConfiguration cfg)
        {
            GridName = cfg.GridName;

            if (BinaryConfiguration != null && cfg.BinaryConfiguration != null)
            {
                BinaryConfiguration.MergeTypes(cfg.BinaryConfiguration);
            }
            else if (cfg.BinaryConfiguration != null)
            {
                BinaryConfiguration = new BinaryConfiguration(cfg.BinaryConfiguration);
            }
            
            JvmClasspath = cfg.JvmClasspath;
            JvmOptions = cfg.JvmOptions;
            Assemblies = cfg.Assemblies;
            SuppressWarnings = cfg.SuppressWarnings;
            LifecycleBeans = cfg.LifecycleBeans;
            Logger = cfg.Logger;
            JvmInitialMemoryMb = cfg.JvmInitialMemoryMb;
            JvmMaxMemoryMb = cfg.JvmMaxMemoryMb;
        }

        /// <summary>
        /// Grid name which is used if not provided in configuration file.
        /// </summary>
        public string GridName { get; set; }

        /// <summary>
        /// Gets or sets the binary configuration.
        /// </summary>
        /// <value>
        /// The binary configuration.
        /// </value>
        public BinaryConfiguration BinaryConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the cache configuration.
        /// </summary>
        /// <value>
        /// The cache configuration.
        /// </value>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<CacheConfiguration> CacheConfiguration { get; set; }

        /// <summary>
        /// URL to Spring configuration file.
        /// <para />
        /// Spring configuration is loaded first, then <see cref="IgniteConfiguration"/> properties are applied.
        /// Null property values do not override Spring values.
        /// Value-typed properties are tracked internally: if setter was not called, Spring value won't be overwritten.
        /// <para />
        /// This merging happens on the top level only; e. g. if there are cache configurations defined in Spring 
        /// and in .NET, .NET caches will overwrite Spring caches.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1056:UriPropertiesShouldNotBeStrings")]
        public string SpringConfigUrl { get; set; }

        /// <summary>
        /// Path jvm.dll file. If not set, it's location will be determined
        /// using JAVA_HOME environment variable.
        /// If path is neither set nor determined automatically, an exception
        /// will be thrown.
        /// </summary>
        public string JvmDllPath { get; set; }

        /// <summary>
        /// Path to Ignite home. If not set environment variable IGNITE_HOME will be used.
        /// </summary>
        public string IgniteHome { get; set; }

        /// <summary>
        /// Classpath used by JVM on Ignite start.
        /// </summary>
        public string JvmClasspath { get; set; }

        /// <summary>
        /// Collection of options passed to JVM on Ignite start.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> JvmOptions { get; set; }

        /// <summary>
        /// List of additional .Net assemblies to load on Ignite start. Each item can be either
        /// fully qualified assembly name, path to assembly to DLL or path to a directory when 
        /// assemblies reside.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> Assemblies { get; set; }

        /// <summary>
        /// Whether to suppress warnings.
        /// </summary>
        public bool SuppressWarnings { get; set; }

        /// <summary>
        /// Lifecycle beans.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<ILifecycleBean> LifecycleBeans { get; set; }

        /// <summary>
        /// Initial amount of memory in megabytes given to JVM. Maps to -Xms Java option.
        /// <code>-1</code> maps to JVM defaults.
        /// Defaults to <see cref="DefaultJvmInitMem"/>.
        /// </summary>
        [DefaultValue(DefaultJvmInitMem)]
        public int JvmInitialMemoryMb { get; set; }

        /// <summary>
        /// Maximum amount of memory in megabytes given to JVM. Maps to -Xmx Java option.
        /// <code>-1</code> maps to JVM defaults.
        /// Defaults to <see cref="DefaultJvmMaxMem"/>.
        /// </summary>
        [DefaultValue(DefaultJvmMaxMem)]
        public int JvmMaxMemoryMb { get; set; }

        /// <summary>
        /// Gets or sets the discovery service provider.
        /// Null for default discovery.
        /// </summary>
        public IDiscoverySpi DiscoverySpi { get; set; }

        /// <summary>
        /// Gets or sets the communication service provider.
        /// Null for default communication.
        /// </summary>
        public ICommunicationSpi CommunicationSpi { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether node should start in client mode.
        /// Client node cannot hold data in the caches.
        /// </summary>
        public bool ClientMode
        {
            get { return _clientMode ?? default(bool); }
            set { _clientMode = value; }
        }

        /// <summary>
        /// Gets or sets a set of event types (<see cref="EventType" />) to be recorded by Ignite. 
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<int> IncludedEventTypes { get; set; }

        /// <summary>
        /// Gets or sets the time after which a certain metric value is considered expired.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "10675199.02:48:05.4775807")]
        public TimeSpan MetricsExpireTime
        {
            get { return _metricsExpireTime ?? DefaultMetricsExpireTime; }
            set { _metricsExpireTime = value; }
        }

        /// <summary>
        /// Gets or sets the number of metrics kept in history to compute totals and averages.
        /// </summary>
        [DefaultValue(DefaultMetricsHistorySize)]
        public int MetricsHistorySize
        {
            get { return _metricsHistorySize ?? DefaultMetricsHistorySize; }
            set { _metricsHistorySize = value; }
        }

        /// <summary>
        /// Gets or sets the frequency of metrics log print out.
        /// <see cref="TimeSpan.Zero"/> to disable metrics print out.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:01:00")]
        public TimeSpan MetricsLogFrequency
        {
            get { return _metricsLogFrequency ?? DefaultMetricsLogFrequency; }
            set { _metricsLogFrequency = value; }
        }

        /// <summary>
        /// Gets or sets the job metrics update frequency.
        /// <see cref="TimeSpan.Zero"/> to update metrics on job start/finish.
        /// Negative value to never update metrics.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:02")]
        public TimeSpan MetricsUpdateFrequency
        {
            get { return _metricsUpdateFrequency ?? DefaultMetricsUpdateFrequency; }
            set { _metricsUpdateFrequency = value; }
        }

        /// <summary>
        /// Gets or sets the network send retry count.
        /// </summary>
        [DefaultValue(DefaultNetworkSendRetryCount)]
        public int NetworkSendRetryCount
        {
            get { return _networkSendRetryCount ?? DefaultNetworkSendRetryCount; }
            set { _networkSendRetryCount = value; }
        }

        /// <summary>
        /// Gets or sets the network send retry delay.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:01")]
        public TimeSpan NetworkSendRetryDelay
        {
            get { return _networkSendRetryDelay ?? DefaultNetworkSendRetryDelay; }
            set { _networkSendRetryDelay = value; }
        }

        /// <summary>
        /// Gets or sets the network timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan NetworkTimeout
        {
            get { return _networkTimeout ?? DefaultNetworkTimeout; }
            set { _networkTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the work directory.
        /// If not provided, a folder under <see cref="IgniteHome"/> will be used.
        /// </summary>
        public string WorkDirectory { get; set; }

        /// <summary>
        /// Gets or sets system-wide local address or host for all Ignite components to bind to. 
        /// If provided it will override all default local bind settings within Ignite.
        /// <para />
        /// If <c>null</c> then Ignite tries to use local wildcard address.That means that all services 
        /// will be available on all network interfaces of the host machine. 
        /// <para />
        /// It is strongly recommended to set this parameter for all production environments.
        /// </summary>
        public string Localhost { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this node should be a daemon node.
        /// <para />
        /// Daemon nodes are the usual grid nodes that participate in topology but not visible on the main APIs, 
        /// i.e. they are not part of any cluster groups.
        /// <para />
        /// Daemon nodes are used primarily for management and monitoring functionality that is built on Ignite 
        /// and needs to participate in the topology, but also needs to be excluded from the "normal" topology, 
        /// so that it won't participate in the task execution or in-memory data grid storage.
        /// </summary>
        public bool IsDaemon
        {
            get { return _isDaemon ?? default(bool); }
            set { _isDaemon = value; }
        }

        /// <summary>
        /// Gets or sets the user attributes for this node.
        /// <para />
        /// These attributes can be retrieved later via <see cref="IClusterNode.GetAttributes"/>.
        /// Environment variables are added to node attributes automatically.
        /// NOTE: attribute names starting with "org.apache.ignite" are reserved for internal use.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public IDictionary<string, object> UserAttributes { get; set; }

        /// <summary>
        /// Gets or sets the atomic data structures configuration.
        /// </summary>
        public AtomicConfiguration AtomicConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the transaction configuration.
        /// </summary>
        public TransactionConfiguration TransactionConfiguration { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether late affinity assignment mode should be used.
        /// <para />
        /// On each topology change, for each started cache, partition-to-node mapping is
        /// calculated using AffinityFunction for cache. When late
        /// affinity assignment mode is disabled then new affinity mapping is applied immediately.
        /// <para />
        /// With late affinity assignment mode, if primary node was changed for some partition, but data for this
        /// partition is not rebalanced yet on this node, then current primary is not changed and new primary 
        /// is temporary assigned as backup. This nodes becomes primary only when rebalancing for all assigned primary 
        /// partitions is finished. This mode can show better performance for cache operations, since when cache 
        /// primary node executes some operation and data is not rebalanced yet, then it sends additional message 
        /// to force rebalancing from other nodes.
        /// <para />
        /// Note, that <see cref="ICacheAffinity"/> interface provides assignment information taking late assignment
        /// into account, so while rebalancing for new primary nodes is not finished it can return assignment 
        /// which differs from assignment calculated by AffinityFunction.
        /// <para />
        /// This property should have the same value for all nodes in cluster.
        /// <para />
        /// If not provided, default value is <see cref="DefaultIsLateAffinityAssignment"/>.
        /// </summary>
        [DefaultValue(DefaultIsLateAffinityAssignment)]
        public bool IsLateAffinityAssignment
        {
            get { return _isLateAffinityAssignment ?? DefaultIsLateAffinityAssignment; }
            set { _isLateAffinityAssignment = value; }
        }

        /// <summary>
        /// Serializes this instance to the specified XML writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="rootElementName">Name of the root element.</param>
        public void ToXml(XmlWriter writer, string rootElementName)
        {
            IgniteArgumentCheck.NotNull(writer, "writer");
            IgniteArgumentCheck.NotNullOrEmpty(rootElementName, "rootElementName");

            IgniteConfigurationXmlSerializer.Serialize(this, writer, rootElementName);
        }

        /// <summary>
        /// Serializes this instance to an XML string.
        /// </summary>
        public string ToXml()
        {
            var sb = new StringBuilder();

            var settings = new XmlWriterSettings
            {
                Indent = true
            };

            using (var xmlWriter = XmlWriter.Create(sb, settings))
            {
                ToXml(xmlWriter, "igniteConfiguration");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Deserializes IgniteConfiguration from the XML reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>Deserialized instance.</returns>
        public static IgniteConfiguration FromXml(XmlReader reader)
        {
            IgniteArgumentCheck.NotNull(reader, "reader");

            return IgniteConfigurationXmlSerializer.Deserialize(reader);
        }

        /// <summary>
        /// Deserializes IgniteConfiguration from the XML string.
        /// </summary>
        /// <param name="xml">Xml string.</param>
        /// <returns>Deserialized instance.</returns>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        [SuppressMessage("Microsoft.Usage", "CA2202: Do not call Dispose more than one time on an object")]
        public static IgniteConfiguration FromXml(string xml)
        {
            IgniteArgumentCheck.NotNullOrEmpty(xml, "xml");

            using (var stringReader = new StringReader(xml))
            using (var xmlReader = XmlReader.Create(stringReader))
            {
                // Skip XML header.
                xmlReader.MoveToContent();

                return FromXml(xmlReader);
            }
        }

        /// <summary>
        /// Gets or sets the logger.
        /// <para />
        /// If no logger is set, logging is delegated to Java, which uses the logger defined in Spring XML (if present)
        /// or logs to console otherwise.
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Gets or sets the failure detection timeout used by <see cref="TcpDiscoverySpi"/> 
        /// and <see cref="TcpCommunicationSpi"/>.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:10")]
        public TimeSpan FailureDetectionTimeout
        {
            get { return _failureDetectionTimeout ?? DefaultFailureDetectionTimeout; }
            set { _failureDetectionTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the swap space SPI.
        /// </summary>
        public ISwapSpaceSpi SwapSpaceSpi { get; set; }
    }
}

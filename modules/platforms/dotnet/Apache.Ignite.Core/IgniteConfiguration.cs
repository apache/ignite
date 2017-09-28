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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.DataStructures.Configuration;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Discovery;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Plugin;
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

        /// <summary>
        /// Default failure detection timeout.
        /// </summary>
        public static readonly TimeSpan DefaultClientFailureDetectionTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Default thread pool size.
        /// </summary>
        public static readonly int DefaultThreadPoolSize = Math.Max(8, Environment.ProcessorCount);

        /// <summary>
        /// Default management thread pool size.
        /// </summary>
        public const int DefaultManagementThreadPoolSize = 4;

        /// <summary>
        /// Default timeout after which long query warning will be printed.
        /// </summary>
        public static readonly TimeSpan DefaultLongQueryWarningTimeout = TimeSpan.FromMilliseconds(3000);

        /// <summary>
        /// Default value for <see cref="ClientConnectorConfigurationEnabled"/>.
        /// </summary>
        public const bool DefaultClientConnectorConfigurationEnabled = true;

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
        private bool? _clientMode;

        /** */
        private TimeSpan? _failureDetectionTimeout;

        /** */
        private TimeSpan? _clientFailureDetectionTimeout;

        /** */
        private int? _publicThreadPoolSize;

        /** */
        private int? _stripedThreadPoolSize;

        /** */
        private int? _serviceThreadPoolSize;

        /** */
        private int? _systemThreadPoolSize;

        /** */
        private int? _asyncCallbackThreadPoolSize;

        /** */
        private int? _managementThreadPoolSize;

        /** */
        private int? _dataStreamerThreadPoolSize;

        /** */
        private int? _utilityCacheThreadPoolSize;

        /** */
        private int? _queryThreadPoolSize;

        /** */
        private TimeSpan? _longQueryWarningTimeout;

        /** */
        private bool? _isActiveOnStart;

        /** Local event listeners. Stored as array to ensure index access. */
        private LocalEventListener[] _localEventListenersInternal;

        /** Map from user-defined listener to it's id. */
        private Dictionary<object, int> _localEventListenerIds;

        /// <summary>
        /// Default network retry count.
        /// </summary>
        public const int DefaultNetworkSendRetryCount = 3;

        /// <summary>
        /// Default late affinity assignment mode.
        /// </summary>
        public const bool DefaultIsLateAffinityAssignment = true;

        /// <summary>
        /// Default value for <see cref="IsActiveOnStart"/> property.
        /// </summary>
        public const bool DefaultIsActiveOnStart = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class.
        /// </summary>
        public IgniteConfiguration()
        {
            JvmInitialMemoryMb = DefaultJvmInitMem;
            JvmMaxMemoryMb = DefaultJvmMaxMem;
            ClientConnectorConfigurationEnabled = DefaultClientConnectorConfigurationEnabled;
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
                var marsh = BinaryUtils.Marshaller;

                configuration.Write(marsh.StartMarshal(stream));

                stream.SynchronizeOutput();

                stream.Seek(0, SeekOrigin.Begin);

                ReadCore(marsh.StartUnmarshal(stream));
            }

            CopyLocalProperties(configuration);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration" /> class from a reader.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        /// <param name="baseConfig">The base configuration.</param>
        internal IgniteConfiguration(BinaryReader binaryReader, IgniteConfiguration baseConfig)
        {
            Debug.Assert(binaryReader != null);
            Debug.Assert(baseConfig != null);

            Read(binaryReader);
            CopyLocalProperties(baseConfig);
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
            writer.WriteTimeSpanAsLongNullable(_failureDetectionTimeout);
            writer.WriteTimeSpanAsLongNullable(_clientFailureDetectionTimeout);
            writer.WriteTimeSpanAsLongNullable(_longQueryWarningTimeout);
            writer.WriteBooleanNullable(_isActiveOnStart);
            writer.WriteObjectDetached(ConsistentId);

            // Thread pools
            writer.WriteIntNullable(_publicThreadPoolSize);
            writer.WriteIntNullable(_stripedThreadPoolSize);
            writer.WriteIntNullable(_serviceThreadPoolSize);
            writer.WriteIntNullable(_systemThreadPoolSize);
            writer.WriteIntNullable(_asyncCallbackThreadPoolSize);
            writer.WriteIntNullable(_managementThreadPoolSize);
            writer.WriteIntNullable(_dataStreamerThreadPoolSize);
            writer.WriteIntNullable(_utilityCacheThreadPoolSize);
            writer.WriteIntNullable(_queryThreadPoolSize);

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

                // Name mapper.
                var mapper = BinaryConfiguration.NameMapper as BinaryBasicNameMapper;
                writer.WriteBoolean(mapper != null && mapper.IsSimpleName);
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

            // Event storage
            if (EventStorageSpi == null)
            {
                writer.WriteByte(0);
            }
            else if (EventStorageSpi is NoopEventStorageSpi)
            {
                writer.WriteByte(1);
            }
            else
            {
                var memEventStorage = EventStorageSpi as MemoryEventStorageSpi;

                if (memEventStorage == null)
                {
                    throw new IgniteException(string.Format(
                        "Unsupported IgniteConfiguration.EventStorageSpi: '{0}'. " +
                        "Supported implementations: '{1}', '{2}'.",
                        EventStorageSpi.GetType(), typeof(NoopEventStorageSpi), typeof(MemoryEventStorageSpi)));
                }

                writer.WriteByte(2);

                memEventStorage.Write(writer);
            }

            if (MemoryConfiguration != null)
            {
                writer.WriteBoolean(true);
                MemoryConfiguration.Write(writer);
            }
            else
            {
                writer.WriteBoolean(false);
            }

            // SQL connector.
#pragma warning disable 618  // Obsolete
            if (SqlConnectorConfiguration != null)
            {
                writer.WriteBoolean(true);
                SqlConnectorConfiguration.Write(writer);
            }
#pragma warning restore 618
            else
            {
                writer.WriteBoolean(false);
            }

            // Client connector.
            if (ClientConnectorConfiguration != null)
            {
                writer.WriteBoolean(true);
                ClientConnectorConfiguration.Write(writer);
            }
            else
            {
                writer.WriteBoolean(false);
            }

            writer.WriteBoolean(ClientConnectorConfigurationEnabled);

            // Persistence.
            if (PersistentStoreConfiguration != null)
            {
                writer.WriteBoolean(true);
                PersistentStoreConfiguration.Write(writer);
            }
            else
            {
                writer.WriteBoolean(false);
            }

            // Plugins (should be last).
            if (PluginConfigurations != null)
            {
                var pos = writer.Stream.Position;

                writer.WriteInt(0); // reserve count

                var cnt = 0;

                foreach (var cfg in PluginConfigurations)
                {
                    if (cfg.PluginConfigurationClosureFactoryId != null)
                    {
                        writer.WriteInt(cfg.PluginConfigurationClosureFactoryId.Value);

                        cfg.WriteBinary(writer);

                        cnt++;
                    }
                }

                writer.Stream.WriteInt(pos, cnt);
            }
            else
            {
                writer.WriteInt(0);
            }

            // Local event listeners (should be last).
            if (LocalEventListeners != null)
            {
                writer.WriteInt(LocalEventListeners.Count);

                foreach (var listener in LocalEventListeners)
                {
                    ValidateLocalEventListener(listener);

                    writer.WriteIntArray(listener.EventTypes.ToArray());
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }

        /// <summary>
        /// Validates the local event listener.
        /// </summary>
        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        // ReSharper disable once UnusedParameter.Local
        private static void ValidateLocalEventListener(LocalEventListener listener)
        {
            if (listener == null)
            {
                throw new IgniteException("LocalEventListeners can't contain nulls.");
            }

            if (listener.ListenerObject == null)
            {
                throw new IgniteException("LocalEventListener.Listener can't be null.");
            }

            if (listener.EventTypes == null || listener.EventTypes.Count == 0)
            {
                throw new IgniteException("LocalEventListener.EventTypes can't be null or empty.");
            }
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
            _failureDetectionTimeout = r.ReadTimeSpanNullable();
            _clientFailureDetectionTimeout = r.ReadTimeSpanNullable();
            _longQueryWarningTimeout = r.ReadTimeSpanNullable();
            _isActiveOnStart = r.ReadBooleanNullable();
            ConsistentId = r.ReadObject<object>();

            // Thread pools
            _publicThreadPoolSize = r.ReadIntNullable();
            _stripedThreadPoolSize = r.ReadIntNullable();
            _serviceThreadPoolSize = r.ReadIntNullable();
            _systemThreadPoolSize = r.ReadIntNullable();
            _asyncCallbackThreadPoolSize = r.ReadIntNullable();
            _managementThreadPoolSize = r.ReadIntNullable();
            _dataStreamerThreadPoolSize = r.ReadIntNullable();
            _utilityCacheThreadPoolSize = r.ReadIntNullable();
            _queryThreadPoolSize = r.ReadIntNullable();

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
                {
                    BinaryConfiguration.CompactFooter = r.ReadBoolean();
                }

                if (r.ReadBoolean())
                {
                    BinaryConfiguration.NameMapper = BinaryBasicNameMapper.SimpleNameInstance;
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

            // Event storage
            switch (r.ReadByte())
            {
                case 1: EventStorageSpi = new NoopEventStorageSpi();
                    break;

                case 2:
                    EventStorageSpi = MemoryEventStorageSpi.Read(r);
                    break;
            }

            if (r.ReadBoolean())
            {
                MemoryConfiguration = new MemoryConfiguration(r);
            }

            // SQL.
            if (r.ReadBoolean())
            {
#pragma warning disable 618  // Obsolete
                SqlConnectorConfiguration = new SqlConnectorConfiguration(r);
#pragma warning restore 618
            }

            // Client.
            if (r.ReadBoolean())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration(r);
            }

            ClientConnectorConfigurationEnabled = r.ReadBoolean();

            // Persistence.
            if (r.ReadBoolean())
            {
                PersistentStoreConfiguration = new PersistentStoreConfiguration(r);
            }
        }

        /// <summary>
        /// Reads data from specified reader into current instance.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        private void Read(BinaryReader binaryReader)
        {
            ReadCore(binaryReader);

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
            IgniteInstanceName = cfg.IgniteInstanceName;

            if (BinaryConfiguration != null && cfg.BinaryConfiguration != null)
            {
                BinaryConfiguration.CopyLocalProperties(cfg.BinaryConfiguration);
            }

            SpringConfigUrl = cfg.SpringConfigUrl;
            JvmClasspath = cfg.JvmClasspath;
            JvmOptions = cfg.JvmOptions;
            Assemblies = cfg.Assemblies;
            SuppressWarnings = cfg.SuppressWarnings;
            LifecycleHandlers = cfg.LifecycleHandlers;
            Logger = cfg.Logger;
            JvmInitialMemoryMb = cfg.JvmInitialMemoryMb;
            JvmMaxMemoryMb = cfg.JvmMaxMemoryMb;
            PluginConfigurations = cfg.PluginConfigurations;
            AutoGenerateIgniteInstanceName = cfg.AutoGenerateIgniteInstanceName;
            PeerAssemblyLoadingMode = cfg.PeerAssemblyLoadingMode;
            LocalEventListeners = cfg.LocalEventListeners;

            if (CacheConfiguration != null && cfg.CacheConfiguration != null)
            {
                var caches = cfg.CacheConfiguration.Where(x => x != null).ToDictionary(x => "_" + x.Name, x => x);

                foreach (var cache in CacheConfiguration)
                {
                    CacheConfiguration src;

                    if (cache != null && caches.TryGetValue("_" + cache.Name, out src))
                    {
                        cache.CopyLocalProperties(src);
                    }
                }
            }
        }

        /// <summary>
        /// Gets or sets optional local instance name.
        /// <para />
        /// This name only works locally and has no effect on topology.
        /// <para />
        /// This property is used to when there are multiple Ignite nodes in one process to distinguish them.
        /// </summary>
        public string IgniteInstanceName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether unique <see cref="IgniteInstanceName"/> should be generated.
        /// <para />
        /// Set this to true in scenarios where new node should be started regardless of other nodes present within
        /// current process. In particular, this setting is useful is ASP.NET and IIS environments, where AppDomains
        /// are loaded and unloaded within a single process during application restarts. Ignite stops all nodes
        /// on <see cref="AppDomain"/> unload, however, IIS does not wait for previous AppDomain to unload before
        /// starting up a new one, which may cause "Ignite instance with this name has already been started" errors.
        /// This setting solves the issue.
        /// </summary>
        public bool AutoGenerateIgniteInstanceName { get; set; }

        /// <summary>
        /// Gets or sets optional local instance name.
        /// <para />
        /// This name only works locally and has no effect on topology.
        /// <para />
        /// This property is used to when there are multiple Ignite nodes in one process to distinguish them.
        /// </summary>
        [Obsolete("Use IgniteInstanceName instead.")]
        public string GridName
        {
            get { return IgniteInstanceName; }
            set { IgniteInstanceName = value; }
        }

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
        /// Lifecycle handlers.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<ILifecycleHandler> LifecycleHandlers { get; set; }

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
        /// Gets or sets pre-configured local event listeners.
        /// <para />
        /// This is similar to calling <see cref="IEvents.LocalListen{T}(IEventListener{T},int[])"/>,
        /// but important difference is that some events occur during startup and can be only received this way.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<LocalEventListener> LocalEventListeners { get; set; }

        /// <summary>
        /// Initializes the local event listeners collections.
        /// </summary>
        private void InitLocalEventListeners()
        {
            if (LocalEventListeners != null && _localEventListenersInternal == null)
            {
                _localEventListenersInternal = LocalEventListeners.ToArray();

                _localEventListenerIds = new Dictionary<object, int>();

                for (var i = 0; i < _localEventListenersInternal.Length; i++)
                {
                    var listener = _localEventListenersInternal[i];
                    ValidateLocalEventListener(listener);
                    _localEventListenerIds[listener.ListenerObject] = i;
                }
            }
        }

        /// <summary>
        /// Gets the local event listeners.
        /// </summary>
        internal LocalEventListener[] LocalEventListenersInternal
        {
            get
            {
                InitLocalEventListeners();

                return _localEventListenersInternal;
            }
        }

        /// <summary>
        /// Gets the local event listener ids.
        /// </summary>
        internal Dictionary<object, int> LocalEventListenerIds
        {
            get
            {
                InitLocalEventListeners();

                return _localEventListenerIds;
            }
        }

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
        [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "value")]
        [DefaultValue(DefaultIsLateAffinityAssignment)]
        [Obsolete("No longer supported, always true.")]
        public bool IsLateAffinityAssignment
        {
            get { return DefaultIsLateAffinityAssignment; }
            // ReSharper disable once ValueParameterNotUsed
            set { /* No-op. */ }
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
        /// Gets or sets the failure detection timeout used by <see cref="TcpDiscoverySpi"/>
        /// and <see cref="TcpCommunicationSpi"/> for client nodes.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:30")]
        public TimeSpan ClientFailureDetectionTimeout
        {
            get { return _clientFailureDetectionTimeout ?? DefaultClientFailureDetectionTimeout; }
            set { _clientFailureDetectionTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the configurations for plugins to be started.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<IPluginConfiguration> PluginConfigurations { get; set; }

        /// <summary>
        /// Gets or sets the event storage interface.
        /// <para />
        /// Only predefined implementations are supported:
        /// <see cref="NoopEventStorageSpi"/>, <see cref="MemoryEventStorageSpi"/>.
        /// </summary>
        public IEventStorageSpi EventStorageSpi { get; set; }

        /// <summary>
        /// Gets or sets the page memory configuration.
        /// <see cref="MemoryConfiguration"/> for more details.
        /// </summary>
        public MemoryConfiguration MemoryConfiguration { get; set; }

        /// <summary>
        /// Gets or sets a value indicating how user assemblies should be loaded on remote nodes.
        /// <para />
        /// For example, when executing <see cref="ICompute.Call{TRes}(IComputeFunc{TRes})"/>,
        /// the assembly with corresponding <see cref="IComputeFunc{TRes}"/> should be loaded on remote nodes.
        /// With this option enabled, Ignite will attempt to send the assembly to remote nodes
        /// and load it there automatically.
        /// <para />
        /// Default is <see cref="Apache.Ignite.Core.Deployment.PeerAssemblyLoadingMode.Disabled"/>.
        /// <para />
        /// Peer loading is enabled for <see cref="ICompute"/> functionality.
        /// </summary>
        public PeerAssemblyLoadingMode PeerAssemblyLoadingMode { get; set; }

        /// <summary>
        /// Gets or sets the size of the public thread pool, which processes compute jobs and user messages.
        /// </summary>
        public int PublicThreadPoolSize
        {
            get { return _publicThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _publicThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the striped thread pool, which processes cache requests.
        /// </summary>
        public int StripedThreadPoolSize
        {
            get { return _stripedThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _stripedThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the service thread pool, which processes Ignite services.
        /// </summary>
        public int ServiceThreadPoolSize
        {
            get { return _serviceThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _serviceThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the system thread pool, which processes internal system messages.
        /// </summary>
        public int SystemThreadPoolSize
        {
            get { return _systemThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _systemThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the asynchronous callback thread pool.
        /// </summary>
        public int AsyncCallbackThreadPoolSize
        {
            get { return _asyncCallbackThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _asyncCallbackThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the management thread pool, which processes internal Ignite jobs.
        /// </summary>
        [DefaultValue(DefaultManagementThreadPoolSize)]
        public int ManagementThreadPoolSize
        {
            get { return _managementThreadPoolSize ?? DefaultManagementThreadPoolSize; }
            set { _managementThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the data streamer thread pool.
        /// </summary>
        public int DataStreamerThreadPoolSize
        {
            get { return _dataStreamerThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _dataStreamerThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the utility cache thread pool.
        /// </summary>
        public int UtilityCacheThreadPoolSize
        {
            get { return _utilityCacheThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _utilityCacheThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the size of the query thread pool.
        /// </summary>
        public int QueryThreadPoolSize
        {
            get { return _queryThreadPoolSize ?? DefaultThreadPoolSize; }
            set { _queryThreadPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets the SQL connector configuration (for JDBC and ODBC).
        /// </summary>
        [Obsolete("Use ClientConnectorConfiguration instead.")]
        public SqlConnectorConfiguration SqlConnectorConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the client connector configuration (for JDBC, ODBC, and thin clients).
        /// </summary>
        public ClientConnectorConfiguration ClientConnectorConfiguration { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether client connector is enabled:
        /// allow thin clients, ODBC and JDBC drivers to work with Ignite
        /// (see <see cref="ClientConnectorConfiguration"/>).
        /// Default is <see cref="DefaultClientConnectorConfigurationEnabled"/>.
        /// </summary>
        [DefaultValue(DefaultClientConnectorConfigurationEnabled)]
        public bool ClientConnectorConfigurationEnabled { get; set; }

        /// <summary>
        /// Gets or sets the timeout after which long query warning will be printed.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:03")]
        public TimeSpan LongQueryWarningTimeout
        {
            get { return _longQueryWarningTimeout ?? DefaultLongQueryWarningTimeout; }
            set { _longQueryWarningTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the persistent store configuration.
        /// </summary>
        public PersistentStoreConfiguration PersistentStoreConfiguration { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether grid should be active on start.
        /// See also <see cref="IIgnite.IsActive"/> and <see cref="IIgnite.SetActive"/>.
        /// <para />
        /// This property is ignored when <see cref="PersistentStoreConfiguration"/> is present:
        /// cluster is always inactive on start when Ignite Persistence is enabled.
        /// </summary>
        [DefaultValue(DefaultIsActiveOnStart)]
        public bool IsActiveOnStart
        {
            get { return _isActiveOnStart ?? DefaultIsActiveOnStart; }
            set { _isActiveOnStart = value; }
        }

        /// <summary>
        /// Gets or sets consistent globally unique node identifier which survives node restarts.
        /// </summary>
        public object ConsistentId { get; set; }
    }
}

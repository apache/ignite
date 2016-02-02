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
 namespace Apache.Ignite.Core
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Discovery;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Lifecycle;
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
        public const int DefaultJvmInitMem = 512;

        /// <summary>
        /// Default maximum JVM memory in megabytes.
        /// </summary>
        public const int DefaultJvmMaxMem = 1024;

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
        /// Default network retry count.
        /// </summary>
        public const int DefaultNetworkSendRetryCount = 3;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class.
        /// </summary>
        public IgniteConfiguration()
        {
            JvmInitialMemoryMb = DefaultJvmInitMem;
            JvmMaxMemoryMb = DefaultJvmMaxMem;

            MetricsExpireTime = DefaultMetricsExpireTime;
            MetricsHistorySize = DefaultMetricsHistorySize;
            MetricsLogFrequency = DefaultMetricsLogFrequency;
            MetricsUpdateFrequency = DefaultMetricsUpdateFrequency;
            NetworkTimeout = DefaultNetworkTimeout;
            NetworkSendRetryCount = DefaultNetworkSendRetryCount;
            NetworkSendRetryDelay = DefaultNetworkSendRetryDelay;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class.
        /// </summary>
        /// <param name="configuration">The configuration to copy.</param>
        public IgniteConfiguration(IgniteConfiguration configuration)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            CopyLocalProperties(configuration);

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var marsh = new Marshaller(configuration.BinaryConfiguration);

                configuration.WriteCore(marsh.StartMarshal(stream));

                stream.SynchronizeOutput();

                stream.Seek(0, SeekOrigin.Begin);

                ReadCore(marsh.StartUnmarshal(stream));
            }
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

            if (!string.IsNullOrEmpty(SpringConfigUrl))
            {
                // Do not write details when there is Spring config.
                writer.WriteBoolean(false);
                return;
            }

            writer.WriteBoolean(true);  // details are present

            WriteCore(writer);
        }

        /// <summary>
        /// Writes this instance to a writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        private void WriteCore(BinaryWriter writer)
        {
            // Simple properties
            writer.WriteBoolean(ClientMode);
            writer.WriteIntArray(IncludedEventTypes == null ? null : IncludedEventTypes.ToArray());

            writer.WriteLong((long) MetricsExpireTime.TotalMilliseconds);
            writer.WriteInt(MetricsHistorySize);
            writer.WriteLong((long) MetricsLogFrequency.TotalMilliseconds);
            var metricsUpdateFreq = (long) MetricsUpdateFrequency.TotalMilliseconds;
            writer.WriteLong(metricsUpdateFreq >= 0 ? metricsUpdateFreq : -1);
            writer.WriteInt(NetworkSendRetryCount);
            writer.WriteLong((long) NetworkSendRetryDelay.TotalMilliseconds);
            writer.WriteLong((long) NetworkTimeout.TotalMilliseconds);
            writer.WriteString(WorkDirectory);
            writer.WriteString(Localhost);

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
        }

        /// <summary>
        /// Reads data from specified reader into current instance.
        /// </summary>
        /// <param name="r">The binary reader.</param>
        private void ReadCore(BinaryReader r)
        {
            // Simple properties
            ClientMode = r.ReadBoolean();
            IncludedEventTypes = r.ReadIntArray();

            MetricsExpireTime = r.ReadLongAsTimespan();
            MetricsHistorySize = r.ReadInt();
            MetricsLogFrequency = r.ReadLongAsTimespan();
            MetricsUpdateFrequency = r.ReadLongAsTimespan();
            NetworkSendRetryCount = r.ReadInt();
            NetworkSendRetryDelay = r.ReadLongAsTimespan();
            NetworkTimeout = r.ReadLongAsTimespan();
            WorkDirectory = r.ReadString();
            Localhost = r.ReadString();

            // Cache config
            var cacheCfgCount = r.ReadInt();
            CacheConfiguration = new List<CacheConfiguration>(cacheCfgCount);
            for (int i = 0; i < cacheCfgCount; i++)
                CacheConfiguration.Add(new CacheConfiguration(r));

            // Discovery config
            DiscoverySpi = r.ReadBoolean() ? new TcpDiscoverySpi(r) : null;
        }

        /// <summary>
        /// Reads data from specified reader into current instance.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        private void Read(BinaryReader binaryReader)
        {
            var r = binaryReader;

            CopyLocalProperties(r.Marshaller.Ignite.Configuration);

            ReadCore(r);

            // Misc
            IgniteHome = r.ReadString();

            JvmInitialMemoryMb = (int) (r.ReadLong()/1024/2014);
            JvmMaxMemoryMb = (int) (r.ReadLong()/1024/2014);

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
            BinaryConfiguration = cfg.BinaryConfiguration == null
                ? null
                : new BinaryConfiguration(cfg.BinaryConfiguration);
            JvmClasspath = cfg.JvmClasspath;
            JvmOptions = cfg.JvmOptions;
            Assemblies = cfg.Assemblies;
            SuppressWarnings = cfg.SuppressWarnings;
            LifecycleBeans = cfg.LifecycleBeans;
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
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1056:UriPropertiesShouldNotBeStrings")]
        [Obsolete("Ignite.NET can be configured natively without Spring. " +
                  "Setting this property will ignore all other properties except " +
                  "IgniteHome, Assemblies, SuppressWarnings, LifecycleBeans, JvmOptions, JvmdllPath, IgniteHome, " +
                  "JvmInitialMemoryMb, JvmMaxMemoryMb.")]
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
        /// Defaults to <see cref="DefaultJvmInitMem"/>.
        /// </summary>
        [DefaultValue(DefaultJvmInitMem)]
        public int JvmInitialMemoryMb { get; set; }

        /// <summary>
        /// Maximum amount of memory in megabytes given to JVM. Maps to -Xmx Java option.
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
        /// Gets or sets a value indicating whether node should start in client mode.
        /// Client node cannot hold data in the caches.
        /// Default is null and takes this setting from Spring configuration.
        /// </summary>
        public bool ClientMode { get; set; }

        /// <summary>
        /// Gets or sets a set of event types (<see cref="EventType" />) to be recorded by Ignite. 
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<int> IncludedEventTypes { get; set; }

        /// <summary>
        /// Gets or sets the time after which a certain metric value is considered expired.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "10675199.02:48:05.4775807")]
        public TimeSpan MetricsExpireTime { get; set; }

        /// <summary>
        /// Gets or sets the number of metrics kept in history to compute totals and averages.
        /// </summary>
        [DefaultValue(DefaultMetricsHistorySize)]
        public int MetricsHistorySize { get; set; }

        /// <summary>
        /// Gets or sets the frequency of metrics log print out.
        /// <see cref="TimeSpan.Zero"/> to disable metrics print out.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:01:00")]
        public TimeSpan MetricsLogFrequency { get; set; }

        /// <summary>
        /// Gets or sets the job metrics update frequency.
        /// <see cref="TimeSpan.Zero"/> to update metrics on job start/finish.
        /// Negative value to never update metrics.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:02")]
        public TimeSpan MetricsUpdateFrequency { get; set; }

        /// <summary>
        /// Gets or sets the network send retry count.
        /// </summary>
        [DefaultValue(DefaultNetworkSendRetryCount)]
        public int NetworkSendRetryCount { get; set; }

        /// <summary>
        /// Gets or sets the network send retry delay.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:01")]
        public TimeSpan NetworkSendRetryDelay { get; set; }

        /// <summary>
        /// Gets or sets the network timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan NetworkTimeout { get; set; }

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
    }
}

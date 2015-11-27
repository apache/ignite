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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Lifecycle;

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
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class.
        /// </summary>
        public IgniteConfiguration()
        {
            JvmInitialMemoryMb = DefaultJvmInitMem;
            JvmMaxMemoryMb = DefaultJvmMaxMem;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteConfiguration"/> class from a reader.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        internal IgniteConfiguration(BinaryReader binaryReader)
        {
            var r = binaryReader;

            GridName = r.ReadString();

            var cacheCfgCount = r.ReadInt();
            CacheConfiguration = new List<CacheConfiguration>(cacheCfgCount);
            for (int i = 0; i < cacheCfgCount; i++)
                CacheConfiguration.Add(new CacheConfiguration(r));

            IgniteHome = r.ReadString();

            JvmInitialMemoryMb = (int) (r.ReadLong() / 1024 / 2014);
            JvmMaxMemoryMb = (int) (r.ReadLong() / 1024 / 2014);

            DiscoveryConfiguration = r.ReadBoolean() ? new DiscoveryConfiguration(r) : null;

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


            // Local data (not from reader)
            JvmDllPath = Process.GetCurrentProcess().Modules.OfType<ProcessModule>()
                .Single(x => string.Equals(x.ModuleName, IgniteUtils.FileJvmDll, StringComparison.OrdinalIgnoreCase))
                .FileName;

            var marsh = r.Marshaller;

            var origCfg = marsh.Ignite.Configuration;

            BinaryConfiguration = marsh.BinaryConfiguration;

            SpringConfigUrl = origCfg.SpringConfigUrl;

            JvmClasspath = origCfg.JvmClasspath;

            JvmOptions = origCfg.JvmOptions;

            Assemblies = origCfg.Assemblies;

            SuppressWarnings = origCfg.SuppressWarnings;

            LifecycleBeans = origCfg.LifecycleBeans;
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
        public ICollection<CacheConfiguration> CacheConfiguration { get; set; }

        /// <summary>
        /// URL to Spring configuration file.
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
        /// Defaults to <see cref="DefaultJvmInitMem"/>.
        /// </summary>
        public int JvmInitialMemoryMb { get; set; }

        /// <summary>
        /// Maximum amount of memory in megabytes given to JVM. Maps to -Xmx Java option.
        /// Defaults to <see cref="DefaultJvmMaxMem"/>.
        /// </summary>
        public int JvmMaxMemoryMb { get; set; }

        /// <summary>
        /// Gets or sets the discovery configuration.
        /// Null for default configuration.
        /// </summary>
        public DiscoveryConfiguration DiscoveryConfiguration { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether node should start in client mode.
        /// Client node cannot hold data in the caches.
        /// Default is null and takes this setting from Spring configuration.
        /// </summary>
        public bool? ClientMode { get; set; }

        /// <summary>
        /// Gets or sets a set of event types (<see cref="EventType" />) to be recorded by Ignite. 
        /// </summary>
        public ICollection<int> IncludedEventTypes { get; set; }

        /// <summary>
        /// Gets or sets the time after which a certain metric value is considered expired.
        /// </summary>
        public TimeSpan? MetricsExpireTime { get; set; }

        /// <summary>
        /// Gets or sets the number of metrics kept in history to compute totals and averages.
        /// </summary>
        public int? MetricsHistorySize { get; set; }

        /// <summary>
        /// Gets or sets the frequency of metrics log print out.
        /// <see cref="TimeSpan.Zero"/> to disable metrics print out.
        /// </summary>
        public TimeSpan? MetricsLogFrequency { get; set; }

        /// <summary>
        /// Gets or sets the job metrics update frequency.
        /// <see cref="TimeSpan.Zero"/> to update metrics on job start/finish.
        /// Negative value to never update metrics.
        /// </summary>
        public TimeSpan? MetricsUpdateFrequency { get; set; }

        /// <summary>
        /// Gets or sets the network send retry count.
        /// </summary>
        public int? NetworkSendRetryCount { get; set; }

        /// <summary>
        /// Gets or sets the network send retry delay.
        /// </summary>
        public TimeSpan? NetworkSendRetryDelay { get; set; }

        /// <summary>
        /// Gets or sets the network timeout.
        /// </summary>
        public TimeSpan? NetworkTimeout { get; set; }

        /// <summary>
        /// Gets or sets the work directory.
        /// If not provided, a folder under <see cref="IgniteHome"/> will be used.
        /// </summary>
        public string WorkDirectory { get; set; }
    }
}

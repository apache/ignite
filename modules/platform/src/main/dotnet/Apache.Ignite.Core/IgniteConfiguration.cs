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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Portable;

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
        /// Default constructor.
        /// </summary>
        public IgniteConfiguration()
        {
            JvmInitialMemoryMb = DefaultJvmInitMem;
            JvmMaxMemoryMb = DefaultJvmMaxMem;
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        internal IgniteConfiguration(IgniteConfiguration cfg)
        {
            SpringConfigUrl = cfg.SpringConfigUrl;
            JvmDllPath = cfg.JvmDllPath;
            IgniteHome = cfg.IgniteHome;
            JvmClasspath = cfg.JvmClasspath;
            SuppressWarnings = cfg.SuppressWarnings;

            JvmOptions = cfg.JvmOptions != null ? new List<string>(cfg.JvmOptions) : null;
            Assemblies = cfg.Assemblies != null ? new List<string>(cfg.Assemblies) : null;

            PortableConfiguration = cfg.PortableConfiguration != null
                ? new PortableConfiguration(cfg.PortableConfiguration)
                : null;

            LifecycleBeans = cfg.LifecycleBeans != null ? new List<ILifecycleBean>(cfg.LifecycleBeans) : null;

            JvmInitialMemoryMb = cfg.JvmInitialMemoryMb;
            JvmMaxMemoryMb = cfg.JvmMaxMemoryMb;
        }

        /// <summary>
        /// Gets or sets the portable configuration.
        /// </summary>
        /// <value>
        /// The portable configuration.
        /// </value>
        public PortableConfiguration PortableConfiguration { get; set; }

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
        public ICollection<string> JvmOptions { get; set; }

        /// <summary>
        /// List of additional .Net assemblies to load on Ignite start. Each item can be either
        /// fully qualified assembly name, path to assembly to DLL or path to a directory when 
        /// assemblies reside.
        /// </summary>
        public IList<string> Assemblies { get; set; }

        /// <summary>
        /// Whether to suppress warnings.
        /// </summary>
        public bool SuppressWarnings { get; set; }

        /// <summary>
        /// Lifecycle beans.
        /// </summary>
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
    }
}

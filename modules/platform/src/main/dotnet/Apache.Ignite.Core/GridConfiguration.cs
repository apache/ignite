/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using GridGain.Lifecycle;
    using GridGain.Portable;

    /// <summary>
    /// Grid configuration.
    /// </summary>
    public class GridConfiguration
    {
        /// <summary>
        /// Default initial JVM memory in megabytes.
        /// </summary>
        public const int DFLT_JVM_INIT_MEM = 512;

        /// <summary>
        /// Default maximum JVM memory in megabytes.
        /// </summary>
        public const int DFLT_JVM_MAX_MEM = 1024;

        /// <summary>
        /// Default constructor.
        /// </summary>
        public GridConfiguration()
        {
            JvmInitialMemoryMB = DFLT_JVM_INIT_MEM;
            JvmMaxMemoryMB = DFLT_JVM_MAX_MEM;
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        internal GridConfiguration(GridConfiguration cfg)
        {
            SpringConfigUrl = cfg.SpringConfigUrl;
            JvmDllPath = cfg.JvmDllPath;
            GridGainHome = cfg.GridGainHome;
            JvmClasspath = cfg.JvmClasspath;
            SuppressWarnings = cfg.SuppressWarnings;

            JvmOptions = cfg.JvmOptions != null ? new List<string>(cfg.JvmOptions) : null;
            Assemblies = cfg.Assemblies != null ? new List<string>(cfg.Assemblies) : null;

            PortableConfiguration = cfg.PortableConfiguration != null ? 
                new PortableConfiguration(cfg.PortableConfiguration) : null;

            LifecycleBeans = cfg.LifecycleBeans != null ? new List<ILifecycleBean>(cfg.LifecycleBeans) : null;

            JvmInitialMemoryMB = cfg.JvmInitialMemoryMB;
            JvmMaxMemoryMB = cfg.JvmMaxMemoryMB;
        }

        /// <summary>
        /// Gets or sets the portable configuration.
        /// </summary>
        /// <value>
        /// The portable configuration.
        /// </value>
        public PortableConfiguration PortableConfiguration
        {
            get;
            set;
        }

        /// <summary>
        /// URL to Spring configuration file.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1056:UriPropertiesShouldNotBeStrings")]
        public string SpringConfigUrl
        {
            get;
            set;
        }

        /// <summary>
        /// Path jvm.dll file. If not set, it's location will be determined
        /// using JAVA_HOME environment variable.
        /// If path is neither set nor determined automatically, an exception
        /// will be thrown.
        /// </summary>
        public string JvmDllPath
        {
            get;
            set;
        }

        /// <summary>
        /// Path to GridGain home. If not set environment variable GRIDGAIN_HOME
        /// will be used.
        /// </summary>
        public string GridGainHome
        {
            get;
            set;
        }

        /// <summary>
        /// Classpath used by JVM on GridGain start.
        /// </summary>
        public string JvmClasspath
        {
            get;
            set;
        }

        /// <summary>
        /// Collection of options passed to JVM on GridGain start.
        /// </summary>
        public ICollection<string> JvmOptions
        {
            get;
            set;
        }
    
        /// <summary>
        /// List of additional .Net assemblies to load on GridGain start. Each item can be either
        /// fully qualified assembly name, path to assembly to DLL or path to a directory when 
        /// assemblies reside.
        /// </summary>
        public IList<string> Assemblies
        {
            get;
            set;
        }

        /// <summary>
        /// Whether to suppress warnings.
        /// </summary>
        public bool SuppressWarnings
        {
            get;
            set;
        }

        /// <summary>
        /// Lifecycle beans.
        /// </summary>
        public ICollection<ILifecycleBean> LifecycleBeans
        {
            get;
            set;
        }

        /// <summary>
        /// Initial amount of memory in megabytes given to JVM. Maps to -Xms Java option.
        /// Defaults to <see cref="GridConfiguration.DFLT_JVM_INIT_MEM"/>.
        /// </summary>
        public int JvmInitialMemoryMB
        {
            get;
            set;
        }

        /// <summary>
        /// Maximum amount of memory in megabytes given to JVM. Maps to -Xmx Java option.
        /// Defaults to <see cref="GridConfiguration.DFLT_JVM_MAX_MEM"/>.
        /// </summary>
        public int JvmMaxMemoryMB
        {
            get;
            set;
        }
    }
}

﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Text;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native interface manager.
    /// </summary>
    public static unsafe class GridManager
    {
        /** Environment variable: GRIDGAIN_HOME. */
        internal const string ENV_GRIDGAIN_HOME = "GRIDGAIN_HOME";

        /** Environment variable: platform. */
        private const string ENV_PLATFORM_KEY = "gridgain.client.platform";

        /** Environment variable value: platform. */
        private const string EVN_PLATFORM_VAL = "dotnet";

        /** Environment variable: whether to set test classpath or not. */
        private const string ENV_GRIDGAIN_NATIVE_TEST_CLASSPATH = "GRIDGAIN_NATIVE_TEST_CLASSPATH";
        
        /** Classpath prefix. */
        private const string CLASSPATH_PREFIX = "-Djava.class.path=";

        /** Java Command line argument: Xms. Case sensitive. */
        private const string CMD_JVM_MIN_MEM_JAVA = "-Xms";

        /** Java Command line argument: Xmx. Case sensitive. */
        private const string CMD_JVM_MAX_MEM_JAVA = "-Xmx";

        /** Monitor for DLL load synchronization. */
        private static readonly object MUX = new object();

        /** First created context. */
        private static void* CTX;

        /** Configuration used on JVM start. */
        private static JvmConfiguration JVM_CFG;

        /** Memory manager. */
        private static PlatformMemoryManager MEM;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static GridManager()
        {
            Environment.SetEnvironmentVariable(ENV_PLATFORM_KEY, EVN_PLATFORM_VAL);
        }

        /// <summary>
        /// Create JVM.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="cbs">Callbacks.</param>
        /// <returns>Context.</returns>
        internal static void* GetContext(GridConfiguration cfg, UnmanagedCallbacks cbs)
        {
            lock (MUX)
            {
                // 1. Warn about possible configuration inconsistency.
                JvmConfiguration jvmCfg = JvmConfig(cfg);

                if (!cfg.SuppressWarnings && JVM_CFG != null)
                {
                    if (!JVM_CFG.Equals(jvmCfg))
                    {
                        Console.WriteLine("Attempting to start GridGain node with different Java " +
                            "configuration; current Java configuration will be ignored (consider " +
                            "starting node in separate process) [oldConfig=" + JVM_CFG +
                            ", newConfig=" + jvmCfg + ']');
                    }
                }

                // 2. Create unmanaged pointer.
                void* ctx = CreateJvm(cfg, cbs);

                cbs.SetContext(ctx);

                // 3. If this is the first JVM created, preserve it.
                if (CTX == null)
                {
                    CTX = ctx;
                    JVM_CFG = jvmCfg;
                    MEM = new PlatformMemoryManager(1024);
                }

                return ctx;
            }
        }
        
        /// <summary>
        /// Memory manager attached to currently running JVM.
        /// </summary>
        internal static PlatformMemoryManager Memory
        {
            get { return MEM; }
        }

        /// <summary>
        /// Destroy JVM.
        /// </summary>
        public static void DestroyJvm()
        {
            lock (MUX)
            {
                if (CTX != null)
                {
                    UU.DestroyJvm(CTX);

                    CTX = null;
                }
            }
        }

        /// <summary>
        /// Create JVM.
        /// </summary>
        /// <returns>JVM.</returns>
        private static void* CreateJvm(GridConfiguration cfg, UnmanagedCallbacks cbs)
        {
            var ggHome = GridGainHome(cfg);

            var cp = CreateClasspath(ggHome, cfg, false);

            var jvmOpts = GetMergedJvmOptions(cfg);
            
            var hasGgHome = !string.IsNullOrWhiteSpace(ggHome);

            var opts = new sbyte*[1 + jvmOpts.Count + (hasGgHome ? 1 : 0)];

            int idx = 0;
                
            opts[idx++] = GridUtils.StringToUtf8Unmanaged(cp);

            if (hasGgHome)
                opts[idx++] = GridUtils.StringToUtf8Unmanaged("-DGRIDGAIN_HOME=" + ggHome);

            foreach (string cfgOpt in jvmOpts)
                opts[idx++] = GridUtils.StringToUtf8Unmanaged(cfgOpt);

            try
            {
                IntPtr mem = Marshal.AllocHGlobal(opts.Length * 8);

                fixed (sbyte** opts0 = opts)
                {
                    PlatformMemoryUtils.CopyMemory(opts0, mem.ToPointer(), opts.Length * 8);
                }

                try
                {
                    return UU.CreateContext(mem.ToPointer(), opts.Length, cbs.CallbacksPointer);
                }
                finally
                {
                    Marshal.FreeHGlobal(mem);
                }
            }
            finally
            {
                foreach (sbyte* opt in opts)
                    Marshal.FreeHGlobal((IntPtr)opt);
            }
        }

        /// <summary>
        /// Gets JvmOptions collection merged with individual properties (Min/Max mem, etc) according to priority.
        /// </summary>
        private static IList<string> GetMergedJvmOptions(GridConfiguration cfg)
        {
            var jvmOpts = cfg.JvmOptions == null ? new List<string>() : cfg.JvmOptions.ToList();

            // JvmInitialMemoryMB / JvmMaxMemoryMB have lower priority than CMD_JVM_OPT
            if (!jvmOpts.Any(opt => opt.StartsWith(CMD_JVM_MIN_MEM_JAVA, StringComparison.OrdinalIgnoreCase)))
                jvmOpts.Add(string.Format("{0}{1}m", CMD_JVM_MIN_MEM_JAVA, cfg.JvmInitialMemoryMB));

            if (!jvmOpts.Any(opt => opt.StartsWith(CMD_JVM_MAX_MEM_JAVA, StringComparison.OrdinalIgnoreCase)))
                jvmOpts.Add(string.Format("{0}{1}m", CMD_JVM_MAX_MEM_JAVA, cfg.JvmMaxMemoryMB));

            return jvmOpts;
        }

        /// <summary>
        /// Create JVM configuration value object.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <returns>JVM configuration.</returns>
        private static JvmConfiguration JvmConfig(GridConfiguration cfg)
        {
            JvmConfiguration jvmCfg = new JvmConfiguration();

            jvmCfg.Home = cfg.GridGainHome;
            jvmCfg.Dll = cfg.JvmDllPath;
            jvmCfg.CP = cfg.JvmClasspath;
            jvmCfg.Options = cfg.JvmOptions;

            return jvmCfg;
        }

        /// <summary>
        /// Append jars from the given path.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="cpStr">Classpath string builder.</param>
        private static void AppendJars(string path, StringBuilder cpStr)
        {
            if (Directory.Exists(path))
            {
                foreach (string jar in Directory.EnumerateFiles(path, "*.jar"))
                {
                    cpStr.Append(jar);
                    cpStr.Append(';');
                }
            }
        }

        /// <summary>
        /// Calculate GridGain home.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <returns></returns>
        internal static string GridGainHome(GridConfiguration cfg)
        {
            var home = cfg == null ? null : cfg.GridGainHome;

            if (string.IsNullOrWhiteSpace(home))
                home = Environment.GetEnvironmentVariable(ENV_GRIDGAIN_HOME);
            else if (!IsGridGainHome(new DirectoryInfo(home)))
                throw new IgniteException(string.Format("GridConfiguration.GridGainHome is not valid: '{0}'", home));

            if (string.IsNullOrWhiteSpace(home))
                home = ResolveGridGainHome();
            else if (!IsGridGainHome(new DirectoryInfo(home)))
                throw new IgniteException(string.Format("{0} is not valid: '{1}'", ENV_GRIDGAIN_HOME, home));

            return home;
        }

        /// <summary>
        /// Automatically resolve GridGain home directory.
        /// </summary>
        /// <returns>GridGain home directory.</returns>
        private static string ResolveGridGainHome()
        {
            var probeDirs = new[]
            {
                Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                Directory.GetCurrentDirectory()
            };

            foreach (var probeDir in probeDirs.Where(x => !string.IsNullOrEmpty(x)))
            {
                var dir = new DirectoryInfo(probeDir);

                while (dir != null)
                {
                    if (IsGridGainHome(dir))
                        return dir.FullName;

                    dir = dir.Parent;
                }
            }

            return null;
        }

        /// <summary>
        /// Determines whether specified dir looks like a GridGain home.
        /// </summary>
        /// <param name="dir">Directory.</param>
        /// <returns>Value indicating whether specified dir looks like a GridGain home.</returns>
        private static bool IsGridGainHome(DirectoryInfo dir)
        {
            return dir.Exists && dir.EnumerateDirectories().Count(x => x.Name == "examples" || x.Name == "bin") == 2;
        }

        /// <summary>
        /// Creates classpath from the given configuration, or default classpath if given config is null.
        /// </summary>
        /// <param name="cfg">The configuration.</param>
        /// <param name="forceTestClasspath">Append test directories even if <see cref="ENV_GRIDGAIN_NATIVE_TEST_CLASSPATH" /> is not set.</param>
        /// <returns>
        /// Classpath string.
        /// </returns>
        internal static string CreateClasspath(GridConfiguration cfg = null, bool forceTestClasspath = false)
        {
            return CreateClasspath(GridGainHome(cfg), cfg, forceTestClasspath);
        }

        /// <summary>
        /// Creates classpath from the given configuration, or default classpath if given config is null.
        /// </summary>
        /// <param name="ggHome">The home dir.</param>
        /// <param name="cfg">The configuration.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        ///     <see cref="ENV_GRIDGAIN_NATIVE_TEST_CLASSPATH" /> is not set.</param>
        /// <returns>
        /// Classpath string.
        /// </returns>
        private static string CreateClasspath(string ggHome, GridConfiguration cfg, bool forceTestClasspath)
        {
            var cpStr = new StringBuilder();

            if (cfg != null && cfg.JvmClasspath != null)
            {
                cpStr.Append(cfg.JvmClasspath);

                if (!cfg.JvmClasspath.EndsWith(";"))
                    cpStr.Append(';');
            }

            if (!string.IsNullOrWhiteSpace(ggHome))
                AppendHomeClasspath(ggHome, forceTestClasspath, cpStr);

            return CLASSPATH_PREFIX + cpStr;
        }

        /// <summary>
        /// Appends classpath from home directory, if it is defined.
        /// </summary>
        /// <param name="ggHome">The home dir.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        ///     <see cref="ENV_GRIDGAIN_NATIVE_TEST_CLASSPATH"/> is not set.</param>
        /// <param name="cpStr">The classpath string.</param>
        private static void AppendHomeClasspath(string ggHome, bool forceTestClasspath, StringBuilder cpStr)
        {
            // Append test directories (if needed) first, because otherwise build *.jar will be picked first.
            if (forceTestClasspath || "true".Equals(Environment.GetEnvironmentVariable(ENV_GRIDGAIN_NATIVE_TEST_CLASSPATH)))
            {
                AppendTestClasses(ggHome + "\\examples", cpStr);
                AppendTestClasses(ggHome + "\\modules", cpStr);
                AppendTestClasses(ggHome + "\\..\\incubator-ignite\\examples", cpStr);
                AppendTestClasses(ggHome + "\\..\\incubator-ignite\\modules", cpStr);
            }

            string ggLibs = ggHome + "\\libs";

            AppendJars(ggLibs, cpStr);

            if (Directory.Exists(ggLibs))
            {
                foreach (string dir in Directory.EnumerateDirectories(ggLibs))
                {
                    if (!dir.EndsWith("optional"))
                        AppendJars(dir, cpStr);
                }
            }
        }

        /// <summary>
        /// Append target (compile) directories to classpath (for testing purposes only).
        /// </summary>
        /// <param name="path">Path</param>
        /// <param name="cp">Classpath builder.</param>
        private static void AppendTestClasses(string path, StringBuilder cp)
        {
            if (Directory.Exists(path))
            {
                AppendTestClasses0(path, cp);

                foreach (string moduleDir in Directory.EnumerateDirectories(path))
                    AppendTestClasses0(moduleDir, cp);
            }
        }

        /// <summary>
        /// Internal routine to append classes and jars from eploded directory.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="cp">Classpath builder.</param>
        private static void AppendTestClasses0(string path, StringBuilder cp)
        {
            if (path.EndsWith("rest-http", StringComparison.OrdinalIgnoreCase))
                return;
            
            if (Directory.Exists(path + "\\target\\classes"))
                cp.Append(path + "\\target\\classes;");

            if (Directory.Exists(path + "\\target\\test-classes"))
                cp.Append(path + "\\target\\test-classes;");

            if (Directory.Exists(path + "\\target\\libs"))
                AppendJars(path + "\\target\\libs", cp);
        }

        /// <summary>
        /// JVM configuration.
        /// </summary>
        private class JvmConfiguration
        {
            /// <summary>
            ///
            /// </summary>
            public string Home
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            public string Dll
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            public string CP
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            public ICollection<string> Options
            {
                get;
                set;
            }

            /** <inheritDoc /> */
            public override int GetHashCode()
            {
                return 0;
            }

            /** <inheritDoc /> */
            [SuppressMessage("ReSharper", "FunctionComplexityOverflow")]
            public override bool Equals(object obj)
            {
                JvmConfiguration other = obj as JvmConfiguration;

                if (other == null) 
                    return false;

                if (!string.Equals(Home, other.Home, StringComparison.OrdinalIgnoreCase))
                    return false;

                if (!string.Equals(CP, other.CP, StringComparison.OrdinalIgnoreCase))
                    return false;

                if (!string.Equals(Dll, other.Dll, StringComparison.OrdinalIgnoreCase))
                    return false;

                return (Options == null && other.Options == null) ||
                       (Options != null && other.Options != null && Options.Count == other.Options.Count
                        && !Options.Except(other.Options).Any());
            }

            /** <inheritDoc /> */
            public override string ToString()
            {
                StringBuilder sb = new StringBuilder("[GridGainHome=" + Home + ", JvmDllPath=" + Dll);

                if (Options != null && Options.Count > 0)
                {
                    sb.Append(", JvmOptions=[");

                    bool first = true;

                    foreach (string opt in Options)
                    {
                        if (first)
                            first = false;
                        else
                            sb.Append(", ");

                        sb.Append(opt);
                    }

                    sb.Append(']');
                }

                sb.Append(", Classpath=" + CP + ']');

                return sb.ToString();
            }
        }
    }
}

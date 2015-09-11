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
    internal static unsafe class IgniteManager
    {
        /** Environment variable: IGNITE_HOME. */
        internal const string EnvIgniteHome = "IGNITE_HOME";

        /** Environment variable: whether to set test classpath or not. */
        internal const string EnvIgniteNativeTestClasspath = "IGNITE_NATIVE_TEST_CLASSPATH";
        
        /** Classpath prefix. */
        private const string ClasspathPrefix = "-Djava.class.path=";

        /** Java Command line argument: Xms. Case sensitive. */
        private const string CmdJvmMinMemJava = "-Xms";

        /** Java Command line argument: Xmx. Case sensitive. */
        private const string CmdJvmMaxMemJava = "-Xmx";

        /** Monitor for DLL load synchronization. */
        private static readonly object SyncRoot = new object();

        /** First created context. */
        private static void* _ctx;

        /** Configuration used on JVM start. */
        private static JvmConfiguration _jvmCfg;

        /** Memory manager. */
        private static PlatformMemoryManager _mem;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static IgniteManager()
        {
            // No-op.
        }

        /// <summary>
        /// Create JVM.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="cbs">Callbacks.</param>
        /// <returns>Context.</returns>
        internal static void* GetContext(IgniteConfiguration cfg, UnmanagedCallbacks cbs)
        {
            lock (SyncRoot)
            {
                // 1. Warn about possible configuration inconsistency.
                JvmConfiguration jvmCfg = JvmConfig(cfg);

                if (!cfg.SuppressWarnings && _jvmCfg != null)
                {
                    if (!_jvmCfg.Equals(jvmCfg))
                    {
                        Console.WriteLine("Attempting to start Ignite node with different Java " +
                            "configuration; current Java configuration will be ignored (consider " +
                            "starting node in separate process) [oldConfig=" + _jvmCfg +
                            ", newConfig=" + jvmCfg + ']');
                    }
                }

                // 2. Create unmanaged pointer.
                void* ctx = CreateJvm(cfg, cbs);

                cbs.SetContext(ctx);

                // 3. If this is the first JVM created, preserve it.
                if (_ctx == null)
                {
                    _ctx = ctx;
                    _jvmCfg = jvmCfg;
                    _mem = new PlatformMemoryManager(1024);
                }

                return ctx;
            }
        }
        
        /// <summary>
        /// Memory manager attached to currently running JVM.
        /// </summary>
        internal static PlatformMemoryManager Memory
        {
            get { return _mem; }
        }

        /// <summary>
        /// Destroy JVM.
        /// </summary>
        public static void DestroyJvm()
        {
            lock (SyncRoot)
            {
                if (_ctx != null)
                {
                    UU.DestroyJvm(_ctx);

                    _ctx = null;
                }
            }
        }

        /// <summary>
        /// Create JVM.
        /// </summary>
        /// <returns>JVM.</returns>
        private static void* CreateJvm(IgniteConfiguration cfg, UnmanagedCallbacks cbs)
        {
            var ggHome = GetIgniteHome(cfg);

            var cp = CreateClasspath(ggHome, cfg, false);

            var jvmOpts = GetMergedJvmOptions(cfg);
            
            var hasGgHome = !string.IsNullOrWhiteSpace(ggHome);

            var opts = new sbyte*[1 + jvmOpts.Count + (hasGgHome ? 1 : 0)];

            int idx = 0;
                
            opts[idx++] = IgniteUtils.StringToUtf8Unmanaged(cp);

            if (hasGgHome)
                opts[idx++] = IgniteUtils.StringToUtf8Unmanaged("-DIGNITE_HOME=" + ggHome);

            foreach (string cfgOpt in jvmOpts)
                opts[idx++] = IgniteUtils.StringToUtf8Unmanaged(cfgOpt);

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
        private static IList<string> GetMergedJvmOptions(IgniteConfiguration cfg)
        {
            var jvmOpts = cfg.JvmOptions == null ? new List<string>() : cfg.JvmOptions.ToList();

            // JvmInitialMemoryMB / JvmMaxMemoryMB have lower priority than CMD_JVM_OPT
            if (!jvmOpts.Any(opt => opt.StartsWith(CmdJvmMinMemJava, StringComparison.OrdinalIgnoreCase)))
                jvmOpts.Add(string.Format("{0}{1}m", CmdJvmMinMemJava, cfg.JvmInitialMemoryMb));

            if (!jvmOpts.Any(opt => opt.StartsWith(CmdJvmMaxMemJava, StringComparison.OrdinalIgnoreCase)))
                jvmOpts.Add(string.Format("{0}{1}m", CmdJvmMaxMemJava, cfg.JvmMaxMemoryMb));

            return jvmOpts;
        }

        /// <summary>
        /// Create JVM configuration value object.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <returns>JVM configuration.</returns>
        private static JvmConfiguration JvmConfig(IgniteConfiguration cfg)
        {
            return new JvmConfiguration
            {
                Home = cfg.IgniteHome,
                Dll = cfg.JvmDllPath,
                Classpath = cfg.JvmClasspath,
                Options = cfg.JvmOptions
            };
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
        /// Calculate Ignite home.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <returns></returns>
        internal static string GetIgniteHome(IgniteConfiguration cfg)
        {
            var home = cfg == null ? null : cfg.IgniteHome;

            if (string.IsNullOrWhiteSpace(home))
                home = Environment.GetEnvironmentVariable(EnvIgniteHome);
            else if (!IsIgniteHome(new DirectoryInfo(home)))
                throw new IgniteException(string.Format("IgniteConfiguration.IgniteHome is not valid: '{0}'", home));

            if (string.IsNullOrWhiteSpace(home))
                home = ResolveIgniteHome();
            else if (!IsIgniteHome(new DirectoryInfo(home)))
                throw new IgniteException(string.Format("{0} is not valid: '{1}'", EnvIgniteHome, home));

            return home;
        }

        /// <summary>
        /// Automatically resolve Ignite home directory.
        /// </summary>
        /// <returns>Ignite home directory.</returns>
        private static string ResolveIgniteHome()
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
                    if (IsIgniteHome(dir))
                        return dir.FullName;

                    dir = dir.Parent;
                }
            }

            return null;
        }

        /// <summary>
        /// Determines whether specified dir looks like a Ignite home.
        /// </summary>
        /// <param name="dir">Directory.</param>
        /// <returns>Value indicating whether specified dir looks like a Ignite home.</returns>
        private static bool IsIgniteHome(DirectoryInfo dir)
        {
            return dir.Exists && dir.EnumerateDirectories().Count(x => x.Name == "examples" || x.Name == "bin") == 2;
        }

        /// <summary>
        /// Creates classpath from the given configuration, or default classpath if given config is null.
        /// </summary>
        /// <param name="cfg">The configuration.</param>
        /// <param name="forceTestClasspath">Append test directories even if <see cref="EnvIgniteNativeTestClasspath" /> is not set.</param>
        /// <returns>
        /// Classpath string.
        /// </returns>
        internal static string CreateClasspath(IgniteConfiguration cfg = null, bool forceTestClasspath = false)
        {
            return CreateClasspath(GetIgniteHome(cfg), cfg, forceTestClasspath);
        }

        /// <summary>
        /// Creates classpath from the given configuration, or default classpath if given config is null.
        /// </summary>
        /// <param name="ggHome">The home dir.</param>
        /// <param name="cfg">The configuration.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        ///     <see cref="EnvIgniteNativeTestClasspath" /> is not set.</param>
        /// <returns>
        /// Classpath string.
        /// </returns>
        private static string CreateClasspath(string ggHome, IgniteConfiguration cfg, bool forceTestClasspath)
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

            return ClasspathPrefix + cpStr;
        }

        /// <summary>
        /// Appends classpath from home directory, if it is defined.
        /// </summary>
        /// <param name="ggHome">The home dir.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        ///     <see cref="EnvIgniteNativeTestClasspath"/> is not set.</param>
        /// <param name="cpStr">The classpath string.</param>
        private static void AppendHomeClasspath(string ggHome, bool forceTestClasspath, StringBuilder cpStr)
        {
            // Append test directories (if needed) first, because otherwise build *.jar will be picked first.
            if (forceTestClasspath || "true".Equals(Environment.GetEnvironmentVariable(EnvIgniteNativeTestClasspath)))
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
            /// Gets or sets the home.
            /// </summary>
            public string Home { get; set; }

            /// <summary>
            /// Gets or sets the DLL.
            /// </summary>
            public string Dll { get; set; }

            /// <summary>
            /// Gets or sets the cp.
            /// </summary>
            public string Classpath { get; set; }

            /// <summary>
            /// Gets or sets the options.
            /// </summary>
            public ICollection<string> Options { get; set; }

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

                if (!string.Equals(Classpath, other.Classpath, StringComparison.OrdinalIgnoreCase))
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
                var sb = new StringBuilder("[IgniteHome=" + Home + ", JvmDllPath=" + Dll);

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

                sb.Append(", Classpath=" + Classpath + ']');

                return sb.ToString();
            }
        }
    }
}

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
    using System.Globalization;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Log;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native interface manager.
    /// </summary>
    public static unsafe class IgniteManager
    {
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
        private static readonly PlatformMemoryManager Mem = new PlatformMemoryManager(1024);

        /// <summary>
        /// Create JVM.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="cbs">Callbacks.</param>
        /// <param name="log"></param>
        /// <returns>Context.</returns>
        internal static void CreateJvmContext(IgniteConfiguration cfg, UnmanagedCallbacks cbs, ILogger log)
        {
            lock (SyncRoot)
            {
                // 1. Warn about possible configuration inconsistency.
                JvmConfiguration jvmCfg = JvmConfig(cfg);

                if (!cfg.SuppressWarnings && _jvmCfg != null)
                {
                    if (!_jvmCfg.Equals(jvmCfg))
                    {
                        log.Warn("Attempting to start Ignite node with different Java " +
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
                }
            }
        }
        
        /// <summary>
        /// Memory manager attached to currently running JVM.
        /// </summary>
        internal static PlatformMemoryManager Memory
        {
            get { return Mem; }
        }

        /// <summary>
        /// Blocks until JVM stops.
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
            var cp = Classpath.CreateClasspath(cfg);

            var jvmOpts = GetMergedJvmOptions(cfg);
            
            var opts = new sbyte*[1 + jvmOpts.Count];

            int idx = 0;
                
            opts[idx++] = IgniteUtils.StringToUtf8Unmanaged(cp);

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
            if (!jvmOpts.Any(opt => opt.StartsWith(CmdJvmMinMemJava, StringComparison.OrdinalIgnoreCase)) &&
                cfg.JvmInitialMemoryMb != IgniteConfiguration.DefaultJvmInitMem)
                jvmOpts.Add(string.Format(CultureInfo.InvariantCulture, "{0}{1}m", CmdJvmMinMemJava, cfg.JvmInitialMemoryMb));

            if (!jvmOpts.Any(opt => opt.StartsWith(CmdJvmMaxMemJava, StringComparison.OrdinalIgnoreCase)) &&
                cfg.JvmMaxMemoryMb != IgniteConfiguration.DefaultJvmMaxMem)
                jvmOpts.Add(string.Format(CultureInfo.InvariantCulture, "{0}{1}m", CmdJvmMaxMemJava, cfg.JvmMaxMemoryMb));

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

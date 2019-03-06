/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Native interface manager.
    /// </summary>
    internal static class IgniteManager
    {
        /** Java Command line argument: Xms. Case sensitive. */
        private const string CmdJvmMinMemJava = "-Xms";

        /** Java Command line argument: Xmx. Case sensitive. */
        private const string CmdJvmMaxMemJava = "-Xmx";

        /** Java Command line argument: file.encoding. Case sensitive. */
        private const string CmdJvmFileEncoding = "-Dfile.encoding=";

        /** Monitor for DLL load synchronization. */
        private static readonly object SyncRoot = new object();

        /** Configuration used on JVM start. */
        private static JvmConfiguration _jvmCfg;

        /** Memory manager. */
        private static readonly PlatformMemoryManager Mem = new PlatformMemoryManager(1024);

        /// <summary>
        /// Create JVM.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="log">Logger</param>
        /// <returns>Callback context.</returns>
        internal static UnmanagedCallbacks CreateJvmContext(IgniteConfiguration cfg, ILogger log)
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
                var jvm = CreateJvm(cfg, log);

                if (cfg.RedirectJavaConsoleOutput)
                {
                    jvm.EnableJavaConsoleWriter();
                }

                var cbs = new UnmanagedCallbacks(log, jvm);
                jvm.RegisterCallbacks(cbs);

                // 3. If this is the first JVM created, preserve configuration.
                if (_jvmCfg == null)
                {
                    _jvmCfg = jvmCfg;
                }

                return cbs;
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
        /// Create JVM.
        /// </summary>
        /// <returns>JVM.</returns>
        private static Jvm CreateJvm(IgniteConfiguration cfg, ILogger log)
        {
            // Do not bother with classpath when JVM exists.
            var jvm = Jvm.Get(true);

            if (jvm != null)
            {
                return jvm;
            }

            var cp = Classpath.CreateClasspath(cfg, log: log);

            var jvmOpts = GetMergedJvmOptions(cfg);

            jvmOpts.Add(cp);

            return Jvm.GetOrCreate(jvmOpts);
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

            if (!jvmOpts.Any(opt => opt.StartsWith(CmdJvmFileEncoding, StringComparison.Ordinal)))
                jvmOpts.Add(string.Format(CultureInfo.InvariantCulture, "{0}UTF-8", CmdJvmFileEncoding));

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

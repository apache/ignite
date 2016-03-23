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

namespace Apache.Ignite.Config
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core;

    /// <summary>
    /// Configurator which uses arguments array.
    /// </summary>
    internal static class ArgsConfigurator
    {
        /** Command line argument: Ignite home. */
        private static readonly string CmdIgniteHome = "-IgniteHome=";

        /** Command line argument: Spring config URL. */
        private static readonly string CmdSpringCfgUrl = "-SpringConfigUrl=";

        /** Command line argument: Path to JVM dll. */
        private static readonly string CmdJvmDll = "-JvmDll=";

        /** Command line argument: JVM classpath. */
        private static readonly string CmdJvmClasspath = "-JvmClasspath=";

        /** Command line argument: suppress warnings flag. */
        private static readonly string CmdSuppressWarn = "-SuppressWarnings=";

        /** Command line argument: JVM option prefix. */
        private static readonly string CmdJvmOpt = "-J";

        /** Command line argument: assembly. */
        private static readonly string CmdAssembly = "-Assembly=";

        /** Command line argument: JvmInitialMemoryMB. */
        private static readonly string CmdJvmMinMem = "-JvmInitialMemoryMB=";

        /** Command line argument: JvmMaxMemoryMB. */
        private static readonly string CmdJvmMaxMem = "-JvmMaxMemoryMB=";

        /// <summary>
        /// Convert configuration to arguments.
        /// </summary>
        /// <param name="cfg"></param>
        /// <returns></returns>
        internal static string[] ToArgs(IgniteConfiguration cfg)
        {
            var args = new List<string>();

            if (cfg.IgniteHome != null)
                args.Add(CmdIgniteHome + cfg.IgniteHome);

            if (cfg.SpringConfigUrl != null)
                args.Add(CmdSpringCfgUrl + cfg.SpringConfigUrl);

            if (cfg.JvmDllPath != null)
                args.Add(CmdJvmDll + cfg.JvmDllPath);

            if (cfg.JvmClasspath != null)
                args.Add(CmdJvmClasspath + cfg.JvmClasspath);
            
            if (cfg.SuppressWarnings)
                args.Add(CmdSuppressWarn + bool.TrueString);

            if (cfg.JvmOptions != null)
            {
                foreach (var jvmOpt in cfg.JvmOptions)
                    args.Add(CmdJvmOpt + jvmOpt);
            }

            if (cfg.Assemblies != null)
            {
                foreach (var assembly in cfg.Assemblies)
                    args.Add(CmdAssembly + assembly);
            }

            args.Add(CmdJvmMinMem + cfg.JvmInitialMemoryMb);
            args.Add(CmdJvmMaxMem + cfg.JvmMaxMemoryMb);

            return args.ToArray();
        }

        /** <inheritDoc /> */
        public static void Configure(IgniteConfiguration cfg, string[] src)
        {
            var jvmOpts = new List<string>();
            var assemblies = new List<string>();

            foreach (var arg in src)
            {
                Func<string, bool> argStartsWith = x => arg.StartsWith(x, StringComparison.OrdinalIgnoreCase);

                if (argStartsWith(CmdIgniteHome))
                    cfg.IgniteHome = arg.Substring(CmdIgniteHome.Length);
                else if (argStartsWith(CmdSpringCfgUrl))
                    cfg.SpringConfigUrl = arg.Substring(CmdSpringCfgUrl.Length);
                else if (argStartsWith(CmdJvmDll))
                    cfg.JvmDllPath = arg.Substring(CmdJvmDll.Length);
                else if (argStartsWith(CmdJvmClasspath))
                    cfg.JvmClasspath = arg.Substring(CmdJvmClasspath.Length);
                else if (argStartsWith(CmdSuppressWarn))
                {
                    var val = arg.Substring(CmdSuppressWarn.Length);

                    cfg.SuppressWarnings = bool.TrueString.Equals(val, StringComparison.OrdinalIgnoreCase);
                }
                else if (argStartsWith(CmdJvmMinMem))
                    cfg.JvmInitialMemoryMb = ConfigValueParser.ParseInt(arg.Substring(CmdJvmMinMem.Length),
                        CmdJvmMinMem);
                else if (argStartsWith(CmdJvmMaxMem))
                    cfg.JvmMaxMemoryMb = ConfigValueParser.ParseInt(arg.Substring(CmdJvmMaxMem.Length),
                        CmdJvmMaxMem);
                else if (argStartsWith(CmdJvmOpt))
                    jvmOpts.Add(arg.Substring(CmdJvmOpt.Length));
                else if (argStartsWith(CmdAssembly))
                    assemblies.Add(arg.Substring(CmdAssembly.Length));
            }

            if (jvmOpts.Count > 0)
            {
                if (cfg.JvmOptions == null)
                    cfg.JvmOptions = jvmOpts;
                else
                    jvmOpts.ForEach(val => cfg.JvmOptions.Add(val));
            }

            if (assemblies.Count > 0)
            {
                if (cfg.Assemblies == null)
                    cfg.Assemblies = assemblies;
                else
                    assemblies.ForEach(val => cfg.Assemblies.Add(val));
            }
        }
    }
}

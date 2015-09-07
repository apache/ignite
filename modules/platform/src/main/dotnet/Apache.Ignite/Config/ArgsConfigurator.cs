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
    using System.Collections.Generic;
    using Apache.Ignite.Core;

    /// <summary>
    /// Configurator which uses arguments array.
    /// </summary>
    internal class ArgsConfigurator : IConfigurator<string[]>
    {
        /** Command line argument: Ignite home. */
        private static readonly string CmdIgniteHome = "-IgniteHome=".ToLower();

        /** Command line argument: Spring config URL. */
        private static readonly string CmdSpringCfgUrl = "-SpringConfigUrl=".ToLower();

        /** Command line argument: Path to JVM dll. */
        private static readonly string CmdJvmDll = "-JvmDll=".ToLower();

        /** Command line argument: JVM classpath. */
        private static readonly string CmdJvmClasspath = "-JvmClasspath=".ToLower();

        /** Command line argument: suppress warnings flag. */
        private static readonly string CmdSuppressWarn = "-SuppressWarnings=".ToLower();

        /** Command line argument: JVM option prefix. */
        private static readonly string CmdJvmOpt = "-J".ToLower();

        /** Command line argument: assembly. */
        private static readonly string CmdAssembly = "-Assembly=".ToLower();

        /** Command line argument: JvmInitialMemoryMB. */
        private static readonly string CmdJvmMinMem = "-JvmInitialMemoryMB=".ToLower();

        /** Command line argument: JvmMaxMemoryMB. */
        private static readonly string CmdJvmMaxMem = "-JvmMaxMemoryMB=".ToLower();

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

        /// <summary>
        /// Convert arguments to configuration.
        /// </summary>
        /// <param name="args">Arguments.</param>
        /// <returns>Configuration.</returns>
        internal static IgniteConfiguration FromArgs(string[] args)
        {
            var cfg = new IgniteConfiguration();

            new ArgsConfigurator().Configure(cfg, args);

            return cfg;
        }

        /** <inheritDoc /> */
        public void Configure(IgniteConfiguration cfg, string[] src)
        {
            var jvmOpts = new List<string>();
            var assemblies = new List<string>();

            foreach (var arg in src)
            {
                var argLow = arg.ToLower();

                if (argLow.StartsWith(CmdIgniteHome))
                    cfg.IgniteHome = arg.Substring(CmdIgniteHome.Length);
                else if (argLow.StartsWith(CmdSpringCfgUrl))
                    cfg.SpringConfigUrl = arg.Substring(CmdSpringCfgUrl.Length);
                else if (argLow.StartsWith(CmdJvmDll))
                    cfg.JvmDllPath = arg.Substring(CmdJvmDll.Length);
                else if (argLow.StartsWith(CmdJvmClasspath))
                    cfg.JvmClasspath = arg.Substring(CmdJvmClasspath.Length);
                else if (argLow.StartsWith(CmdSuppressWarn))
                {
                    var val = arg.Substring(CmdSuppressWarn.Length);

                    cfg.SuppressWarnings = bool.TrueString.ToLower().Equals(val.ToLower());
                }
                else if (argLow.StartsWith(CmdJvmMinMem))
                    cfg.JvmInitialMemoryMb = ConfigValueParser.ParseInt(arg.Substring(CmdJvmMinMem.Length),
                        CmdJvmMinMem);
                else if (argLow.StartsWith(CmdJvmMaxMem))
                    cfg.JvmMaxMemoryMb = ConfigValueParser.ParseInt(arg.Substring(CmdJvmMaxMem.Length),
                        CmdJvmMaxMem);
                else if (argLow.StartsWith(CmdJvmOpt))
                    jvmOpts.Add(arg.Substring(CmdJvmOpt.Length));
                else if (argLow.StartsWith(CmdAssembly))
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

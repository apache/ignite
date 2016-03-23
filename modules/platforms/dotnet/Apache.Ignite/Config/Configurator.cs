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
    internal static class Configurator
    {
        /** Command line argument: Ignite home. */
        public const string CmdIgniteHome = "IgniteHome";

        /** Command line argument: Spring config URL. */
        private const string CmdSpringCfgUrl = "SpringConfigUrl";

        /** Command line argument: Path to JVM dll. */
        private const string CmdJvmDll = "JvmDll";

        /** Command line argument: JVM classpath. */
        private const string CmdJvmClasspath = "JvmClasspath";

        /** Command line argument: suppress warnings flag. */
        private const string CmdSuppressWarn = "SuppressWarnings";

        /** Command line argument: JVM option prefix. */
        public const string CmdJvmOpt = "J";

        /** Command line argument: assembly. */
        private const string CmdAssembly = "Assembly";

        /** Command line argument: JvmInitialMemoryMB. */
        private const string CmdJvmMinMem = "JvmInitialMemoryMB";

        /** Command line argument: JvmMaxMemoryMB. */
        private const string CmdJvmMaxMem = "JvmMaxMemoryMB";

        /** <inheritDoc /> */
        public static IgniteConfiguration GetConfiguration(IEnumerable<Tuple<string, string>> args)
        {
            var jvmOpts = new List<string>();
            var assemblies = new List<string>();

            var cfg = new IgniteConfiguration();

            // TODO: Start from config section somehow

            foreach (var arg in args)
            {
                Func<string, bool> argStartsWith = x => arg.Item1.Equals(x, StringComparison.OrdinalIgnoreCase);

                if (argStartsWith(CmdIgniteHome))
                    cfg.IgniteHome = arg.Item2;
                else if (argStartsWith(CmdSpringCfgUrl))
                    cfg.SpringConfigUrl = arg.Item2;
                else if (argStartsWith(CmdJvmDll))
                    cfg.JvmDllPath = arg.Item2;
                else if (argStartsWith(CmdJvmClasspath))
                    cfg.JvmClasspath = arg.Item2;
                else if (argStartsWith(CmdSuppressWarn))
                {
                    cfg.SuppressWarnings = bool.TrueString.Equals(arg.Item2, StringComparison.OrdinalIgnoreCase);
                }
                else if (argStartsWith(CmdJvmMinMem))
                    cfg.JvmInitialMemoryMb = ConfigValueParser.ParseInt(arg.Item2, CmdJvmMinMem);
                else if (argStartsWith(CmdJvmMaxMem))
                    cfg.JvmMaxMemoryMb = ConfigValueParser.ParseInt(arg.Item2, CmdJvmMaxMem);
                else if (argStartsWith(CmdJvmOpt))
                    jvmOpts.Add(arg.Item2);
                else if (argStartsWith(CmdAssembly))
                    assemblies.Add(arg.Item2);
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

            return cfg;
        }
    }
}

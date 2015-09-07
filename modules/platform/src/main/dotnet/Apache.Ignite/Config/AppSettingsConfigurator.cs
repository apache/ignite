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
    using System.Collections.Specialized;
    using Apache.Ignite.Core;

    /// <summary>
    /// Configurator which uses application configuration.
    /// </summary>
    internal class AppSettingsConfigurator : IConfigurator<NameValueCollection>
    {
        /** Common configuration property prefix. */
        private static readonly string CfgPrefix = "Ignite.".ToLower();

        /** Configuration property: Ignite home. */
        private static readonly string CfgHome = "Home".ToLower();

        /** Configuration property: Spring config URL. */
        private static readonly string CfgSpringCfgUrl = "SpringConfigUrl".ToLower();

        /** Configuration property: Path to JVM dll. */
        private static readonly string CfgJvmDll = "JvmDll".ToLower();

        /** Configuration property: JVM classpath. */
        private static readonly string CfgJvmClasspath = "JvmClasspath".ToLower();

        /** Configuration property: suppress warnings flag. */
        private static readonly string CfgSuppressWarn = "SuppressWarnings".ToLower();

        /** Configuration property: JVM option prefix. */
        private static readonly string CfgJvmOptPrefix = "JvmOption".ToLower();

        /** Configuration property: assembly prefix. */
        private static readonly string CfgAssemblyPrefix = "Assembly".ToLower();

        /** Configuration property: JVM min memory. */
        private static readonly string CfgJvmMinMem = "JvmInitialMemoryMB".ToLower();

        /** Configuration property: JVM max memory. */
        private static readonly string CfgJvmMaxMem = "JvmMaxMemoryMB".ToLower();

        /** <inheritDoc /> */
        public void Configure(IgniteConfiguration cfg, NameValueCollection src)
        {
            var jvmOpts = new List<string>();
            var assemblies = new List<string>();

            foreach (string key in src.Keys)
            {
                var key0 = key.ToLower();

                if (key0.StartsWith(CfgPrefix))
                {
                    key0 = key0.Substring(CfgPrefix.Length);

                    var val = src[key];

                    if (CfgHome.Equals(key0))
                        cfg.IgniteHome = val;
                    else if (CfgSpringCfgUrl.Equals(key0))
                        cfg.SpringConfigUrl = val;
                    else if (CfgJvmDll.Equals(key0))
                        cfg.JvmDllPath = val;
                    else if (CfgJvmClasspath.Equals(key0))
                        cfg.JvmClasspath = val;
                    else if (CfgSuppressWarn.Equals(key0))
                        cfg.SuppressWarnings = val != null && bool.TrueString.ToLower().Equals(val.ToLower());
                    else if (key0.StartsWith(CfgJvmOptPrefix))
                        jvmOpts.Add(val);
                    else if (key0.StartsWith(CfgAssemblyPrefix))
                        assemblies.Add(val);
                    else if (CfgJvmMinMem.Equals(key0))
                        cfg.JvmInitialMemoryMb = ConfigValueParser.ParseInt(val, key);
                    else if (CfgJvmMaxMem.Equals(key0))
                        cfg.JvmMaxMemoryMb = ConfigValueParser.ParseInt(val, key);
                }
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

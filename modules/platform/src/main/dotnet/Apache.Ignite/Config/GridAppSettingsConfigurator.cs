/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Config
{
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using Apache.Ignite.Core;

    /// <summary>
    /// Configurator which uses application configuration.
    /// </summary>
    internal class GridAppSettingsConfigurator : IGridConfigurator<NameValueCollection>
    {
        /** Common configuration property prefix. */
        private static readonly string CfgPrefix = "GridGain.".ToLower();

        /** Configuration property: GridGain home. */
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
            List<string> jvmOpts = new List<string>();
            List<string> assemblies = new List<string>();

            foreach (string key in src.Keys)
            {
                string key0 = key.ToLower();

                if (key0.StartsWith(CfgPrefix))
                {
                    key0 = key0.Substring(CfgPrefix.Length);

                    string val = src[key];

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
                        cfg.JvmInitialMemoryMb = GridConfigValueParser.ParseInt(val, key);
                    else if (CfgJvmMaxMem.Equals(key0))
                        cfg.JvmMaxMemoryMb = GridConfigValueParser.ParseInt(val, key);
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

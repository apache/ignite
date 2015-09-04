/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Runner.Config
{
    using System.Collections.Generic;
    using System.Collections.Specialized;

    /// <summary>
    /// Configurator which uses application configuration.
    /// </summary>
    internal class GridAppSettingsConfigurator : IGridConfigurator<NameValueCollection>
    {
        /** Common configuration property prefix. */
        private static readonly string CFG_PREFIX = "GridGain.".ToLower();

        /** Configuration property: GridGain home. */
        private static readonly string CFG_HOME = "Home".ToLower();

        /** Configuration property: Spring config URL. */
        private static readonly string CFG_SPRING_CFG_URL = "SpringConfigUrl".ToLower();

        /** Configuration property: Path to JVM dll. */
        private static readonly string CFG_JVM_DLL = "JvmDll".ToLower();

        /** Configuration property: JVM classpath. */
        private static readonly string CFG_JVM_CLASSPATH = "JvmClasspath".ToLower();

        /** Configuration property: suppress warnings flag. */
        private static readonly string CFG_SUPPRESS_WARN = "SuppressWarnings".ToLower();

        /** Configuration property: JVM option prefix. */
        private static readonly string CFG_JVM_OPT_PREFIX = "JvmOption".ToLower();

        /** Configuration property: assembly prefix. */
        private static readonly string CFG_ASSEMBLY_PREFIX = "Assembly".ToLower();

        /** Configuration property: JVM min memory. */
        private static readonly string CFG_JVM_MIN_MEM = "JvmInitialMemoryMB".ToLower();

        /** Configuration property: JVM max memory. */
        private static readonly string CFG_JVM_MAX_MEM = "JvmMaxMemoryMB".ToLower();

        /** <inheritDoc /> */
        public void Configure(GridConfiguration cfg, NameValueCollection src)
        {
            List<string> jvmOpts = new List<string>();
            List<string> assemblies = new List<string>();

            foreach (string key in src.Keys)
            {
                string key0 = key.ToLower();

                if (key0.StartsWith(CFG_PREFIX))
                {
                    key0 = key0.Substring(CFG_PREFIX.Length);

                    string val = src[key];

                    if (CFG_HOME.Equals(key0))
                        cfg.GridGainHome = val;
                    else if (CFG_SPRING_CFG_URL.Equals(key0))
                        cfg.SpringConfigUrl = val;
                    else if (CFG_JVM_DLL.Equals(key0))
                        cfg.JvmDllPath = val;
                    else if (CFG_JVM_CLASSPATH.Equals(key0))
                        cfg.JvmClasspath = val;
                    else if (CFG_SUPPRESS_WARN.Equals(key0))
                        cfg.SuppressWarnings = val != null && bool.TrueString.ToLower().Equals(val.ToLower());
                    else if (key0.StartsWith(CFG_JVM_OPT_PREFIX))
                        jvmOpts.Add(val);
                    else if (key0.StartsWith(CFG_ASSEMBLY_PREFIX))
                        assemblies.Add(val);
                    else if (CFG_JVM_MIN_MEM.Equals(key0))
                        cfg.JvmInitialMemoryMB = GridConfigValueParser.ParseInt(val, key);
                    else if (CFG_JVM_MAX_MEM.Equals(key0))
                        cfg.JvmMaxMemoryMB = GridConfigValueParser.ParseInt(val, key);
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

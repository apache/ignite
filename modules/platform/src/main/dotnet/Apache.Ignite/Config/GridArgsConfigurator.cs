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
    using Apache.Ignite.Config;
    using Apache.Ignite.Core;

    /// <summary>
    /// Configurator which uses arguments array.
    /// </summary>
    internal class GridArgsConfigurator : IGridConfigurator<string[]>
    {
        /** Command line argument: GridGain home. */
        private static readonly string CMD_GRIDGAIN_HOME = "-IgniteHome=".ToLower();

        /** Command line argument: Spring config URL. */
        private static readonly string CMD_SPRING_CFG_URL = "-SpringConfigUrl=".ToLower();

        /** Command line argument: Path to JVM dll. */
        private static readonly string CMD_JVM_DLL = "-JvmDll=".ToLower();

        /** Command line argument: JVM classpath. */
        private static readonly string CMD_JVM_CLASSPATH = "-JvmClasspath=".ToLower();

        /** Command line argument: suppress warnings flag. */
        private static readonly string CMD_SUPPRESS_WARN = "-SuppressWarnings=".ToLower();

        /** Command line argument: JVM option prefix. */
        private static readonly string CMD_JVM_OPT = "-J".ToLower();

        /** Command line argument: assembly. */
        private static readonly string CMD_ASSEMBLY = "-Assembly=".ToLower();

        /** Command line argument: JvmInitialMemoryMB. */
        private static readonly string CMD_JVM_MIN_MEM = "-JvmInitialMemoryMB=".ToLower();

        /** Command line argument: JvmMaxMemoryMB. */
        private static readonly string CMD_JVM_MAX_MEM = "-JvmMaxMemoryMB=".ToLower();

        /// <summary>
        /// Convert configuration to arguments.
        /// </summary>
        /// <param name="cfg"></param>
        /// <returns></returns>
        internal static string[] ToArgs(IgniteConfiguration cfg)
        {
            List<string> args = new List<string>();

            if (cfg.IgniteHome != null)
                args.Add(CMD_GRIDGAIN_HOME + cfg.IgniteHome);

            if (cfg.SpringConfigUrl != null)
                args.Add(CMD_SPRING_CFG_URL + cfg.SpringConfigUrl);

            if (cfg.JvmDllPath != null)
                args.Add(CMD_JVM_DLL + cfg.JvmDllPath);

            if (cfg.JvmClasspath != null)
                args.Add(CMD_JVM_CLASSPATH + cfg.JvmClasspath);
            
            if (cfg.SuppressWarnings)
                args.Add(CMD_SUPPRESS_WARN + bool.TrueString);

            if (cfg.JvmOptions != null)
            {
                foreach (string jvmOpt in cfg.JvmOptions)
                    args.Add(CMD_JVM_OPT + jvmOpt);
            }

            if (cfg.Assemblies != null)
            {
                foreach (string assembly in cfg.Assemblies)
                    args.Add(CMD_ASSEMBLY + assembly);
            }

            args.Add(CMD_JVM_MIN_MEM + cfg.JvmInitialMemoryMb);
            args.Add(CMD_JVM_MAX_MEM + cfg.JvmMaxMemoryMb);

            return args.ToArray();
        }

        /// <summary>
        /// Convert arguments to configuration.
        /// </summary>
        /// <param name="args">Arguments.</param>
        /// <returns>Configuration.</returns>
        internal static IgniteConfiguration FromArgs(string[] args)
        {
            IgniteConfiguration cfg = new IgniteConfiguration();

            new GridArgsConfigurator().Configure(cfg, args);

            return cfg;
        }

        /** <inheritDoc /> */
        public void Configure(IgniteConfiguration cfg, string[] src)
        {
            List<string> jvmOpts = new List<string>();
            List<string> assemblies = new List<string>();

            foreach (string arg in src)
            {
                string argLow = arg.ToLower();

                if (argLow.StartsWith(CMD_GRIDGAIN_HOME))
                    cfg.IgniteHome = arg.Substring(CMD_GRIDGAIN_HOME.Length);
                else if (argLow.StartsWith(CMD_SPRING_CFG_URL))
                    cfg.SpringConfigUrl = arg.Substring(CMD_SPRING_CFG_URL.Length);
                else if (argLow.StartsWith(CMD_JVM_DLL))
                    cfg.JvmDllPath = arg.Substring(CMD_JVM_DLL.Length);
                else if (argLow.StartsWith(CMD_JVM_CLASSPATH))
                    cfg.JvmClasspath = arg.Substring(CMD_JVM_CLASSPATH.Length);
                else if (argLow.StartsWith(CMD_SUPPRESS_WARN))
                {
                    string val = arg.Substring(CMD_SUPPRESS_WARN.Length);

                    cfg.SuppressWarnings = bool.TrueString.ToLower().Equals(val.ToLower());
                }
                else if (argLow.StartsWith(CMD_JVM_MIN_MEM))
                    cfg.JvmInitialMemoryMb = GridConfigValueParser.ParseInt(arg.Substring(CMD_JVM_MIN_MEM.Length),
                        CMD_JVM_MIN_MEM);
                else if (argLow.StartsWith(CMD_JVM_MAX_MEM))
                    cfg.JvmMaxMemoryMb = GridConfigValueParser.ParseInt(arg.Substring(CMD_JVM_MAX_MEM.Length),
                        CMD_JVM_MAX_MEM);
                else if (argLow.StartsWith(CMD_JVM_OPT))
                    jvmOpts.Add(arg.Substring(CMD_JVM_OPT.Length));
                else if (argLow.StartsWith(CMD_ASSEMBLY))
                    assemblies.Add(arg.Substring(CMD_ASSEMBLY.Length));
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

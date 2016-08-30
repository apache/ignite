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
    using System.Configuration;
    using System.IO;
    using System.Linq;
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
        public const string CmdAssembly = "Assembly";

        /** Command line argument: JvmInitialMemoryMB. */
        private const string CmdJvmMinMem = "JvmInitialMemoryMB";

        /** Command line argument: JvmMaxMemoryMB. */
        private const string CmdJvmMaxMem = "JvmMaxMemoryMB";

        /** Command line argument: Config section name to read config from. */
        private const string CmdConfigSection = "ConfigSectionName";

        /** Command line argument: Config file name to read config section from. */
        private const string CmdConfigFile = "ConfigFileName";

        /** Hidden command line argument: Force test classpath. */
        private const string CmdForceTestClasspath = "ForceTestClasspath";

        /** <inheritDoc /> */
        public static IgniteConfiguration GetConfiguration(Tuple<string, string>[] args)
        {
            var jvmOpts = new List<string>();
            var assemblies = new List<string>();

            var cfg = ReadConfigurationSection(args) ?? new IgniteConfiguration();

            foreach (var arg in args)
            {
                var arg0 = arg;  // copy captured variable
                Func<string, bool> argIs = x => arg0.Item1.Equals(x, StringComparison.OrdinalIgnoreCase);

                if (argIs(CmdIgniteHome))
                    cfg.IgniteHome = arg.Item2;
                else if (argIs(CmdSpringCfgUrl))
                    cfg.SpringConfigUrl = arg.Item2;
                else if (argIs(CmdJvmDll))
                    cfg.JvmDllPath = arg.Item2;
                else if (argIs(CmdJvmClasspath))
                    cfg.JvmClasspath = arg.Item2;
                else if (argIs(CmdSuppressWarn))
                {
                    cfg.SuppressWarnings = bool.TrueString.Equals(arg.Item2, StringComparison.OrdinalIgnoreCase);
                }
                else if (argIs(CmdJvmMinMem))
                    cfg.JvmInitialMemoryMb = ConfigValueParser.ParseInt(arg.Item2, CmdJvmMinMem);
                else if (argIs(CmdJvmMaxMem))
                    cfg.JvmMaxMemoryMb = ConfigValueParser.ParseInt(arg.Item2, CmdJvmMaxMem);
                else if (argIs(CmdJvmOpt))
                    jvmOpts.Add(arg.Item2);
                else if (argIs(CmdAssembly))
                    assemblies.Add(arg.Item2);
                else if (argIs(CmdForceTestClasspath) && arg.Item2 == "true")
                    Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");
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

        /// <summary>
        /// Reads the configuration section.
        /// </summary>
        private static IgniteConfiguration ReadConfigurationSection(Tuple<string, string>[] args)
        {
            var fileName = FindValue(args, CmdConfigFile);
            var sectionName = FindValue(args, CmdConfigSection);

            if (string.IsNullOrEmpty(fileName) && string.IsNullOrEmpty(sectionName))
                return null;

            var cfg = string.IsNullOrEmpty(fileName)
                ? ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None)
                : ConfigurationManager.OpenMappedExeConfiguration(GetConfigMap(fileName), ConfigurationUserLevel.None);

            var section = string.IsNullOrEmpty(sectionName)
                ? cfg.Sections.OfType<IgniteConfigurationSection>().FirstOrDefault()
                : (IgniteConfigurationSection) cfg.GetSection(sectionName);

            if (section == null)
                throw new ConfigurationErrorsException(
                    string.Format("Could not find {0} in current application configuration",
                        typeof(IgniteConfigurationSection).Name));

            return section.IgniteConfiguration;
        }

        /// <summary>
        /// Gets the configuration file map.
        /// </summary>
        private static ExeConfigurationFileMap GetConfigMap(string fileName)
        {
            var fullFileName = Path.GetFullPath(fileName);

            if (!File.Exists(fullFileName))
                throw new ConfigurationErrorsException("Specified config file does not exist: " + fileName);

            return new ExeConfigurationFileMap {ExeConfigFilename = fullFileName};
        }

        /// <summary>
        /// Finds the config value.
        /// </summary>
        private static string FindValue(IEnumerable<Tuple<string, string>> args, string name)
        {
            return args.Where(x => name.Equals(x.Item1, StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Item2)
                    .FirstOrDefault();
        }
    }
}

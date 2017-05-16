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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Linq;
    using System.IO;
    using System.Reflection;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// IgniteHome resolver.
    /// </summary>
    internal static class IgniteHome
    {
        /** Environment variable: IGNITE_HOME. */
        internal const string EnvIgniteHome = "IGNITE_HOME";

        /// <summary>
        /// Calculate Ignite home.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="log">The log.</param>
        public static string Resolve(IgniteConfiguration cfg, ILogger log = null)
        {
            var home = cfg == null ? null : cfg.IgniteHome;

            if (string.IsNullOrWhiteSpace(home))
            {
                home = Environment.GetEnvironmentVariable(EnvIgniteHome);

                if (log != null)
                    log.Debug("IgniteHome retrieved from {0} environment variable: '{1}'", EnvIgniteHome, home);
            }
            else if (!IsIgniteHome(new DirectoryInfo(home)))
                throw new IgniteException(string.Format("IgniteConfiguration.IgniteHome is not valid: '{0}'", home));

            if (string.IsNullOrWhiteSpace(home))
                home = Resolve(log);
            else if (!IsIgniteHome(new DirectoryInfo(home)))
                throw new IgniteException(string.Format("{0} is not valid: '{1}'", EnvIgniteHome, home));

            if (log != null)
                log.Debug("IgniteHome resolved to '{0}'", home);

            return home;
        }

        /// <summary>
        /// Automatically resolve Ignite home directory.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <returns>
        /// Ignite home directory.
        /// </returns>
        private static string Resolve(ILogger log)
        {
            var probeDirs = new[]
            {
                Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                Directory.GetCurrentDirectory()
            };

            if (log != null)
                log.Debug("Attempting to resolve IgniteHome in the assembly directory " +
                          "'{0}' and current directory '{1}'...", probeDirs[0], probeDirs[1]);


            foreach (var probeDir in probeDirs.Where(x => !string.IsNullOrEmpty(x)))
            {
                if (log != null)
                    log.Debug("Probing IgniteHome in '{0}'...", probeDir);

                var dir = new DirectoryInfo(probeDir);

                while (dir != null)
                {
                    if (IsIgniteHome(dir))
                        return dir.FullName;

                    dir = dir.Parent;
                }
            }

            return null;
        }

        /// <summary>
        /// Determines whether specified dir looks like a Ignite home.
        /// </summary>
        /// <param name="dir">Directory.</param>
        /// <returns>Value indicating whether specified dir looks like a Ignite home.</returns>
        private static bool IsIgniteHome(DirectoryInfo dir)
        {
            try
            {
                return dir.Exists &&
                       (dir.EnumerateDirectories().Count(x => x.Name == "examples" || x.Name == "bin") == 2 &&
                        dir.EnumerateDirectories().Count(x => x.Name == "modules" || x.Name == "platforms") == 1)
                       || // NuGet home
                       (dir.EnumerateDirectories().Any(x => x.Name == "Libs") &&
                        (dir.EnumerateFiles("Apache.Ignite.Core.dll").Any() ||
                         dir.EnumerateFiles("Apache.Ignite.*.nupkg").Any()));
            }
            catch (IOException)
            {
                return false;
            }
        }
    }
}

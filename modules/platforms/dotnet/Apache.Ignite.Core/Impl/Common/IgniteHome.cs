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
    using System.Collections.Generic;
    using System.Linq;
    using System.IO;
    using System.Reflection;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// IgniteHome resolver.
    /// </summary>
    public static class IgniteHome
    {
        /** Environment variable: IGNITE_HOME. */
        internal const string EnvIgniteHome = "IGNITE_HOME";

        /// <summary>
        /// Calculate Ignite home.
        /// </summary>
        /// <param name="igniteHome">Optional known home.</param>
        /// <param name="log">The log.</param>
        public static string Resolve(string igniteHome = null, ILogger log = null)
        {
            var home = igniteHome;

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
            {
                log.Debug("IGNITE_HOME resolved to: {0}", home);
            }

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
            foreach (var probeDir in GetProbeDirectories().Where(x => !string.IsNullOrEmpty(x)))
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
        /// Gets directories to probe for Ignite Home.
        /// </summary>
        private static IEnumerable<string> GetProbeDirectories()
        {
            var entryAsm = Assembly.GetEntryAssembly();
            if (entryAsm != null)
            {
                yield return Path.GetDirectoryName(entryAsm.Location);
            }

            var executingAsmPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            if (!string.IsNullOrWhiteSpace(executingAsmPath))
            {
                yield return executingAsmPath;

                // NuGet home - for LINQPad.
                yield return Path.Combine(executingAsmPath, "..", "..", "build", "output");
            }

            yield return Directory.GetCurrentDirectory();
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
                if (!dir.Exists)
                {
                    return false;
                }

                // Binary release or NuGet home:
                var libs = Path.Combine(dir.FullName, "libs");

                if (Directory.Exists(libs) &&
                    Directory.EnumerateFiles(libs, "ignite-core-*.jar", SearchOption.TopDirectoryOnly).Any())
                {
                    return true;
                }

                // Source release home:
                var javaSrc = Path.Combine(dir.FullName,
                    "modules", "core", "src", "main", "java", "org", "apache", "ignite");

                return Directory.Exists(javaSrc);
            }
            catch (IOException)
            {
                return false;
            }
        }
    }
}

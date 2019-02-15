/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    public static class IgniteHome
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

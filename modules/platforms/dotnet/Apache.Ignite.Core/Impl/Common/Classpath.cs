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
    using System.Diagnostics.CodeAnalysis;
    using System.Text;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Classpath resolver.
    /// </summary>
    internal static class Classpath
    {
        /** Environment variable: whether to set test classpath or not. */
        internal const string EnvIgniteNativeTestClasspath = "IGNITE_NATIVE_TEST_CLASSPATH";

        /** Classpath prefix. */
        private const string ClasspathPrefix = "-Djava.class.path=";

        /** Classpath separator. */
        [SuppressMessage("Microsoft.Performance", "CA1802:UseLiteralsWhereAppropriate")]
        private static readonly string ClasspathSeparator = Os.IsWindows ? ";" : ":";
        
        /** Excluded modules from test classpath */
        private static readonly string[] TestExcludedModules = { "rest-http", "spring-data" };

        /// <summary>
        /// Creates classpath from the given configuration, or default classpath if given config is null.
        /// </summary>
        /// <param name="classPath">Known or additional classpath, can be null.</param>
        /// <param name="igniteHome">Ignite home, can be null.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        /// <see cref="EnvIgniteNativeTestClasspath" /> is not set.</param>
        /// <param name="log">The log.</param>
        /// <returns>
        /// Classpath string.
        /// </returns>
        internal static string CreateClasspath(string classPath, string igniteHome, bool forceTestClasspath = false,
            ILogger log = null)
        {
            var cpStr = new StringBuilder();

            if (!string.IsNullOrWhiteSpace(classPath))
            {
                cpStr.Append(classPath);

                if (!classPath.EndsWith(ClasspathSeparator))
                    cpStr.Append(ClasspathSeparator);
            }

            if (!string.IsNullOrWhiteSpace(igniteHome))
                AppendHomeClasspath(igniteHome, forceTestClasspath, cpStr);

            if (log != null)
            {
                log.Debug("Classpath resolved to: {0}", cpStr);
            }

            var res = cpStr.ToString();
            res = res.StartsWith(ClasspathPrefix) ? res : ClasspathPrefix + res;

            return res;
        }

        /// <summary>
        /// Appends classpath from home directory, if it is defined.
        /// </summary>
        /// <param name="ggHome">The home dir.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        ///     <see cref="EnvIgniteNativeTestClasspath"/> is not set.</param>
        /// <param name="cpStr">The classpath string.</param>
        private static void AppendHomeClasspath(string ggHome, bool forceTestClasspath, StringBuilder cpStr)
        {
            // Append test directories (if needed) first, because otherwise build *.jar will be picked first.
            if (forceTestClasspath || bool.TrueString.Equals(
                    Environment.GetEnvironmentVariable(EnvIgniteNativeTestClasspath),
                    StringComparison.OrdinalIgnoreCase))
            {
                AppendTestClasses(Path.Combine(ggHome, "examples"), cpStr);
                AppendTestClasses(Path.Combine(ggHome, "modules"), cpStr);
                AppendTestClasses(Path.Combine(ggHome, "modules", "extdata", "platform"), cpStr);
            }

            string ggLibs = Path.Combine(ggHome, "libs");

            AppendJars(ggLibs, cpStr);

            if (Directory.Exists(ggLibs))
            {
                foreach (string dir in Directory.EnumerateDirectories(ggLibs))
                {
                    if (!dir.EndsWith("optional"))
                        AppendJars(dir, cpStr);
                }
            }
        }

        /// <summary>
        /// Append target (compile) directories to classpath (for testing purposes only).
        /// </summary>
        /// <param name="path">Path</param>
        /// <param name="cp">Classpath builder.</param>
        private static void AppendTestClasses(string path, StringBuilder cp)
        {
            if (Directory.Exists(path))
            {
                AppendTestClasses0(path, cp);

                foreach (string moduleDir in Directory.EnumerateDirectories(path))
                    AppendTestClasses0(moduleDir, cp);
            }
        }

        /// <summary>
        /// Internal routine to append classes and jars from eploded directory.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="cp">Classpath builder.</param>
        private static void AppendTestClasses0(string path, StringBuilder cp)
        {
            var shouldExcluded = TestExcludedModules.Any(excl => 
                path.IndexOf(excl, StringComparison.OrdinalIgnoreCase) >= 0);
            
            if (shouldExcluded)
                return;

            var dir = Path.Combine(path, "target", "classes");
            if (Directory.Exists(dir))
            {
                cp.Append(dir).Append(ClasspathSeparator);
            }

            dir = Path.Combine(path, "target", "test-classes");
            if (Directory.Exists(dir))
            {
                cp.Append(dir).Append(ClasspathSeparator);
            }

            dir = Path.Combine(path, "target", "libs");
            if (Directory.Exists(dir))
            {
                AppendJars(dir, cp);
            }
        }

        /// <summary>
        /// Append jars from the given path.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="cpStr">Classpath string builder.</param>
        private static void AppendJars(string path, StringBuilder cpStr)
        {
            if (Directory.Exists(path))
            {
                foreach (var jar in Directory.EnumerateFiles(path, "*.jar"))
                {
                    cpStr.Append(jar).Append(ClasspathSeparator);
                }
            }
        }
    }
}

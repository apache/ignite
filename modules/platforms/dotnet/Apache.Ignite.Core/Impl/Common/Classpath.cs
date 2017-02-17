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
    using System.Text;
    using System.IO;
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

        /// <summary>
        /// Creates classpath from the given configuration, or default classpath if given config is null.
        /// </summary>
        /// <param name="cfg">The configuration.</param>
        /// <param name="forceTestClasspath">Append test directories even if
        /// <see cref="EnvIgniteNativeTestClasspath" /> is not set.</param>
        /// <param name="log">The log.</param>
        /// <returns>
        /// Classpath string.
        /// </returns>
        internal static string CreateClasspath(IgniteConfiguration cfg = null, bool forceTestClasspath = false, 
            ILogger log = null)
        {
            var cpStr = new StringBuilder();

            if (cfg != null && cfg.JvmClasspath != null)
            {
                cpStr.Append(cfg.JvmClasspath);

                if (!cfg.JvmClasspath.EndsWith(";"))
                    cpStr.Append(';');
            }

            var ggHome = IgniteHome.Resolve(cfg, log);

            if (!string.IsNullOrWhiteSpace(ggHome))
                AppendHomeClasspath(ggHome, forceTestClasspath, cpStr);

            if (log != null)
                log.Debug("Classpath resolved to: " + cpStr);

            return ClasspathPrefix + cpStr;
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
            if (forceTestClasspath || "true".Equals(Environment.GetEnvironmentVariable(EnvIgniteNativeTestClasspath)))
            {
                AppendTestClasses(ggHome + "\\examples", cpStr);
                AppendTestClasses(ggHome + "\\modules", cpStr);
            }

            string ggLibs = ggHome + "\\libs";

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
            if (path.EndsWith("rest-http", StringComparison.OrdinalIgnoreCase))
                return;

            if (Directory.Exists(path + "\\target\\classes"))
                cp.Append(path + "\\target\\classes;");

            if (Directory.Exists(path + "\\target\\test-classes"))
                cp.Append(path + "\\target\\test-classes;");

            if (Directory.Exists(path + "\\target\\libs"))
                AppendJars(path + "\\target\\libs", cp);
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
                foreach (string jar in Directory.EnumerateFiles(path, "*.jar"))
                {
                    cpStr.Append(jar);
                    cpStr.Append(';');
                }
            }
        }
    }
}

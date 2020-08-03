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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Tests.Process;

    /// <summary>
    /// Starts Java server nodes.
    /// Uses Maven project in JavaServer folder to download and run arbitrary release
    /// (Maven is the fastest way to download a release (we only need the core jar, bare minimum).
    /// Server node sets thin client port to 10890.
    /// </summary>
    public static class JavaServer
    {
        /** Client port. */
        public const int ClientPort = 10890;

        /** Apache Ignite artifact group ID. */
        public const string GroupIdIgnite = "org.apache.ignite";
        
        /** Maven command to execute the main class. */
        private const string MavenCommandExec = "compile exec:java -D\"exec.mainClass\"=\"Runner\"";

        /** Java server sources path. */
        private static readonly string JavaServerSourcePath = Path.Combine(
            TestUtils.GetDotNetSourceDir().FullName,
            "Apache.Ignite.Core.Tests",
            "JavaServer");

        /** Full path to Maven binary. */
        private static readonly string MavenPath = GetMaven();

        /// <summary>
        /// Starts a server node with a given version.
        /// </summary>
        /// <param name="groupId">Maven artifact group id.</param>
        /// <param name="version">Product version.</param>
        /// <returns>Disposable object to stop the server.</returns>
        public static IDisposable Start(string groupId, string version)
        {
            IgniteArgumentCheck.NotNullOrEmpty(version, "version");

            var pomWrapper =
                ReplaceIgniteVersionInPomFile(groupId, version, Path.Combine(JavaServerSourcePath, "pom.xml"));
            
            var process = new System.Diagnostics.Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = Os.IsWindows ? "cmd.exe" : "/bin/bash",
                    Arguments = Os.IsWindows
                        ? string.Format("/c \"{0} {1}\"", MavenPath, MavenCommandExec)
                        : string.Format("-c \"{0} {1}\"", MavenPath, MavenCommandExec.Replace("\"", "\\\"")),
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    WorkingDirectory = JavaServerSourcePath,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                }
            };

            process.Start();
            
            var processWrapper = new DisposeAction(() =>
            {
                process.KillProcessTree();
                pomWrapper.Dispose();
            });

            try
            {
                var listDataReader = new ListDataReader();
                process.AttachProcessConsoleReader(listDataReader, new IgniteProcessConsoleOutputReader());

                // Wait for node to come up with a thin client connection.
                if (WaitForStart(listDataReader))
                {
                    return processWrapper;
                }

                throw new Exception("Failed to start Java node: " + string.Join(",", listDataReader.GetOutput()));
            }
            catch (Exception)
            {
                processWrapper.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Updates pom.xml with given Ignite version.
        /// </summary>
        private static IDisposable ReplaceIgniteVersionInPomFile(string groupId, string version, string pomFile)
        {
            var pomContent = File.ReadAllText(pomFile);
            var originalPomContent = pomContent;
            
            pomContent = Regex.Replace(pomContent,
                @"<version>\d+\.\d+\.\d+</version>",
                string.Format("<version>{0}</version>", version));
            
            pomContent = Regex.Replace(pomContent,
                @"<groupId>org.*?</groupId>",
                string.Format("<groupId>{0}</groupId>", groupId));
            
            File.WriteAllText(pomFile, pomContent);
            
            return new DisposeAction(() => File.WriteAllText(pomFile, originalPomContent));
        }

        /// <summary>
        /// Gets client configuration to connect to the Java server.
        /// </summary>
        public static IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration("127.0.0.1:" + ClientPort);
        }

        /// <summary>
        /// Waits for server node to fully start.
        /// </summary>
        private static bool WaitForStart(ListDataReader reader)
        {
            return TestUtils.WaitForCondition(
                () => reader.GetOutput().Any(m => m.Contains("Ignite node started OK")), 
                60 * 3 * 1000);
        }

        /// <summary>
        /// Gets maven path. 
        /// </summary>
        private static string GetMaven()
        {
            var extensions = Os.IsWindows ? new[] {".cmd", ".bat"} : new[] {string.Empty};
            
            return new[] {"MAVEN_HOME", "M2_HOME", "M3_HOME", "MVN_HOME"}
                .Select(Environment.GetEnvironmentVariable)
                .Where(x => !string.IsNullOrEmpty(x))
                .Select(x => Path.Combine(x, "bin", "mvn"))
                .SelectMany(x => extensions.Select(ext => x + ext))
                .Where(File.Exists)
                .FirstOrDefault() ?? "mvn";
        }
    }
}
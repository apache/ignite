/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
        /// <param name="version">Product version.</param>
        /// <returns>Disposable object to stop the server.</returns>
        public static IDisposable Start(string version)
        {
            IgniteArgumentCheck.NotNullOrEmpty(version, "version");

            ReplaceIgniteVersionInPomFile(version, Path.Combine(JavaServerSourcePath, "pom.xml"));
            
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
            
            var listDataReader = new ListDataReader();
            process.AttachProcessConsoleReader(listDataReader, new IgniteProcessConsoleOutputReader());
            
            var processWrapper = new DisposeAction(() => process.KillProcessTree());

            // Wait for node to come up with a thin client connection.
            if (WaitForStart())
            {
                return processWrapper;
            }

            if (!process.HasExited)
            {
                processWrapper.Dispose();
            }
            
            throw new Exception("Failed to start Java node: " + string.Join(",", listDataReader.GetOutput()));
        }

        /// <summary>
        /// Updates pom.xml with given Ignite version.
        /// </summary>
        private static void ReplaceIgniteVersionInPomFile(string version, string pomFile)
        {
            var pomContent = File.ReadAllText(pomFile);
            pomContent = Regex.Replace(pomContent,
                @"<version>\d+\.\d+\.\d+</version>",
                string.Format("<version>{0}</version>", version));
            File.WriteAllText(pomFile, pomContent);
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
        private static bool WaitForStart()
        {
            return TestUtils.WaitForCondition(() =>
            {
                try
                {
                    // Port 10890 is set in Runner.java
                    using (var client = Ignition.StartClient(GetClientConfiguration()))
                    {
                        // Create cache to ensure valid grid state.
                        client.GetOrCreateCache<int, int>(typeof(JavaServer).FullName);
                        return true;
                    }
                }
                catch (Exception)
                {
                    return false;
                }
            }, 60000);
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
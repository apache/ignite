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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Failure;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Test utility methods - Windows-specific part (full framework).
    /// </summary>
    public static partial class TestUtils
    {
        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public static string CreateTestClasspath()
        {
            var home = IgniteHome.Resolve();
            return Classpath.CreateClasspath(null, home, forceTestClasspath: true);
        }

        /// <summary>
        /// Kill Ignite processes.
        /// </summary>
        public static void KillProcesses()
        {
            IgniteProcess.KillAll();
        }

        /// <summary>
        /// Gets the default code-based test configuration.
        /// </summary>
        public static IgniteConfiguration GetTestConfiguration(bool? jvmDebug = null, string name = null)
        {
            return new IgniteConfiguration
            {
                DiscoverySpi = GetStaticDiscovery(),
                Localhost = "127.0.0.1",
                JvmOptions = TestJavaOptions(jvmDebug),
                JvmClasspath = CreateTestClasspath(),
                IgniteInstanceName = name,
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = DataStorageConfiguration.DefaultDataRegionName,
                        InitialSize = 128 * 1024 * 1024,
                        MaxSize = Environment.Is64BitProcess
                            ? DataRegionConfiguration.DefaultMaxSize
                            : 256 * 1024 * 1024
                    }
                },
                FailureHandler = new NoOpFailureHandler(),
                WorkDirectory = WorkDir
            };
        }

        /// <summary>
        /// Runs the test in new process.
        /// </summary>
        [SuppressMessage("ReSharper", "AssignNullToNotNullAttribute")]
        public static void RunTestInNewProcess(string fixtureName, string testName)
        {
            var procStart = new ProcessStartInfo
            {
                FileName = typeof(TestUtils).Assembly.Location,
                Arguments = fixtureName + " " + testName,
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            var proc = System.Diagnostics.Process.Start(procStart);
            Assert.IsNotNull(proc);

            try
            {
                proc.AttachProcessConsoleReader();

                Assert.IsTrue(proc.WaitForExit(30000));
                Assert.AreEqual(0, proc.ExitCode);
            }
            finally
            {
                if (!proc.HasExited)
                {
                    proc.Kill();
                }
            }
        }
    }
}

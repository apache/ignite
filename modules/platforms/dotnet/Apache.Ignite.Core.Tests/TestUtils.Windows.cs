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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Failure;
    using Apache.Ignite.Core.Impl;
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
            return Classpath.CreateClasspath(forceTestClasspath: true);
        }

        /// <summary>
        /// Gets the name of the temporary directory.
        /// </summary>
        public static string GetTempDirectoryName()
        {
            return IgniteUtils.GetTempDirectoryName();
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
                IgniteProcess.AttachProcessConsoleReader(proc);

                Assert.IsTrue(proc.WaitForExit(19000));
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

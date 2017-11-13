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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests custom JAR deployment, classpath and IgniteHome logic.
    /// </summary>
    public class DeploymentTest
    {
        /** */
        private string _tempFolder;

        /// <summary>
        /// Tests the custom deployment where IGNITE_HOME can't be resolved, and there is a user-defined classpath.
        /// </summary>
        [Test]
        public void TestCustomDeployment()
        {
            // Create temp folder
            var folder = _tempFolder;
            DeployTo(folder);

            // Build classpath
            var classpath = string.Join(";", Directory.GetFiles(folder).Select(Path.GetFileName));

            // Copy config
            var springPath = Path.GetFullPath("config\\compute\\compute-grid2.xml");
            var springFile = Path.GetFileName(springPath);
            File.Copy(springPath, Path.Combine(folder, springFile));

            // Start a node and make sure it works properly
            var exePath = Path.Combine(folder, "Apache.Ignite.exe");

            var proc = IgniteProcess.Start(exePath, string.Empty, args: new[]
            {
                "-springConfigUrl=" + springFile,
                "-jvmClasspath=" + classpath,
                "-assembly=" + Path.GetFileName(GetType().Assembly.Location),
                "-J-ea",
                "-J-Xms512m",
                "-J-Xmx512m"
            });

            Assert.IsNotNull(proc);

            VerifyNodeStarted(exePath);
        }

        /// <summary>
        /// Tests missing JARs.
        /// </summary>
        [Test]
        public void TestMissingJarsCauseProperException()
        {
            // Create temp folder
            var folder = _tempFolder;
            DeployTo(folder);

            // Build classpath
            var classpath = string.Join(";",
                Directory.GetFiles(folder).Where(x => !x.Contains("ignite-core-")).Select(Path.GetFileName));

            // Start a node and check the exception.
            var exePath = Path.Combine(folder, "Apache.Ignite.exe");
            var reader = new ListDataReader();

            var proc = IgniteProcess.Start(exePath, string.Empty, args: new[]
            {
                "-jvmClasspath=" + classpath,
                "-J-ea",
                "-J-Xms512m",
                "-J-Xmx512m"
            }, outReader: reader);

            // Wait for process to fail.
            Assert.IsNotNull(proc);
            Assert.IsTrue(TestUtils.WaitForCondition(() => proc.HasExited, 1000));
            Assert.AreEqual(-1, proc.ExitCode);

            // Check error message.
            Assert.AreEqual("ERROR: Apache.Ignite.Core.Common.IgniteException: Java class is not found " +
                            "(did you set IGNITE_HOME environment variable?): " +
                            "org/apache/ignite/internal/processors/platform/PlatformIgnition",
                reader.GetOutput().First());
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _tempFolder = IgniteUtils.GetTempDirectoryName();
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
            IgniteProcess.KillAll();

            Directory.Delete(_tempFolder, true);
        }

        /// <summary>
        /// Deploys binaries to specified folder
        /// </summary>
        private void DeployTo(string folder)
        {
            // Copy jars.
            var home = IgniteHome.Resolve(null);

            var jarNames = new[] {@"\ignite-core-", @"\cache-api-1.0.0.jar", @"\modules\spring\"};

            var jars = Directory.GetFiles(home, "*.jar", SearchOption.AllDirectories)
                .Where(jarPath => jarNames.Any(jarPath.Contains)).ToArray();

            Assert.Greater(jars.Length, 3);

            foreach (var jar in jars)
            {
                var fileName = Path.GetFileName(jar);
                Assert.IsNotNull(fileName);
                File.Copy(jar, Path.Combine(folder, fileName), true);
            }

            // Copy .NET binaries
            foreach (var asm in new[] {typeof(IgniteRunner).Assembly, typeof(Ignition).Assembly, GetType().Assembly})
            {
                Assert.IsNotNull(asm.Location);
                File.Copy(asm.Location, Path.Combine(folder, Path.GetFileName(asm.Location)));
            }
        }

        /// <summary>
        /// Verifies that custom-deployed node has started.
        /// </summary>
        private static void VerifyNodeStarted(string exePath)
        {
            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = "config\\compute\\compute-grid1.xml",
            }))
            {
                Assert.IsTrue(ignite.WaitTopology(2));

                var remoteProcPath = ignite.GetCluster().ForRemotes().GetCompute().Call(new ProcessPathFunc());

                Assert.AreEqual(exePath, remoteProcPath);
            }
        }

        #pragma warning disable 649
        /// <summary>
        /// Function that returns process path.
        /// </summary>
        [Serializable]
        private class ProcessPathFunc : IComputeFunc<string>
        {
            [InstanceResource]
            private IIgnite _ignite;

            /** <inheritdoc /> */
            public string Invoke()
            {
                // Should be null
                var igniteHome = _ignite.GetConfiguration().IgniteHome;

                return typeof(IgniteRunner).Assembly.Location + igniteHome;
            }
        }
    }
}

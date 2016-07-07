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

#pragma warning disable 649
#pragma warning disable 169
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests custom JAR deployment, classpath and IgniteHome logic.
    /// </summary>
    public class DeploymentTest
    {
        /// <summary>
        /// Tests the custom deployment where IGNITE_HOME can't be resolved, and there is a user-defined classpath.
        /// </summary>
        [Test]
        public void TestCustomDeployment()
        {
            // Create temp folder
            var folder = GetTempFolder();

            // Copy jars
            var home = IgniteHome.Resolve(null);

            var jarNames = new[] {@"\ignite-core-", @"\geronimo-jcache_1.0_spec-1.0-alpha-1.jar", @"\modules\spring\" };

            var jars = Directory.GetFiles(home, "*.jar", SearchOption.AllDirectories)
                .Where(jarPath => jarNames.Any(jarPath.Contains)).ToArray();

            Assert.Greater(jars.Length, 3);

            foreach (var jar in jars)
                // ReSharper disable once AssignNullToNotNullAttribute
                File.Copy(jar, Path.Combine(folder, Path.GetFileName(jar)), true);

            // Build classpath
            var classpath = string.Join(";", Directory.GetFiles(folder).Select(Path.GetFileName));

            // Copy .NET binaries
            foreach (var asm in new[] {typeof (IgniteRunner).Assembly, typeof (Ignition).Assembly, GetType().Assembly})
                File.Copy(asm.Location, Path.Combine(folder, Path.GetFileName(asm.Location)));

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
                "-J-ea",
                "-J-Xcheck:jni",
                "-J-Xms512m",
                "-J-Xmx512m"
            });

            Assert.IsNotNull(proc);

            try
            {
                VerifyNodeStarted(exePath);
            }
            finally
            {
                proc.Kill();

                Assert.IsTrue(
                    TestUtils.WaitForCondition(() =>
                    {
                        try
                        {
                            Directory.Delete(folder, true);
                            return true;
                        }
                        catch (Exception)
                        {
                            return false;
                        }
                    }, 1000), "Failed to remove temp directory: " + folder);
            }
        }

        /// <summary>
        /// Verifies that custom-deployed node has started.
        /// </summary>
        private static void VerifyNodeStarted(string exePath)
        {
            using (var ignite = Ignition.Start(new IgniteConfiguration
            {
                SpringConfigUrl = "config\\compute\\compute-grid1.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            }))
            {
                Assert.IsTrue(ignite.WaitTopology(2));

                var remoteProcPath = ignite.GetCluster().ForRemotes().GetCompute().Call(new ProcessPathFunc());

                Assert.AreEqual(exePath, remoteProcPath);
            }
        }

        /// <summary>
        /// Gets the temporary folder.
        /// </summary>
        private static string GetTempFolder()
        {
            const string prefix = "ig-test-";
            var temp = Path.GetTempPath();

            for (int i = 0; i < int.MaxValue; i++)
            {
                {
                    try
                    {
                        var path = Path.Combine(temp, prefix + i);

                        if (Directory.Exists(path))
                            Directory.Delete(path, true);

                        return Directory.CreateDirectory(path).FullName;
                    }
                    catch (Exception)
                    {
                        // Ignore
                    }
                }
            }

            throw new InvalidOperationException();
        }

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

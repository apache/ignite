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

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Cache test.
    /// </summary>
    public class StartupTest
    {
        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);

            foreach (var proc in Process.GetProcesses())
            {
                if (proc.ProcessName.Equals("Apache.Ignite"))
                {
                    proc.Kill();
                    proc.WaitForExit();
                }
            }
        }

        /// <summary>
        /// Tests code configuration.
        /// </summary>
        [Test]
        public void TestCodeConfig()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                CacheConfiguration = new[] {new CacheConfiguration("testcache")}
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetCache<int, int>("testcache");

                cache[1] = 5;

                Assert.AreEqual(5, cache[1]);
            }
        }

        /// <summary>
        /// Tests code configuration.
        /// </summary>
        [Test]
        public void TestSpringConfig()
        {
            using (var ignite = Ignition.Start("config\\ignite-config.xml"))
            {
                var cache = ignite.GetCache<int, int>("testcache");

                cache[1] = 5;

                Assert.AreEqual(5, cache[1]);
            }
        }

        /// <summary>
        /// Tests the executable that is included in NuGet.
        /// </summary>
        [Test]
        public void TestApacheIgniteExe()
        {
            var asm = GetType().Assembly;
            var version = asm.GetName().Version.ToString(3);
            var packageDirName = "Apache.Ignite." + version + "*";
            
            var asmDir = Path.GetDirectoryName(asm.Location);
            Assert.IsNotNull(asmDir, asmDir);

            var packagesDir = Path.GetFullPath(Path.Combine(asmDir, @"..\..\packages"));
            Assert.IsTrue(Directory.Exists(packagesDir), packagesDir);

            var packageDir = Directory.GetDirectories(packagesDir, packageDirName).Single();
            Assert.IsTrue(Directory.Exists(packageDir), packageDir);

            var exePath = Path.Combine(packageDir, @"lib\net40\Apache.Ignite.exe");
            Assert.IsTrue(File.Exists(exePath), exePath);

            var springPath = Path.GetFullPath(@"config\ignite-config.xml");
            Assert.IsTrue(File.Exists(springPath), springPath);

            var procInfo = new ProcessStartInfo(exePath, "-springConfigUrl=" + springPath)
            {
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            
            var proc = Process.Start(procInfo);
            Assert.IsNotNull(proc);
            Assert.IsFalse(proc.HasExited);

            TestUtil.AttachProcessConsoleReader(proc);

            using (var ignite = Ignition.Start(@"config\ignite-config.xml"))
            {
                for (var i = 0; i < 100; i++)
                {
                    if (ignite.GetCluster().GetNodes().Count == 2)
                    {
                        return;
                    }

                    Thread.Sleep(100);
                }
                
                Assert.Fail("Failed to join to remote node.");
            }
        }
    }
}

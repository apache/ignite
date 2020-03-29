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
        /** */
        private string _tempDir;

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

            if (!string.IsNullOrEmpty(_tempDir))
            {
                Directory.Delete(_tempDir, true);
                _tempDir = null;
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

            _tempDir = PathUtils.GetTempDirectoryName();
            PathUtils.CopyDirectory(packageDir, _tempDir);

            var exePath = Path.Combine(_tempDir, @"lib\net40\Apache.Ignite.exe");
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

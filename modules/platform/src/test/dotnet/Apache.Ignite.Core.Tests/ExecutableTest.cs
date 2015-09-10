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

// ReSharper disable UnusedVariable
// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Tests.Process;
    using Microsoft.CSharp;
    using NUnit.Framework;

    /// <summary>
    /// Tests for executable.
    /// </summary>
    public class ExecutableTest
    {
        /** Spring configuration path. */
        private static readonly string SpringCfgPath = "config\\compute\\compute-standalone.xml";

        /** Min memory Java task. */
        private const string MinMemTask = "org.apache.ignite.platform.PlatformMinMemoryTask";

        /** Max memory Java task. */
        private const string MaxMemTask = "org.apache.ignite.platform.PlatformMaxMemoryTask";

        /** Grid. */
        private IIgnite _grid;

        /// <summary>
        /// Test fixture set-up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            TestUtils.KillProcesses();

            _grid = Ignition.Start(Configuration(SpringCfgPath));
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);

            TestUtils.KillProcesses();
        }

        /// <summary>
        /// Set-up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();

            Assert.IsTrue(_grid.WaitTopology(1, 30000));

            IgniteProcess.SaveConfigurationBackup();
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            IgniteProcess.RestoreConfigurationBackup();
        }

        /// <summary>
        /// Test data pass through configuration file.
        /// </summary>
        [Test]
        public void TestConfig()
        {
            IgniteProcess.ReplaceConfiguration("config\\Apache.Ignite.exe.config.test");

            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath()
                );

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            var cfg = RemoteConfig();

            Assert.AreEqual(SpringCfgPath, cfg.SpringConfigUrl);
            Assert.IsTrue(cfg.JvmOptions.Contains("-DOPT1") && cfg.JvmOptions.Contains("-DOPT2"));
            Assert.IsTrue(cfg.Assemblies.Contains("test-1.dll") && cfg.Assemblies.Contains("test-2.dll"));
            Assert.AreEqual(601, cfg.JvmInitialMemoryMb);
            Assert.AreEqual(702, cfg.JvmMaxMemoryMb);
        }

        /// <summary>
        /// Test assemblies passing through command-line. 
        /// </summary>
        [Test]
        public void TestAssemblyCmd()
        {
            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SpringCfgPath,
                "-assembly=test-1.dll",
                "-assembly=test-2.dll"
                );

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            var cfg = RemoteConfig();

            Assert.IsTrue(cfg.Assemblies.Contains("test-1.dll") && cfg.Assemblies.Contains("test-2.dll"));
        }

        /// <summary>
        /// Test JVM options passing through command-line. 
        /// </summary>
        [Test]
        public void TestJvmOptsCmd()
        {
            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SpringCfgPath,
                "-J-DOPT1",
                "-J-DOPT2"
                );

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            var cfg = RemoteConfig();

            Assert.IsTrue(cfg.JvmOptions.Contains("-DOPT1") && cfg.JvmOptions.Contains("-DOPT2"));
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: raw java options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdRaw()
        {
            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SpringCfgPath,
                "-J-Xms506m",
                "-J-Xmx607m"
                );

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            var minMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 506*1024*1024, minMem);

            var maxMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 607*1024*1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: custom options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdCustom()
        {
            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SpringCfgPath,
                "-JvmInitialMemoryMB=615",
                "-JvmMaxMemoryMB=863"
                );

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            var minMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 615*1024*1024, minMem);

            var maxMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 863*1024*1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing from application configuration.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsAppConfig()
        {
            IgniteProcess.ReplaceConfiguration("config\\Apache.Ignite.exe.config.test");

            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            var proc = new IgniteProcess("-jvmClasspath=" + TestUtils.CreateTestClasspath());

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            var minMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 601*1024*1024, minMem);

            var maxMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 702*1024*1024, maxMem);

            proc.Kill();

            Assert.IsTrue(_grid.WaitTopology(1, 30000));

            // Command line options overwrite config file options
            // ReSharper disable once RedundantAssignment
            proc = new IgniteProcess("-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-J-Xms605m", "-J-Xmx706m");

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            minMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 605*1024*1024, minMem);

            maxMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 706*1024*1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: custom options + raw options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdCombined()
        {
            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SpringCfgPath,
                "-J-Xms555m",
                "-J-Xmx666m",
                "-JvmInitialMemoryMB=128",
                "-JvmMaxMemoryMB=256"
                );

            Assert.IsTrue(_grid.WaitTopology(2, 30000));

            // Raw JVM options (Xms/Xmx) should override custom options
            var minMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 555*1024*1024, minMem);

            var maxMem = _grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 666*1024*1024, maxMem);
        }

        /// <summary>
        /// Get remote node configuration.
        /// </summary>
        /// <returns>Configuration.</returns>
        private RemoteConfiguration RemoteConfig()
        {
            return _grid.Cluster.ForRemotes().Compute().Call(new RemoteConfigurationClosure());
        }

        /// <summary>
        /// Configuration for node.
        /// </summary>
        /// <param name="path">Path to Java XML configuration.</param>
        /// <returns>Node configuration.</returns>
        private static IgniteConfiguration Configuration(string path)
        {
            var cfg = new IgniteConfiguration();


            var portCfg = new PortableConfiguration();

            ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

            portTypeCfgs.Add(new PortableTypeConfiguration(typeof (RemoteConfiguration)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof (RemoteConfigurationClosure)));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.PortableConfiguration = portCfg;

            cfg.JvmClasspath = TestUtils.CreateTestClasspath();

            cfg.JvmOptions = new List<string>
            {
                "-ea",
                "-Xcheck:jni",
                "-Xms4g",
                "-Xmx4g",
                "-DIGNITE_QUIET=false",
                "-Xnoagent",
                "-Djava.compiler=NONE",
                "-Xdebug",
                "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005",
                "-XX:+HeapDumpOnOutOfMemoryError"
            };

            cfg.SpringConfigUrl = path;

            return cfg;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="outputPath"></param>
        private static void GenerateDll(string outputPath)
        {
            var codeProvider = new CSharpCodeProvider();

#pragma warning disable 0618

            var icc = codeProvider.CreateCompiler();

#pragma warning restore 0618

            var parameters = new CompilerParameters();
            parameters.GenerateExecutable = false;
            parameters.OutputAssembly = outputPath;

            var src = "namespace Apache.Ignite.Client.Test { public class Foo {}}";

            var results = icc.CompileAssemblyFromSource(parameters, src);

            Assert.False(results.Errors.HasErrors);
        }

        /// <summary>
        /// Asserts that JVM maximum memory corresponds to Xmx parameter value.
        /// </summary>
        private static void AssertJvmMaxMemory(long expected, long actual)
        {
            // allow 20% tolerance because max memory in Java is not exactly equal to Xmx parameter value
            Assert.LessOrEqual(actual, expected);
            Assert.Greater(actual, expected/5*4);
        }

        /// <summary>
        /// Closure which extracts configuration and passes it back.
        /// </summary>
        public class RemoteConfigurationClosure : IComputeFunc<RemoteConfiguration>
        {

#pragma warning disable 0649

            /** Grid. */
            [InstanceResource] private IIgnite _grid;

#pragma warning restore 0649

            /** <inheritDoc /> */

            public RemoteConfiguration Invoke()
            {
                var grid0 = (Ignite) ((IgniteProxy) _grid).Target;

                var cfg = grid0.Configuration;

                var res = new RemoteConfiguration
                {
                    IgniteHome = cfg.IgniteHome,
                    SpringConfigUrl = cfg.SpringConfigUrl,
                    JvmDll = cfg.JvmDllPath,
                    JvmClasspath = cfg.JvmClasspath,
                    JvmOptions = cfg.JvmOptions,
                    Assemblies = cfg.Assemblies,
                    JvmInitialMemoryMb = cfg.JvmInitialMemoryMb,
                    JvmMaxMemoryMb = cfg.JvmMaxMemoryMb
                };

                Console.WriteLine("RETURNING CFG: " + cfg);

                return res;
            }
        }

        /// <summary>
        /// Configuration.
        /// </summary>
        public class RemoteConfiguration
        {
            /// <summary>
            /// GG home.
            /// </summary>
            public string IgniteHome { get; set; }

            /// <summary>
            /// Spring config URL.
            /// </summary>
            public string SpringConfigUrl { get; set; }

            /// <summary>
            /// JVM DLL.
            /// </summary>
            public string JvmDll { get; set; }

            /// <summary>
            /// JVM classpath.
            /// </summary>
            public string JvmClasspath { get; set; }

            /// <summary>
            /// JVM options.
            /// </summary>
            public ICollection<string> JvmOptions { get; set; }

            /// <summary>
            /// Assemblies.
            /// </summary>
            public ICollection<string> Assemblies { get; set; }

            /// <summary>
            /// Minimum JVM memory (Xms).
            /// </summary>
            public int JvmInitialMemoryMb { get; set; }

            /// <summary>
            /// Maximum JVM memory (Xms).
            /// </summary>
            public int JvmMaxMemoryMb { get; set; }

        }
    }
}

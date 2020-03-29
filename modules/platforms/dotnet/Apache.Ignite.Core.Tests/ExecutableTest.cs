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
// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests for Apache.Ignite.exe.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ExecutableTest
    {
        /** Spring configuration path. */
        private const string SpringCfgPath = "config\\compute\\compute-standalone.xml";

        /** Min memory Java task. */
        private const string MinMemTask = "org.apache.ignite.platform.PlatformMinMemoryTask";

        /** Max memory Java task. */
        private const string MaxMemTask = "org.apache.ignite.platform.PlatformMaxMemoryTask";

        /** Grid. */
        private IIgnite _grid;

        /** Temp dir for assemblies. */
        private string _tempDir;

        /// <summary>
        /// Set-up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _tempDir = PathUtils.GetTempDirectoryName();

            TestUtils.KillProcesses();

            _grid = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(RemoteConfiguration)),
                        new BinaryTypeConfiguration(typeof(RemoteConfigurationClosure))
                    }
                },
                SpringConfigUrl = SpringCfgPath
            });

            Assert.IsTrue(_grid.WaitTopology(1));

            IgniteProcess.SaveConfigurationBackup();
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);

            TestUtils.KillProcesses();

            IgniteProcess.RestoreConfigurationBackup();

            Directory.Delete(_tempDir, true);
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

            var proc = new IgniteProcess();

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var cfg = RemoteConfig();

            Assert.AreEqual(SpringCfgPath, cfg.SpringConfigUrl);
            Assert.AreEqual(602, cfg.JvmInitialMemoryMb);
            Assert.AreEqual(702, cfg.JvmMaxMemoryMb);

            CollectionAssert.Contains(cfg.LoadedAssemblies, "test-1");
            CollectionAssert.Contains(cfg.LoadedAssemblies, "test-2");
            Assert.Null(cfg.Assemblies);

            CollectionAssert.Contains(cfg.JvmOptions, "-DOPT1");
            CollectionAssert.Contains(cfg.JvmOptions, "-DOPT2");
        }

        /// <summary>
        /// Test assemblies passing through command-line. 
        /// </summary>
        [Test]
        public void TestAssemblyCmd()
        {
            var dll1 = GenerateDll("test-1.dll", true);
            var dll2 = GenerateDll("test-2.dll", true);

            var proc = new IgniteProcess(
                "-springConfigUrl=" + SpringCfgPath,
                "-assembly=" + dll1,
                "-assembly=" + dll2);

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var cfg = RemoteConfig();

            CollectionAssert.Contains(cfg.LoadedAssemblies, "test-1");
            CollectionAssert.Contains(cfg.LoadedAssemblies, "test-2");
        }

        /// <summary>
        /// Test JVM options passing through command-line. 
        /// </summary>
        [Test]
        public void TestJvmOptsCmd()
        {
            var proc = new IgniteProcess(
                "-springConfigUrl=" + SpringCfgPath,
                "-J-DOPT1",
                "-J-DOPT2"
                );

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

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
                "-springConfigUrl=" + SpringCfgPath,
                "-J-Xms506m",
                "-J-Xmx607m"
                );

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var minMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 506*1024*1024, minMem);

            var maxMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 607*1024*1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: custom options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdCustom()
        {
            var proc = new IgniteProcess(
                "-springConfigUrl=" + SpringCfgPath,
                "-JvmInitialMemoryMB=616",
                "-JvmMaxMemoryMB=866"
                );

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var minMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 616*1024*1024, minMem);

            var maxMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 866*1024*1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing from application configuration.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsAppConfig(
            [Values("config\\Apache.Ignite.exe.config.test", "config\\Apache.Ignite.exe.config.test2")] string config)
        {
            IgniteProcess.ReplaceConfiguration(config);

            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            var proc = new IgniteProcess();

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var minMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 602*1024*1024, minMem);

            var maxMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 702*1024*1024, maxMem);

            proc.Kill();

            Assert.IsTrue(_grid.WaitTopology(1));

            // Command line options overwrite config file options
            // ReSharper disable once RedundantAssignment
            proc = new IgniteProcess("-J-Xms606m", "-J-Xmx706m");

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            minMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 606*1024*1024, minMem);

            maxMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 706*1024*1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: custom options + raw options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdCombined()
        {
            var proc = new IgniteProcess(
                "-springConfigUrl=" + SpringCfgPath,
                "-J-Xms556m",
                "-J-Xmx666m",
                "-JvmInitialMemoryMB=128",
                "-JvmMaxMemoryMB=256"
                );

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            // Raw JVM options (Xms/Xmx) should override custom options
            var minMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MinMemTask, null);
            Assert.AreEqual((long) 556*1024*1024, minMem);

            var maxMem = _grid.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<long>(MaxMemTask, null);
            AssertJvmMaxMemory((long) 666*1024*1024, maxMem);
        }

        /// <summary>
        /// Tests the .NET XML configuration specified in app.config.
        /// </summary>
        [Test]
        public void TestXmlConfigurationAppConfig()
        {
            IgniteProcess.ReplaceConfiguration("config\\Apache.Ignite.exe.config.test3");

            var proc = new IgniteProcess();

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var remoteCfg = RemoteConfig();
            Assert.IsTrue(remoteCfg.JvmOptions.Contains("-DOPT25"));

            proc.Kill();

            Assert.IsTrue(_grid.WaitTopology(1));
        }

        /// <summary>
        /// Tests the .NET XML configuration specified in command line.
        /// </summary>
        [Test]
        public void TestXmlConfigurationCmd()
        {
            var proc = new IgniteProcess("-configFileName=config\\ignite-dotnet-cfg.xml");

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var remoteCfg = RemoteConfig();
            Assert.IsTrue(remoteCfg.JvmOptions.Contains("-DOPT25"));

            proc.Kill();

            Assert.IsTrue(_grid.WaitTopology(1));
        }

        /// <summary>
        /// Tests invalid command arguments.
        /// </summary>
        [Test]
        public void TestInvalidCmdArgs()
        {
            var ignoredWarns = new[]
            {
                "WARNING: An illegal reflective access operation has occurred",
                "WARNING: Illegal reflective access by org.apache.ignite.internal.util.GridUnsafe$2 " +
                "(file:/C:/w/incubator-ignite/modules/core/target/classes/) to field java.nio.Buffer.address",
                "WARNING: Please consider reporting this to the maintainers of org.apache.ignite.internal.util." +
                "GridUnsafe$2",
                "WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations",
                "WARNING: All illegal access operations will be denied in a future release"
            };

            Action<string, string> checkError = (args, err) =>
            {
                var reader = new ListDataReader();
                var proc = new IgniteProcess(reader, args);

                int exitCode;
                Assert.IsTrue(proc.Join(30000, out exitCode));
                Assert.AreEqual(-1, exitCode);

                Assert.AreEqual(err, reader.GetOutput()
                    .Except(ignoredWarns)
                    .FirstOrDefault(x => !string.IsNullOrWhiteSpace(x)));
            };

            checkError("blabla", "ERROR: Apache.Ignite.Core.Common.IgniteException: Missing argument value: " +
                                 "'blabla'. See 'Apache.Ignite.exe /help'");

            checkError("blabla=foo", "ERROR: Apache.Ignite.Core.Common.IgniteException: " +
                                     "Unknown argument: 'blabla'. See 'Apache.Ignite.exe /help'");

            checkError("assembly=", "ERROR: Apache.Ignite.Core.Common.IgniteException: Missing argument value: " +
                                 "'assembly'. See 'Apache.Ignite.exe /help'");

            checkError("assembly=x.dll", "ERROR: Apache.Ignite.Core.Common.IgniteException: " +
                                         "Failed to load assembly: x.dll");

            checkError("configFileName=wrong.config", "ERROR: System.Configuration.ConfigurationErrorsException: " +
                                                      "Specified config file does not exist: wrong.config");

            checkError("configSectionName=wrongSection", "ERROR: System.Configuration.ConfigurationErrorsException: " +
                                                         "Could not find IgniteConfigurationSection " +
                                                         "in current application configuration");

            checkError("JvmInitialMemoryMB=A_LOT", "ERROR: System.InvalidOperationException: Failed to configure " +
                                                   "Ignite: property 'JvmInitialMemoryMB' has value 'A_LOT', " +
                                                   "which is not an integer.");

            checkError("JvmMaxMemoryMB=ALL_OF_IT", "ERROR: System.InvalidOperationException: Failed to configure " +
                                                   "Ignite: property 'JvmMaxMemoryMB' has value 'ALL_OF_IT', " +
                                                   "which is not an integer.");
        }

        /// <summary>
        /// Tests a scenario where XML config has references to types from dynamically loaded assemblies.
        /// </summary>
        [Test]
        public void TestXmlConfigurationReferencesTypesFromDynamicallyLoadedAssemblies()
        {
            const string code = @"
                using System;
                using Apache.Ignite.Core.Log;
                namespace CustomNs { 
                    class CustomLogger : ILogger { 
                        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, 
                                        string category, string nativeErrorInfo, Exception ex) {} 
                        public bool IsEnabled(LogLevel level) { return true; } 
                } }";

            var dllPath = GenerateDll("CustomAsm.dll", true, code);

            var proc = new IgniteProcess(
                "-configFileName=config\\ignite-dotnet-cfg-logger.xml",
                "-assembly=" + dllPath);

            Assert.IsTrue(proc.Alive);
            Assert.IsTrue(_grid.WaitTopology(2));

            var remoteCfg = RemoteConfig();
            Assert.AreEqual("CustomNs.CustomLogger", remoteCfg.LoggerTypeName);
        }

        /// <summary>
        /// Get remote node configuration.
        /// </summary>
        /// <returns>Configuration.</returns>
        private RemoteConfiguration RemoteConfig()
        {
            return _grid.GetCluster().ForRemotes().GetCompute().Call(new RemoteConfigurationClosure());
        }

        /// <summary>
        /// Generates a DLL dynamically.
        /// </summary>
        /// <param name="outputPath">Target path.</param>
        /// <param name="randomPath">Whether to use random path.</param>
        /// <param name="code">Code to compile.</param>
        private string GenerateDll(string outputPath, bool randomPath = false, string code = null)
        {
            // Put resulting DLLs to the random temp dir to make sure they are not resolved from current dir.
            var resPath = randomPath ? Path.Combine(_tempDir, outputPath) : outputPath;

            var parameters = new CompilerParameters
            {
                GenerateExecutable = false,
                OutputAssembly = resPath,
                ReferencedAssemblies = { typeof(IIgnite).Assembly.Location }
            };

            var src = code ?? "namespace Apache.Ignite.Client.Test { public class Foo {}}";

            var results = CodeDomProvider.CreateProvider("CSharp").CompileAssemblyFromSource(parameters, src);

            Assert.False(
                results.Errors.HasErrors,
                string.Join(Environment.NewLine, results.Errors.Cast<CompilerError>().Select(e => e.ToString())));

            return resPath;
        }

        /// <summary>
        /// Asserts that JVM maximum memory corresponds to Xmx parameter value.
        /// </summary>
        private static void AssertJvmMaxMemory(long expected, long actual)
        {
            // allow 20% tolerance because max memory in Java is not exactly equal to Xmx parameter value
            Assert.LessOrEqual(actual, expected / 4 * 5);
            Assert.Greater(actual, expected / 5 * 4);
        }

        /// <summary>
        /// Closure which extracts configuration and passes it back.
        /// </summary>
        private class RemoteConfigurationClosure : IComputeFunc<RemoteConfiguration>
        {

#pragma warning disable 0649

            /** Grid. */
            [InstanceResource] private IIgnite _grid;

#pragma warning restore 0649

            /** <inheritDoc /> */

            public RemoteConfiguration Invoke()
            {
                var grid0 = (Ignite) _grid;

                var cfg = grid0.Configuration;

                var res = new RemoteConfiguration
                {
                    IgniteHome = cfg.IgniteHome,
                    SpringConfigUrl = cfg.SpringConfigUrl,
                    JvmDll = cfg.JvmDllPath,
                    JvmClasspath = cfg.JvmClasspath,
                    JvmOptions = cfg.JvmOptions,
                    Assemblies = cfg.Assemblies,
                    LoadedAssemblies = AppDomain.CurrentDomain.GetAssemblies().Select(a => a.GetName().Name).ToArray(),
                    JvmInitialMemoryMb = cfg.JvmInitialMemoryMb,
                    JvmMaxMemoryMb = cfg.JvmMaxMemoryMb,
                    LoggerTypeName = cfg.Logger == null ? null : cfg.Logger.GetType().FullName
                };

                Console.WriteLine("RETURNING CFG: " + cfg);

                return res;
            }
        }

        /// <summary>
        /// Configuration.
        /// </summary>
        private class RemoteConfiguration
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
            /// Assemblies.
            /// </summary>
            public ICollection<string> LoadedAssemblies { get; set; }

            /// <summary>
            /// Minimum JVM memory (Xms).
            /// </summary>
            public int JvmInitialMemoryMb { get; set; }

            /// <summary>
            /// Maximum JVM memory (Xms).
            /// </summary>
            public int JvmMaxMemoryMb { get; set; }

            /// <summary>
            /// Logger type name.
            /// </summary>
            public string LoggerTypeName { get; set; }
        }
    }
}

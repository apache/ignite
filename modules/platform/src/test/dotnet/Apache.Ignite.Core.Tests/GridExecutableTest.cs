/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

// ReSharper disable UnusedVariable
namespace GridGain.Client
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;

    using GridGain.Client.Process;
    using GridGain.Compute;
    using GridGain.Impl;
    using GridGain.Portable;
    using GridGain.Resource;    

    using Microsoft.CSharp;

    using NUnit.Framework;

    /// <summary>
    /// Tests for executable.
    /// </summary>
    public class GridExecutableTest
    {
        /** Spring configuration path. */
        private static readonly string SPRING_CFG_PATH = "config\\compute\\compute-standalone.xml";

        /** Min memory Java task. */
        private const string MIN_MEM_TASK = "org.gridgain.interop.GridInteropMinMemoryTask";

        /** Max memory Java task. */
        private const string MAX_MEM_TASK = "org.gridgain.interop.GridInteropMaxMemoryTask";

        /** Grid. */
        private IIgnite grid;

        /// <summary>
        /// Test fixture set-up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            GridTestUtils.KillProcesses();

            grid = GridFactory.Start(Configuration(SPRING_CFG_PATH));
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            GridFactory.StopAll(true);

            GridTestUtils.KillProcesses();
        }

        /// <summary>
        /// Set-up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            GridTestUtils.KillProcesses();

            Assert.IsTrue(grid.WaitTopology(1, 30000));

            GridProcess.SaveConfigurationBackup();
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            GridProcess.RestoreConfigurationBackup();
        }

        /// <summary>
        /// Test data pass through configuration file.
        /// </summary>
        [Test]
        public void TestConfig()
        {
            GridProcess.ReplaceConfiguration("config\\GridGain.exe.config.test");

            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            GridProcess proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath()
            );

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            RemoteConfiguration cfg = RemoteConfig();

            Assert.AreEqual(SPRING_CFG_PATH, cfg.SpringConfigUrl);
            Assert.IsTrue(cfg.JvmOptions.Contains("-DOPT1") && cfg.JvmOptions.Contains("-DOPT2"));
            Assert.IsTrue(cfg.Assemblies.Contains("test-1.dll") && cfg.Assemblies.Contains("test-2.dll"));
            Assert.AreEqual(601, cfg.JvmInitialMemoryMB);
            Assert.AreEqual(702, cfg.JvmMaxMemoryMB);
        }

        /// <summary>
        /// Test assemblies passing through command-line. 
        /// </summary>
        [Test]
        public void TestAssemblyCmd()
        {
            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            GridProcess proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SPRING_CFG_PATH,
                "-assembly=test-1.dll",
                "-assembly=test-2.dll"
            );

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            RemoteConfiguration cfg = RemoteConfig();

            Assert.IsTrue(cfg.Assemblies.Contains("test-1.dll") && cfg.Assemblies.Contains("test-2.dll"));
        }

        /// <summary>
        /// Test JVM options passing through command-line. 
        /// </summary>
        [Test]
        public void TestJvmOptsCmd()
        {
            GridProcess proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SPRING_CFG_PATH,
                "-J-DOPT1", 
                "-J-DOPT2"            
            );

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            RemoteConfiguration cfg = RemoteConfig();

            Assert.IsTrue(cfg.JvmOptions.Contains("-DOPT1") && cfg.JvmOptions.Contains("-DOPT2"));
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: raw java options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdRaw()
        {
            var proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SPRING_CFG_PATH,
                "-J-Xms506m",
                "-J-Xmx607m"
            );

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            var minMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MIN_MEM_TASK, null);
            Assert.AreEqual((long) 506 * 1024 * 1024, minMem);

            var maxMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MAX_MEM_TASK, null);
            AssertJvmMaxMemory((long) 607 * 1024 * 1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: custom options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdCustom()
        {
            var proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SPRING_CFG_PATH,
                "-JvmInitialMemoryMB=615",
                "-JvmMaxMemoryMB=863"
            );

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            var minMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MIN_MEM_TASK, null);
            Assert.AreEqual((long) 615 * 1024 * 1024, minMem);

            var maxMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MAX_MEM_TASK, null);
            AssertJvmMaxMemory((long) 863 * 1024 * 1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing from application configuration.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsAppConfig()
        {
            GridProcess.ReplaceConfiguration("config\\GridGain.exe.config.test");

            GenerateDll("test-1.dll");
            GenerateDll("test-2.dll");

            var proc = new GridProcess("-jvmClasspath=" + GridTestUtils.CreateTestClasspath());

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            var minMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MIN_MEM_TASK, null);
            Assert.AreEqual((long) 601 * 1024 * 1024, minMem);

            var maxMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MAX_MEM_TASK, null);
            AssertJvmMaxMemory((long) 702 * 1024 * 1024, maxMem);

            proc.Kill();

            Assert.IsTrue(grid.WaitTopology(1, 30000));
            
            // Command line options overwrite config file options
            // ReSharper disable once RedundantAssignment
            proc = new GridProcess("-jvmClasspath=" + GridTestUtils.CreateTestClasspath(), 
                "-J-Xms605m", "-J-Xmx706m");

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            minMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MIN_MEM_TASK, null);
            Assert.AreEqual((long) 605 * 1024 * 1024, minMem);

            maxMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MAX_MEM_TASK, null);
            AssertJvmMaxMemory((long) 706 * 1024 * 1024, maxMem);
        }

        /// <summary>
        /// Test JVM memory options passing through command-line: custom options + raw options.
        /// </summary>
        [Test]
        public void TestJvmMemoryOptsCmdCombined()
        {
            var proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + SPRING_CFG_PATH,
                "-J-Xms555m",
                "-J-Xmx666m",
                "-JvmInitialMemoryMB=128",
                "-JvmMaxMemoryMB=256"
            );

            Assert.IsTrue(grid.WaitTopology(2, 30000));

            // Raw JVM options (Xms/Xmx) should override custom options
            var minMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MIN_MEM_TASK, null);
            Assert.AreEqual((long) 555 * 1024 * 1024, minMem);

            var maxMem = grid.Cluster.ForRemotes().Compute().ExecuteJavaTask<long>(MAX_MEM_TASK, null);
            AssertJvmMaxMemory((long) 666 * 1024 * 1024, maxMem);
        }

        /// <summary>
        /// Get remote node configuration.
        /// </summary>
        /// <returns>Configuration.</returns>
        private RemoteConfiguration RemoteConfig()
        {
            return grid.Cluster.ForRemotes().Compute().Call(new RemoteConfigurationClosure());
        }

        /// <summary>
        /// Configuration for node.
        /// </summary>
        /// <param name="path">Path to Java XML configuration.</param>
        /// <returns>Node configuration.</returns>
        private static GridConfiguration Configuration(string path)
        {
            GridConfiguration cfg = new GridConfiguration();

            
            PortableConfiguration portCfg = new PortableConfiguration();

            ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(RemoteConfiguration)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(RemoteConfigurationClosure)));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.PortableConfiguration = portCfg;

            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            cfg.JvmOptions = new List<string> { "-ea", "-Xcheck:jni", "-Xms4g", "-Xmx4g", "-DGRIDGAIN_QUIET=false", "-Xnoagent", "-Djava.compiler=NONE", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005", "-XX:+HeapDumpOnOutOfMemoryError" };

            cfg.SpringConfigUrl = path;

            return cfg;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="outputPath"></param>
        private static void GenerateDll(string outputPath)
        {
            CSharpCodeProvider codeProvider = new CSharpCodeProvider();

#pragma warning disable 0618

            ICodeCompiler icc = codeProvider.CreateCompiler();

#pragma warning restore 0618

            CompilerParameters parameters = new CompilerParameters();
            parameters.GenerateExecutable = false;
            parameters.OutputAssembly = outputPath;

            string src = "namespace GridGain.Client.Test { public class Foo {}}";

            CompilerResults results = icc.CompileAssemblyFromSource(parameters, src);

            Assert.False(results.Errors.HasErrors);
        }

        /// <summary>
        /// Asserts that JVM maximum memory corresponds to Xmx parameter value.
        /// </summary>
        private static void AssertJvmMaxMemory(long expected, long actual)
        {
            // allow 20% tolerance because max memory in Java is not exactly equal to Xmx parameter value
            Assert.LessOrEqual(actual, expected);
            Assert.Greater(actual, expected / 5 * 4); 
        }

        /// <summary>
        /// Closure which extracts configuration and passes it back.
        /// </summary>
        public class RemoteConfigurationClosure : IComputeFunc<RemoteConfiguration>
        {

#pragma warning disable 0649

            /** Grid. */
            [InstanceResource]
            private IIgnite grid;

#pragma warning restore 0649

            /** <inheritDoc /> */
            public RemoteConfiguration Invoke()
            {
                GridImpl grid0 = (GridImpl) ((GridProxy) grid).Target;

                GridConfiguration cfg = grid0.Configuration;

                RemoteConfiguration res = new RemoteConfiguration
                {
                    GridGainHome = cfg.GridGainHome,
                    SpringConfigUrl = cfg.SpringConfigUrl,
                    JvmDll = cfg.JvmDllPath,
                    JvmClasspath = cfg.JvmClasspath,
                    JvmOptions = cfg.JvmOptions,
                    Assemblies = cfg.Assemblies,
                    JvmInitialMemoryMB = cfg.JvmInitialMemoryMB,
                    JvmMaxMemoryMB = cfg.JvmMaxMemoryMB
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
            public string GridGainHome
            {
                get;
                set;
            }

            /// <summary>
            /// Spring config URL.
            /// </summary>
            public string SpringConfigUrl
            {
                get;
                set;
            }

            /// <summary>
            /// JVM DLL.
            /// </summary>
            public string JvmDll
            {
                get;
                set;
            }

            /// <summary>
            /// JVM classpath.
            /// </summary>
            public string JvmClasspath
            {
                get;
                set;
            }

            /// <summary>
            /// JVM options.
            /// </summary>
            public ICollection<string> JvmOptions
            {
                get;
                set;
            }

            /// <summary>
            /// Assemblies.
            /// </summary>
            public ICollection<string> Assemblies
            {
                get;
                set;
            }
            
            /// <summary>
            /// Minimum JVM memory (Xms).
            /// </summary>
            public int JvmInitialMemoryMB
            {
                get;
                set;
            }

            /// <summary>
            /// Maximum JVM memory (Xms).
            /// </summary>
            public int JvmMaxMemoryMB
            {
                get;
                set;
            }

        }
    }
}

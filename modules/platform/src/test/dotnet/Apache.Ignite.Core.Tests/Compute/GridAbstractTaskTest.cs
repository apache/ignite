/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    using GridGain;
    using GridGain.Portable;
    using GridGain.Client.Process;

    using NUnit.Framework;

    /// <summary>
    /// Base class for all task-related tests.
    /// </summary>
    public abstract class GridAbstractTaskTest
    {
        /** */
        protected const string GRID1_NAME = "grid1";

        /** */
        protected const string GRID2_NAME = "grid2";

        /** */
        protected const string GRID3_NAME = "grid3";

        /** */
        protected const string CACHE1_NAME = "cache1";

        /** Whether this is a test with forked JVMs. */
        private readonly bool fork;

        /** First node. */
        [NonSerialized]
        protected IGrid grid1;

        /** Second node. */
        [NonSerialized]
        private IGrid grid2;

        /** Third node. */
        [NonSerialized]
        private IGrid grid3;

        /** Second process. */
        [NonSerialized]
        private GridProcess proc2;

        /** Third process. */
        [NonSerialized]
        private GridProcess proc3;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected GridAbstractTaskTest(bool fork)
        {
            this.fork = fork;
        }

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            GridTestUtils.KillProcesses();

            if (fork)
            {
                grid1 = GridFactory.Start(Configuration("config\\compute\\compute-standalone.xml"));

                proc2 = Fork("config\\compute\\compute-standalone.xml");

                while (true)
                {
                    if (!proc2.Alive)
                        throw new Exception("Process 2 died unexpectedly: " + proc2.Join());

                    if (grid1.Cluster.Nodes().Count < 2)
                        Thread.Sleep(100);
                    else
                        break;
                }

                proc3 = Fork("config\\compute\\compute-standalone.xml");

                while (true)
                {
                    if (!proc3.Alive)
                        throw new Exception("Process 3 died unexpectedly: " + proc3.Join());

                    if (grid1.Cluster.Nodes().Count < 3)
                        Thread.Sleep(100);
                    else
                        break;
                }
            }
            else
            {
                grid1 = GridFactory.Start(Configuration("config\\compute\\compute-grid1.xml"));
                grid2 = GridFactory.Start(Configuration("config\\compute\\compute-grid2.xml"));
                grid3 = GridFactory.Start(Configuration("config\\compute\\compute-grid3.xml"));
            }
        }

        [SetUp]
        public void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        [TestFixtureTearDown]
        public void StopClient()
        {
            if (grid1 != null)
                GridFactory.Stop(grid1.Name, true);

            if (fork)
            {
                if (proc2 != null) {
                    proc2.Kill();

                    proc2.Join();
                }

                if (proc3 != null)
                {
                    proc3.Kill();

                    proc3.Join();
                }
            }
            else
            {
                if (grid2 != null)
                    GridFactory.Stop(grid2.Name, true);

                if (grid3 != null)
                    GridFactory.Stop(grid3.Name, true);
            }
        }

        /// <summary>
        /// Configuration for node.
        /// </summary>
        /// <param name="path">Path to Java XML configuration.</param>
        /// <returns>Node configuration.</returns>
        protected GridConfiguration Configuration(string path)
        {
            GridConfiguration cfg = new GridConfiguration();

            if (!fork)
            {
                PortableConfiguration portCfg = new PortableConfiguration();

                ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

                PortableTypeConfigurations(portTypeCfgs);

                portCfg.TypeConfigurations = portTypeCfgs;

                cfg.PortableConfiguration = portCfg;
            }

            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            cfg.JvmOptions = GridTestUtils.TestJavaOptions();

            cfg.SpringConfigUrl = path;

            return cfg;
        }

        /// <summary>
        /// Create forked process with the following Spring config.
        /// </summary>
        /// <param name="path">Path to Java XML configuration.</param>
        /// <returns>Forked process.</returns>
        private static GridProcess Fork(string path)
        {
            return new GridProcess(
                "-springConfigUrl=" + path,
                "-J-ea",
                "-J-Xcheck:jni",
                "-J-Xms512m",
                "-J-Xmx512m",
                "-J-DIGNITE_QUIET=false"
                //"-J-Xnoagent", "-J-Djava.compiler=NONE", "-J-Xdebug", "-J-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5006"
            );
        }

        /// <summary>
        /// Define portable types.
        /// </summary>
        /// <param name="portTypeCfgs">Portable type configurations.</param>
        protected virtual void PortableTypeConfigurations(ICollection<PortableTypeConfiguration> portTypeCfgs)
        {
            // No-op.
        }
    }
}

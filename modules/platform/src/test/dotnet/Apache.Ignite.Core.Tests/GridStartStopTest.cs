/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client 
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Compute;

    using NUnit.Framework;

    /// <summary>
    /// 
    /// </summary>
    [Category(GridTestUtils.CATEGORY_INTENSIVE)]
    public class GridStartStopTest
    {
        /// <summary>
        /// 
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            GridTestUtils.KillProcesses();
        }

        /// <summary>
        /// 
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            GridFactory.StopAll(true);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartDefault()
        {
            GridConfiguration cfg = new GridConfiguration();

            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IGrid grid = GridFactory.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.Cluster.Nodes().Count);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartWithConfigPath()
        {
            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config/default-config.xml";
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IGrid grid = GridFactory.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.Cluster.Nodes().Count);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartGetStop()
        {
            List<string> cfgs = new List<string> { "config\\start-test-grid1.xml", "config\\start-test-grid2.xml", "config\\start-test-grid3.xml" };

            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = cfgs[0];
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IGrid grid1 = GridFactory.Start(cfg);

            Assert.AreEqual("grid1", grid1.Name);

            cfg.SpringConfigUrl = cfgs[1];

            IGrid grid2 = GridFactory.Start(cfg);

            Assert.AreEqual("grid2", grid2.Name);

            cfg.SpringConfigUrl = cfgs[2];

            IGrid grid3 = GridFactory.Start(cfg);

            Assert.IsNull(grid3.Name);

            Assert.AreSame(grid1, GridFactory.Grid("grid1"));

            Assert.AreSame(grid2, GridFactory.Grid("grid2"));

            Assert.AreSame(grid3, GridFactory.Grid(null));

            try
            {
                GridFactory.Grid("invalid_name");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            Assert.IsTrue(GridFactory.Stop("grid1", true));

            try
            {
                GridFactory.Grid("grid1");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            grid2.Dispose();

            try
            {
                GridFactory.Grid("grid2");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            grid3.Dispose();

            try
            {
                GridFactory.Grid(null);
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            foreach (string cfgName in cfgs)
            {
                cfg.SpringConfigUrl = cfgName;
                cfg.JvmOptions = GridTestUtils.TestJavaOptions();

                GridFactory.Start(cfg);
            }

            foreach (string gridName in new List<string> { "grid1", "grid2", null })
                Assert.IsNotNull(GridFactory.Grid(gridName));

            GridFactory.StopAll(true);

            foreach (string gridName in new List<string> { "grid1", "grid2", null })
            {
                try
                {
                    GridFactory.Grid(gridName);
                }
                catch (IgniteException e)
                {
                    Console.WriteLine("Expected exception: " + e);
                }
            }
        }

        /*
        [Test]
        public void TestStartInvalidJvmOptions()
        {
            GridGain.Impl.GridManager.DestroyJvm();

            GridConfiguration cfg = new GridConfiguration();

            cfg.NativeXmlConfig = "config\\start-test-grid1.xml";
            cfg.NativeJvmOptions = new List<string> { "invalid_option"};

            try
            {
                GridFactory.Start(cfg);

                Assert.Fail("Start should fail.");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            cfg.NativeJvmOptions = new List<string> { "-Xmx1g", "-Xms1g" };

            GridFactory.Start(cfg);
        }
        */

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartTheSameName()
        {
            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid1.xml";
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IGrid grid1 = GridFactory.Start(cfg);

            Assert.AreEqual("grid1", grid1.Name);

            try
            {
                GridFactory.Start(cfg);

                Assert.Fail("Start should fail.");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestUsageAfterStop()
        {
            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid1.xml";
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IGrid grid = GridFactory.Start(cfg);

            Assert.IsNotNull(grid.Cache<int, int>("cache1"));

            grid.Dispose();

            try
            {
                grid.Cache<int, int>("cache1");

                Assert.Fail();
            }
            catch (InvalidOperationException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartStopLeak()
        {
            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid1.xml";
            cfg.JvmOptions = new List<string> { "-Xcheck:jni", "-Xms256m", "-Xmx256m", "-XX:+HeapDumpOnOutOfMemoryError" };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            for (int i = 0; i < 20; i++)
            {
                Console.WriteLine("Iteration: " + i);

                IGrid grid = GridFactory.Start(cfg);

                UseGrid(grid);

                if (i % 2 == 0) // Try to stop grid from another thread.
                {
                    Thread t = new Thread(() => {
                        grid.Dispose();
                    });

                    t.Start();

                    t.Join();
                }
                else
                    grid.Dispose();

                GC.Collect(); // At the time of writing java references are cleaned from finalizer, so GC is needed.
            }
        }

        /// <summary>
        /// Tests the client mode flag.
        /// </summary>
        [Test]
        public void TestClientMode()
        {
            var servCfg = new GridConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid1.xml",
                JvmOptions = GridTestUtils.TestJavaOptions(),
                JvmClasspath = GridTestUtils.CreateTestClasspath()
            };

            var clientCfg = new GridConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid2.xml",
                JvmOptions = GridTestUtils.TestJavaOptions(),
                JvmClasspath = GridTestUtils.CreateTestClasspath()
            };

            try
            {
                using (GridFactory.Start(servCfg))  // start server-mode grid first
                {
                    GridFactory.ClientMode = true;

                    using (var grid = GridFactory.Start(clientCfg))
                    {
                        UseGrid(grid);
                    }
                }
            }
            finally 
            {
                GridFactory.ClientMode = false;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="grid"></param>
        private void UseGrid(IGrid grid)
        {
            // Create objects holding references to java objects.

            ICompute comp = grid.Compute();

            comp = comp.WithKeepPortable();

            IClusterGroup prj = grid.Cluster.ForOldest();

            Assert.IsTrue(prj.Nodes().Count > 0);

            Assert.IsNotNull(prj.Compute());

            var cache = grid.Cache<int, int>("cache1");

            Assert.IsNotNull(cache);

            cache.GetAndPut(1, 1);

            Assert.AreEqual(1, cache.Get(1));
        }
    }
}

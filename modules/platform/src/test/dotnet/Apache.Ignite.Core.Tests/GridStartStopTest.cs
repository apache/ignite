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
    using System.Threading;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// 
    /// </summary>
    [Category(GridTestUtils.CategoryIntensive)]
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
            Ignition.StopAll(true);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartDefault()
        {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.Cluster.Nodes().Count);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartWithConfigPath()
        {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.SpringConfigUrl = "config/default-config.xml";
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = Ignition.Start(cfg);

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

            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.SpringConfigUrl = cfgs[0];
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid1 = Ignition.Start(cfg);

            Assert.AreEqual("grid1", grid1.Name);

            cfg.SpringConfigUrl = cfgs[1];

            IIgnite grid2 = Ignition.Start(cfg);

            Assert.AreEqual("grid2", grid2.Name);

            cfg.SpringConfigUrl = cfgs[2];

            IIgnite grid3 = Ignition.Start(cfg);

            Assert.IsNull(grid3.Name);

            Assert.AreSame(grid1, Ignition.GetIgnite("grid1"));

            Assert.AreSame(grid2, Ignition.GetIgnite("grid2"));

            Assert.AreSame(grid3, Ignition.GetIgnite(null));

            try
            {
                Ignition.GetIgnite("invalid_name");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            Assert.IsTrue(Ignition.Stop("grid1", true));

            try
            {
                Ignition.GetIgnite("grid1");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            grid2.Dispose();

            try
            {
                Ignition.GetIgnite("grid2");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            grid3.Dispose();

            try
            {
                Ignition.GetIgnite(null);
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            foreach (string cfgName in cfgs)
            {
                cfg.SpringConfigUrl = cfgName;
                cfg.JvmOptions = GridTestUtils.TestJavaOptions();

                Ignition.Start(cfg);
            }

            foreach (string gridName in new List<string> { "grid1", "grid2", null })
                Assert.IsNotNull(Ignition.GetIgnite(gridName));

            Ignition.StopAll(true);

            foreach (string gridName in new List<string> { "grid1", "grid2", null })
            {
                try
                {
                    Ignition.GetIgnite(gridName);
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

            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.NativeXmlConfig = "config\\start-test-grid1.xml";
            cfg.NativeJvmOptions = new List<string> { "invalid_option"};

            try
            {
                Ignition.Start(cfg);

                Assert.Fail("Start should fail.");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            cfg.NativeJvmOptions = new List<string> { "-Xmx1g", "-Xms1g" };

            Ignition.Start(cfg);
        }
        */

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartTheSameName()
        {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid1.xml";
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid1 = Ignition.Start(cfg);

            Assert.AreEqual("grid1", grid1.Name);

            try
            {
                Ignition.Start(cfg);

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
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid1.xml";
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = Ignition.Start(cfg);

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
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid1.xml";
            cfg.JvmOptions = new List<string> { "-Xcheck:jni", "-Xms256m", "-Xmx256m", "-XX:+HeapDumpOnOutOfMemoryError" };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            for (int i = 0; i < 20; i++)
            {
                Console.WriteLine("Iteration: " + i);

                IIgnite grid = Ignition.Start(cfg);

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
            var servCfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid1.xml",
                JvmOptions = GridTestUtils.TestJavaOptions(),
                JvmClasspath = GridTestUtils.CreateTestClasspath()
            };

            var clientCfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid2.xml",
                JvmOptions = GridTestUtils.TestJavaOptions(),
                JvmClasspath = GridTestUtils.CreateTestClasspath()
            };

            try
            {
                using (Ignition.Start(servCfg))  // start server-mode grid first
                {
                    Ignition.ClientMode = true;

                    using (var grid = Ignition.Start(clientCfg))
                    {
                        UseGrid(grid);
                    }
                }
            }
            finally 
            {
                Ignition.ClientMode = false;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="grid"></param>
        private void UseGrid(IIgnite grid)
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

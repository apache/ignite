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
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Ignite start/stop tests.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class IgniteStartStopTest
    {
        /// <summary>
        /// 
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();
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
            var cfg = new IgniteConfiguration {JvmClasspath = TestUtils.CreateTestClasspath()};

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.Cluster.Nodes().Count);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartWithConfigPath()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config/default-config.xml",
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.Cluster.Nodes().Count);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartGetStop()
        {
            var cfgs = new List<string> { "config\\start-test-grid1.xml", "config\\start-test-grid2.xml", "config\\start-test-grid3.xml" };

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = cfgs[0],
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid1 = Ignition.Start(cfg);

            Assert.AreEqual("grid1", grid1.Name);

            cfg.SpringConfigUrl = cfgs[1];

            var grid2 = Ignition.Start(cfg);

            Assert.AreEqual("grid2", grid2.Name);

            cfg.SpringConfigUrl = cfgs[2];

            var grid3 = Ignition.Start(cfg);

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

            foreach (var cfgName in cfgs)
            {
                cfg.SpringConfigUrl = cfgName;
                cfg.JvmOptions = TestUtils.TestJavaOptions();

                Ignition.Start(cfg);
            }

            foreach (var gridName in new List<string> { "grid1", "grid2", null })
                Assert.IsNotNull(Ignition.GetIgnite(gridName));

            Ignition.StopAll(true);

            foreach (var gridName in new List<string> { "grid1", "grid2", null })
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

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestStartTheSameName()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid1.xml",
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid1 = Ignition.Start(cfg);

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
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid1.xml",
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid = Ignition.Start(cfg);

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
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid1.xml",
                JvmOptions = new List<string> {"-Xcheck:jni", "-Xms256m", "-Xmx256m", "-XX:+HeapDumpOnOutOfMemoryError"},
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            for (var i = 0; i < 20; i++)
            {
                Console.WriteLine("Iteration: " + i);

                var grid = Ignition.Start(cfg);

                UseIgnite(grid);

                if (i % 2 == 0) // Try to stop ignite from another thread.
                {
                    var t = new Thread(() => {
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
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var clientCfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid2.xml",
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            try
            {
                using (Ignition.Start(servCfg))  // start server-mode ignite first
                {
                    Ignition.ClientMode = true;

                    using (var grid = Ignition.Start(clientCfg))
                    {
                        UseIgnite(grid);
                    }
                }
            }
            finally 
            {
                Ignition.ClientMode = false;
            }
        }

        /// <summary>
        /// Uses the ignite.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        private static void UseIgnite(IIgnite ignite)
        {
            // Create objects holding references to java objects.
            var comp = ignite.Compute();

            // ReSharper disable once RedundantAssignment
            comp = comp.WithKeepPortable();

            var prj = ignite.Cluster.ForOldest();

            Assert.IsTrue(prj.Nodes().Count > 0);

            Assert.IsNotNull(prj.Compute());

            var cache = ignite.Cache<int, int>("cache1");

            Assert.IsNotNull(cache);

            cache.GetAndPut(1, 1);

            Assert.AreEqual(1, cache.Get(1));
        }
    }
}

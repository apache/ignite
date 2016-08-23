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
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Tests.Process;
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
            TestUtils.KillProcesses();
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

            Assert.AreEqual(1, grid.GetCluster().GetNodes().Count);
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

            Assert.AreEqual(1, grid.GetCluster().GetNodes().Count);
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
            Assert.AreSame(grid1, Ignition.TryGetIgnite("grid1"));

            Assert.AreSame(grid2, Ignition.GetIgnite("grid2"));
            Assert.AreSame(grid2, Ignition.TryGetIgnite("grid2"));

            Assert.AreSame(grid3, Ignition.GetIgnite(null));
            Assert.AreSame(grid3, Ignition.TryGetIgnite(null));
            Assert.AreSame(grid3, Ignition.TryGetIgnite());

            Assert.Throws<IgniteException>(() => Ignition.GetIgnite("invalid_name"));
            Assert.IsNull(Ignition.TryGetIgnite("invalid_name"));


            Assert.IsTrue(Ignition.Stop("grid1", true));
            Assert.Throws<IgniteException>(() => Ignition.GetIgnite("grid1"));

            grid2.Dispose();
            Assert.Throws<IgniteException>(() => Ignition.GetIgnite("grid2"));

            grid3.Dispose();
            Assert.Throws<IgniteException>(() => Ignition.GetIgnite("grid3"));

            foreach (var cfgName in cfgs)
            {
                cfg.SpringConfigUrl = cfgName;
                cfg.JvmOptions = TestUtils.TestJavaOptions();

                Ignition.Start(cfg);
            }

            foreach (var gridName in new List<string> { "grid1", "grid2", null })
                Assert.IsNotNull(Ignition.GetIgnite(gridName));

            Ignition.StopAll(true);

            foreach (var gridName in new List<string> {"grid1", "grid2", null})
                Assert.Throws<IgniteException>(() => Ignition.GetIgnite(gridName));
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

            Assert.IsNotNull(grid.GetCache<int, int>("cache1"));

            grid.Dispose();

            try
            {
                grid.GetCache<int, int>("cache1");

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
                using (var serv = Ignition.Start(servCfg))  // start server-mode ignite first
                {
                    Assert.IsFalse(serv.GetCluster().GetLocalNode().IsClient);

                    Ignition.ClientMode = true;

                    using (var grid = Ignition.Start(clientCfg))
                    {
                        Assert.IsTrue(grid.GetCluster().GetLocalNode().IsClient);

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
            var comp = ignite.GetCompute();

            // ReSharper disable once RedundantAssignment
            comp = comp.WithKeepBinary();

            var prj = ignite.GetCluster().ForOldest();

            Assert.IsTrue(prj.GetNodes().Count > 0);

            Assert.IsNotNull(prj.GetCompute());

            var cache = ignite.GetCache<int, int>("cache1");

            Assert.IsNotNull(cache);

            cache.GetAndPut(1, 1);

            Assert.AreEqual(1, cache.Get(1));
        }

        /// <summary>
        /// Tests the processor initialization and grid usage right after topology enter.
        /// </summary>
        [Test]
        public void TestProcessorInit()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid1.xml",
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            // Start local node
            var grid = Ignition.Start(cfg);

            // Start remote node in a separate process
            // ReSharper disable once UnusedVariable
            var proc = new IgniteProcess(
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + Path.GetFullPath(cfg.SpringConfigUrl),
                "-J-Xms512m", "-J-Xmx512m");

            var cts = new CancellationTokenSource();
            var token = cts.Token;

            // Spam message subscriptions on a separate thread 
            // to test race conditions during processor init on remote node
            var listenTask = Task.Factory.StartNew(() =>
            {
                var filter = new MessageListener();

                while (!token.IsCancellationRequested)
                {
                    var listenId = grid.GetMessaging().RemoteListen(filter);

                    grid.GetMessaging().StopRemoteListen(listenId);
                }
                // ReSharper disable once FunctionNeverReturns
            });

            // Wait for remote node to join
            Assert.IsTrue(grid.WaitTopology(2));

            // Wait some more for initialization
            Thread.Sleep(1000);

            // Cancel listen task and check that it finishes
            cts.Cancel();
            Assert.IsTrue(listenTask.Wait(5000));
        }

        /// <summary>
        /// Noop message filter.
        /// </summary>
        [Serializable]
        private class MessageListener : IMessageListener<int>
        {
            /** <inheritdoc /> */
            public bool Invoke(Guid nodeId, int message)
            {
                return true;
            }
        }
    }
}

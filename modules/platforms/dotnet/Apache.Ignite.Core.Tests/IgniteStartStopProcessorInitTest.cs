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

#if !NETCOREAPP
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.IO;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Ignite start/stop tests.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class IgniteStartStopProcessorInitTest
    {
        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            TestUtils.KillProcesses();
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the processor initialization and grid usage right after topology enter.
        /// </summary>
        [Test]
        public void TestProcessorInit()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "Config\\spring-test.xml",
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            // Start local node
            var grid = Ignition.Start(cfg);

            // Start remote node in a separate process
            // ReSharper disable once UnusedVariable
            var proc = new IgniteProcess(
                "-springConfigUrl=" + Path.GetFullPath(cfg.SpringConfigUrl),
                "-J-Xms512m",
                "-J-Xmx512m");

            Assert.IsTrue(proc.Alive);

            var cts = new CancellationTokenSource();
            var token = cts.Token;

            // Spam message subscriptions on a separate thread
            // to test race conditions during processor init on remote node
            var listenTask = TaskRunner.Run(() =>
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
#endif

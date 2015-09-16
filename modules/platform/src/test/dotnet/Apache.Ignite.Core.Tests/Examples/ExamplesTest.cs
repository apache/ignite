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

namespace Apache.Ignite.Core.Tests.Examples
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using NUnit.Framework;

    /// <summary>
    /// Tests all examples in various modes.
    /// </summary>
    public class ExamplesTest
    {
        /// <summary>
        /// Tests the example in a single node mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCases")]
        public void TestLocalNode(Example example)
        {
            example.Run();
        }

        /// <summary>
        /// Tests the example with standalone GridGain.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCases")]
        public void TestRemoteNodes(Example example)
        {
            TestRemoteNodes(example, false);
        }

        /// <summary>
        /// Tests the example with standalone GridGain.exe nodes while local node is in client mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCases")]
        public void TestRemoteNodesClientMode(Example example)
        {
            TestRemoteNodes(example, true);
        }

        /// <summary>
        /// Tests the example with standalone GridGain.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        /// <param name="clientMode">Client mode flag.</param>
        private static void TestRemoteNodes(Example example, bool clientMode)
        {
            // Exclude CrossPlatformExample and LifecycleExample
            if (string.IsNullOrEmpty(example.SpringConfigUrl))
            {
                Assert.IsTrue(new[] {"CrossPlatformExample", "LifecycleExample"}.Contains(example.Name));

                return;
            }

            Assert.IsTrue(File.Exists(example.SpringConfigUrl));

            var gridConfig = new GridConfiguration {SpringConfigUrl = example.SpringConfigUrl};

            // Try with multiple standalone nodes
            for (var i = 0; i < 2; i++)
            {
                // Start a grid to monitor topology
                // Stop it after topology check so we don't interfere with example
                GridFactory.ClientMode = false;

                using (var grid = GridFactory.Start(gridConfig))
                {
                    GridProcess.Start(example.SpringConfigUrl, example.NeedsTestDll);

                    Assert.IsTrue(WaitTopology(grid, i + 2, 30000));
                }

                GridFactory.ClientMode = clientMode;
                example.Run();
            }
        }

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Environment.SetEnvironmentVariable("GRIDGAIN_NATIVE_TEST_CLASSPATH", "true");

            Directory.SetCurrentDirectory(PathUtil.GridGainHome);
        }

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            GridFactory.ClientMode = false;
            GridProcess.KillAll();
        }

        /// <summary>
        /// Gets the test cases.
        /// </summary>
        public IEnumerable<Example> TestCases
        {
            get { return Example.All; }
        }

        /// <summary>
        /// Wait for particular topology size.
        /// </summary>
        private static bool WaitTopology(IGrid grid, int size, int timeout)
        {
            while (grid.Cluster.Nodes().Count != size)
            {
                if (timeout <= 0)
                    return false;
                
                Thread.Sleep(100);

                timeout -= 100;
            }

            return true;
        }
    }
}

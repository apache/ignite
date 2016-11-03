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
    using Apache.Ignite.Core.Tests.Process;
    using Apache.Ignite.ExamplesDll.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests all examples in various modes.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ExamplesTest
    {
        /** */
        private IDisposable _changedConfig;

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
        /// Tests the example with standalone Apache.Ignite.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCases")]
        public void TestRemoteNodes(Example example)
        {
            TestRemoteNodes(example, false);
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes while local node is in client mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCases")]
        public void TestRemoteNodesClientMode(Example example)
        {
            TestRemoteNodes(example, true);
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        /// <param name="clientMode">Client mode flag.</param>
        private static void TestRemoteNodes(Example example, bool clientMode)
        {
            // Exclude LifecycleExample
            if (string.IsNullOrEmpty(example.ConfigPath))
            {
                Assert.AreEqual("LifecycleExample", example.Name);

                return;
            }

            var configPath = Path.Combine(PathUtil.IgniteHome, PathUtil.DevPrefix, example.ConfigPath);

            // Try with multiple standalone nodes
            for (var i = 0; i < 2; i++)
            {
                // Start a grid to monitor topology
                // Stop it after topology check so we don't interfere with example
                Ignition.ClientMode = false;

                using (var ignite = Ignition.StartFromApplicationConfiguration(
                    "igniteConfiguration", configPath))
                {
                    var args = new List<string> { "-configFileName=" + configPath};

                    if (example.NeedsTestDll)
                        args.Add(" -assembly=" + typeof(AverageSalaryJob).Assembly.Location);

                    var proc = new IgniteProcess(args.ToArray());

                    Assert.IsTrue(ignite.WaitTopology(i + 2));
                    Assert.IsTrue(proc.Alive);
                }

                Ignition.ClientMode = clientMode;

                // Run twice to catch issues with standalone node state
                example.Run();
                example.Run();
            }
        }

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");

            Directory.SetCurrentDirectory(PathUtil.IgniteHome);

            _changedConfig = TestAppConfig.Change(PathUtil.ExamplesAppConfigPath);
        }

        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            _changedConfig.Dispose();
        }

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.ClientMode = false;
            IgniteProcess.KillAll();
        }

        /// <summary>
        /// Gets the test cases.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once MemberCanBeMadeStatic.Global
        public IEnumerable<Example> TestCases
        {
            get { return Example.GetExamples(); }
        }
    }
}

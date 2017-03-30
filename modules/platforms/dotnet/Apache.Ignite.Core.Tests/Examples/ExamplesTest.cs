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
    using Apache.Ignite.Core.Tests.Process;
    using Apache.Ignite.ExamplesDll.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests all examples in various modes.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ExamplesTest
    {
        /** */
        private static readonly Example[] AllExamples = Example.GetExamples().ToArray();

        /** */
        private static readonly string[] LocalOnlyExamples =
        {
            "LifecycleExample", "ClientReconnectExample", "MultiTieredCacheExample"
        };

        /** */
        private static readonly string[] NoDllExamples = { "BinaryModeExample", "NearCacheExample" };

        /** */
        private IDisposable _changedConfig;

        /** */
        private bool _remoteNodeStarted;
        /// <summary>
        /// Tests the example in a single node mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesLocal")]
        public void TestLocalNode(Example example)
        {
            StopRemoteNodes();

            if (LocalOnlyExamples.Contains(example.Name))
            {
                Assert.IsFalse(example.NeedsTestDll, "Local-only example should not mention test dll.");
                Assert.IsNull(example.ConfigPath, "Local-only example should not mention app.config path.");
            }

            example.Run();
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesRemote")]
        public void TestRemoteNodes(Example example)
        {
            TestRemoteNodes(example, false);
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes while local node is in client mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesRemote")]
        public void TestRemoteNodesClientMode(Example example)
        {
            TestRemoteNodes(example, true);
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        /// <param name="clientMode">Client mode flag.</param>
        private void TestRemoteNodes(Example example, bool clientMode)
        {
            Assert.IsTrue(PathUtil.ExamplesAppConfigPath.EndsWith(example.ConfigPath,
                StringComparison.OrdinalIgnoreCase), "All examples should use the same app.config.");

            Assert.IsTrue(example.NeedsTestDll || NoDllExamples.Contains(example.Name),
                "Examples that allow standalone nodes should mention test dll.");

            StartRemoteNodes();

            Ignition.ClientMode = clientMode;

            // Run twice to catch issues with standalone node state
            example.Run();
            example.Run();
        }

        /// <summary>
        /// Starts standalone node.
        /// </summary>
        private void StartRemoteNodes()
        {
            if (_remoteNodeStarted)
                return;

            // Start a grid to monitor topology;
            // Stop it after topology check so we don't interfere with example.
            Ignition.ClientMode = false;

            using (var ignite = Ignition.StartFromApplicationConfiguration(
                "igniteConfiguration", PathUtil.ExamplesAppConfigPath))
            {
                var args = new List<string>
                {
                    "-configFileName=" + PathUtil.ExamplesAppConfigPath,
                    " -assembly=" + typeof(AverageSalaryJob).Assembly.Location
                };

                var proc = new IgniteProcess(args.ToArray());

                Assert.IsTrue(ignite.WaitTopology(2), 
                    string.Format("Standalone node failed to join topology: [{0}]", proc.GetInfo()));

                Assert.IsTrue(proc.Alive, string.Format("Standalone node stopped unexpectedly: [{0}]",
                    proc.GetInfo()));
            }

            _remoteNodeStarted = true;
        }

        /// <summary>
        /// Stops standalone nodes.
        /// </summary>
        private void StopRemoteNodes()
        {
            if (_remoteNodeStarted)
            {
                IgniteProcess.KillAll();
                _remoteNodeStarted = false;
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

            Ignition.StopAll(true);

            IgniteProcess.KillAll();
        }

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.ClientMode = false;
        }

        /// <summary>
        /// Gets the test cases for local-only scenario.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once MemberCanBeMadeStatic.Global
        public IEnumerable<Example> TestCasesLocal
        {
            get { return AllExamples.Where(x => x.Name != "NearCacheExample"); }
        }

        /// <summary>
        /// Gets the test cases for remote node scenario.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once MemberCanBeMadeStatic.Global
        public IEnumerable<Example> TestCasesRemote
        {
            get
            {
                return AllExamples.Where(x => !LocalOnlyExamples.Contains(x.Name));
            }
        }
    }
}

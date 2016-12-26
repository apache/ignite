﻿/*
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
        private IDisposable _changedConfig;

        /// <summary>
        /// Tests the example in a single node mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesLocal")]
        public void TestLocalNode(Example example)
        {
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
        private static void TestRemoteNodes(Example example, bool clientMode)
        {
            Assert.IsNotEmpty(example.ConfigPath);

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

                    Assert.IsTrue(ignite.WaitTopology(i + 2), 
                        string.Format("Standalone node failed to join topology: [{0}]", proc.GetInfo()));

                    Assert.IsTrue(proc.Alive, string.Format("Standalone node stopped unexpectedly: [{0}]", 
                        proc.GetInfo()));
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
        /// Gets the test cases for local-only scenario.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once MemberCanBeMadeStatic.Global
        public IEnumerable<Example> TestCasesLocal
        {
            get { return Example.GetExamples().Where(x => x.Name != "NearCacheExample"); }
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
                var localOnly = new[] {"LifecycleExample", "ClientReconnectExample", "MultiTieredCacheExample" };

                return Example.GetExamples().Where(x => !localOnly.Contains(x.Name));
            }
        }
    }
}

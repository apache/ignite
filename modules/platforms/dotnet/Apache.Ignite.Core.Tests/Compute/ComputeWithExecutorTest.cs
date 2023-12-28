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

namespace Apache.Ignite.Core.Tests.Compute
{
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="ICompute.WithExecutor"/>.
    /// </summary>
    public class ComputeWithExecutorTest
    {
        /** Thread name task name. */
        private const string ThreadNameTask = "org.apache.ignite.platform.PlatformComputeGetThreadNameTask";

        /**  */
        private IIgnite _springConfigGrid;

        /**  */
        private IIgnite _codeConfigGrid;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cfg1 = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"Config\Compute\compute-grid-custom-executor.xml"
            };

            _springConfigGrid = Ignition.Start(cfg1);

            var cfg2 = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ExecutorConfiguration = new[]
                {
                    new ExecutorConfiguration
                    {
                        Name = "dotNetExecutor",
                        Size = 3
                    },
                    new ExecutorConfiguration
                    {
                        Name = "dotNetExecutor2",
                        Size = 1
                    }
                }
            };

            _codeConfigGrid = Ignition.Start(cfg2);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests custom executors.
        /// </summary>
        [TestCase("dotNetExecutor", "dotNetExecutor-#", true)]
        [TestCase("dotNetExecutor2", "pub-#", true)]
        [TestCase("invalid", "pub-#", true)]
        [TestCase("dotNetExecutor", "dotNetExecutor-#", false)]
        [TestCase("dotNetExecutor2", "dotNetExecutor2-#", false)]
        [TestCase("invalid", "pub-#", false)]
        public void TestWithExecutor(string executorName, string expectedThreadNamePrefix, bool springConfig)
        {
            var grid = springConfig ? _springConfigGrid : _codeConfigGrid;
            var compute = grid.GetCluster().ForLocal().GetCompute();
            var computeWithExecutor = compute.WithExecutor(executorName);

            var res = computeWithExecutor.ExecuteJavaTask<string>(ThreadNameTask, null);

            Assert.AreNotSame(compute, computeWithExecutor);
            Assert.AreEqual(expectedThreadNamePrefix, res.Substring(0, expectedThreadNamePrefix.Length));
        }
    }
}

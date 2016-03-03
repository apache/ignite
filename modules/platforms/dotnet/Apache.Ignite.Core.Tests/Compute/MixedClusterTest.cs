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

#pragma warning disable 618  // SpringConfigUrl
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute in a cluster with Java-only and .NET nodes.
    /// </summary>
    public class MixedClusterTest
    {
        /** */
        private const string SpringConfig = @"Config\Compute\compute-grid1.xml";

        /** */
        private const string SpringConfig2 = @"Config\Compute\compute-grid2.xml";

        /** */
        private const string StartTask = "org.apache.ignite.platform.PlatformStartIgniteTask";

        /** */
        private const string StopTask = "org.apache.ignite.platform.PlatformStopIgniteTask";

        /// <summary>
        /// Tests the compute.
        /// </summary>
        [Test]
        public void TestCompute()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration()) {SpringConfigUrl = SpringConfig};

            using (var ignite = Ignition.Start(cfg))
            {
                var javaNodeName = StartJavaNode(ignite, SpringConfig2);

                try
                {
                    Assert.IsTrue(ignite.WaitTopology(2));

                    TestDotNetTask(ignite);
                    TestJavaTask(ignite);
                }
                finally
                {
                    StopJavaNode(ignite, javaNodeName);
                }
            }
        }

        /// <summary>
        /// Tests the dot net task.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        private static void TestDotNetTask(IIgnite ignite)
        {
            var results = ignite.GetCompute().Broadcast(new ComputeFunc());

            // There are two nodes, but only one can execute .NET jobs.
            Assert.AreEqual(new[] {int.MaxValue}, results.ToArray());
        }

        /// <summary>
        /// Tests the dot net task.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        private static void TestJavaTask(IIgnite ignite)
        {
            // Java task can execute on both nodes.
            var res = ignite.GetCompute().ExecuteJavaTask<ICollection>(ComputeApiTest.BroadcastTask, null);

            Assert.AreEqual(2, res.Count);
        }

        /// <summary>
        /// Starts the java node.
        /// </summary>
        private static string StartJavaNode(IIgnite grid, string config)
        {
            return grid.GetCompute().ExecuteJavaTask<string>(StartTask, config);
        }

        /// <summary>
        /// Stops the java node.
        /// </summary>
        private static void StopJavaNode(IIgnite grid, string name)
        {
            grid.GetCompute().ExecuteJavaTask<object>(StopTask, name);
        }

        /// <summary>
        /// Test func.
        /// </summary>
        [Serializable]
        private class ComputeFunc : IComputeFunc<int>
        {
            /** <inheritdoc /> */
            public int Invoke()
            {
                return int.MaxValue;
            }
        }
    }
}

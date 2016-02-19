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
    using System.Diagnostics;
    using System.IO;
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

        /// <summary>
        /// Tests the compute.
        /// </summary>
        [Test]
        public void TestCompute()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration()) {SpringConfigUrl = SpringConfig};

            using (var ignite = Ignition.Start(cfg))
            {
                var proc = StartJavaNode(ignite.GetConfiguration().IgniteHome);

                try
                {
                    Assert.IsTrue(ignite.WaitTopology(2));

                    var results = ignite.GetCompute().Broadcast(new ComputeFunc());

                    // There are two nodes, but only one can execute .NET jobs
                    Assert.AreEqual(new[] {int.MaxValue}, results.ToArray());
                }
                finally
                {
                    proc.Kill();
                }
            }
        }

        /// <summary>
        /// Starts the java node.
        /// </summary>
        private static Process StartJavaNode(string igniteHome)
        {
            var batPath = Path.Combine(igniteHome, @"bin\\ignite.bat");

            var startInfo = new ProcessStartInfo(batPath, SpringConfig) {CreateNoWindow = true};

            return Process.Start(startInfo);
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

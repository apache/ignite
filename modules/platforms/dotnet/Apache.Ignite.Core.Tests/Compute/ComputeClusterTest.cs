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
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests cluster group.
    /// </summary>
    public class ComputeClusterTest
    {
        /// <summary>
        /// Tests daemons projection.
        /// </summary>
        [Test]
        public void TestForDaemons()
        {
            using (var grid = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var cluster = grid.GetCluster();

                Assert.AreEqual(0, cluster.ForDaemons().GetNodes().Count);

                using (var daemon = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    GridName = "daemon",
                    IsDaemon = true
                }))
                {
                    Assert.IsTrue(daemon.GetCluster().GetLocalNode().IsDaemon);

                    var daemons = cluster.ForDaemons().GetNodes();

                    Assert.AreEqual(1, daemons.Count);
                    Assert.IsTrue(daemons.Single().IsDaemon);
                }
            }
        }
    }
}

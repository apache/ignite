/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Dataload
{
    using System;
    using System.Threading;
    using NUnit.Framework;

    /// <summary>
    /// Streamer test with topology change.
    /// </summary>
    public class DataStreamerTestTopologyChange
    {
        /// <summary>
        /// Tests the streamer on a node without cache.
        /// </summary>
        [Test]
        public void TestNoCacheNode()
        {
            const string cacheName = "cache";

            var cacheNodeCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"Config\cache-local-node.xml",
                IgniteInstanceName = "cacheGrid"
            };

            using (var gridNoCache = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.Throws<ArgumentException>(() => gridNoCache.GetCache<int, int>(cacheName));

                var gridWithCache = Ignition.Start(cacheNodeCfg);

                var streamer = gridNoCache.GetDataStreamer<int, int>(cacheName);

                streamer.AddData(1, 2);
                streamer.Flush();

                Ignition.Stop(gridWithCache.Name, true);

                Thread.Sleep(500);  // Wait for node to stop

                var task = streamer.AddData(2, 3);
                streamer.Flush();

                var ex = Assert.Throws<AggregateException>(task.Wait).InnerException;

                Assert.IsNotNull(ex);

                Assert.AreEqual("Java exception occurred [class=org.apache.ignite.cache." +
                                "CacheServerNotFoundException, message=Failed to find server node for cache " +
                                "(all affinity nodes have left the grid or cache was stopped): cache]", ex.Message);
            }
        }

        /// <summary>
        /// Streamer test with destroyed cache.
        /// </summary>
        [Test]
        public void TestDestroyCache()
        {
            const string cacheName = "cache";

            using (var grid = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                grid.CreateCache<int, int>(cacheName);

                var streamer = grid.GetDataStreamer<int, int>(cacheName);

                var task = streamer.AddData(1, 2);
                streamer.Flush();
                task.Wait();

                grid.DestroyCache(cacheName);

                task = streamer.AddData(2, 3);
                streamer.Flush();

                var ex = Assert.Throws<AggregateException>(task.Wait).InnerException;

                Assert.IsNotNull(ex);

                Assert.AreEqual("class org.apache.ignite.IgniteCheckedException: DataStreamer data loading failed.", 
                    ex.Message);
            }
        }
    }
}

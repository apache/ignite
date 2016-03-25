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
                GridName = "cacheGrid"
            };

            using (var gridNoCache = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.Throws<ArgumentException>(() => gridNoCache.GetCache<int, int>(cacheName));

                var gridWithCache = Ignition.Start(cacheNodeCfg);

                var streamer = gridNoCache.GetDataStreamer<int, int>(cacheName);

                streamer.AddData(1, 2).Wait();
                streamer.Flush();

                Ignition.Stop(gridWithCache.Name, true);

                Thread.Sleep(500);  // Wait for node to stop

                streamer.AddData(2, 3);
                streamer.Flush();

                var cache = gridNoCache.GetCache<int, int>(cacheName);
                Assert.AreEqual(3, cache[2]);
            }
        }

        /// <summary>
        /// Streamer test with destroyed cache.
        /// </summary>
        [Test]
        public void TestDestroyCache()
        {
            // TODO
        }
    }
}

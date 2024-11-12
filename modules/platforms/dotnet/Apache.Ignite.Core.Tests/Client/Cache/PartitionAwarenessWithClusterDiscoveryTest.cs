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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.Linq;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests Partition Awareness functionality combined with Cluster Discovery.
    /// </summary>
    public class PartitionAwarenessWithClusterDiscoveryTest : PartitionAwarenessTest
    {
#if NETCOREAPP // TODO: IGNITE-15710
        [Test]
        public void CacheGet_NewNodeEnteredTopology_RequestIsRoutedToNewNode()
        {
            // Warm-up.
            Assert.AreEqual(1, _cache.Get(1));

            // Before topology change.
            Assert.AreEqual(12, _cache.Get(12));
            Assert.AreEqual(1, GetClientRequestGridIndex());

            Assert.AreEqual(14, _cache.Get(14));
            Assert.AreEqual(2, GetClientRequestGridIndex());

            // After topology change.
            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                TestUtils.WaitForTrueCondition(() =>
                {
                    // Keys 12 and 14 belong to a new node now (-1).
                    Assert.AreEqual(12, _cache.Get(12));
                    if (GetClientRequestGridIndex() != -1)
                    {
                        return false;
                    }

                    Assert.AreEqual(14, _cache.Get(14));
                    Assert.AreEqual(-1, GetClientRequestGridIndex());

                    return true;
                }, 6000);
            }
        }
#endif


        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            var cfg = base.GetClientConfiguration();

            // Enable discovery and keep only one endpoint, let the client discover others.
            cfg.EnableClusterDiscovery = true;
            cfg.Endpoints = cfg.Endpoints.Take(1).ToList();

            return cfg;
        }
    }
}

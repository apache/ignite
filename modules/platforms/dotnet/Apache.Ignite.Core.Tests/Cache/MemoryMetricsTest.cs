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

#pragma warning disable 618
namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Memory metrics tests.
    /// </summary>
    public class MemoryMetricsTest
    {
        /** */
        private const string MemoryPolicyWithMetrics = "plcWithMetrics";

        /** */
        private const string MemoryPolicyNoMetrics = "plcNoMetrics";

        /// <summary>
        /// Tests the memory metrics.
        /// </summary>
        [Test]
        public void TestMemoryMetrics()
        {
            var ignite = StartIgniteWithTwoPolicies();

            // Verify metrics.
            var metrics = ignite.GetMemoryMetrics().OrderBy(x => x.Name).ToArray();
            Assert.AreEqual(4, metrics.Length);  // two defined plus system and plus TxLog.

            var emptyMetrics = metrics[0];
            Assert.AreEqual(MemoryPolicyNoMetrics, emptyMetrics.Name);
            AssertMetricsAreEmpty(emptyMetrics);

            var memMetrics = metrics[1];
            Assert.AreEqual(MemoryPolicyWithMetrics, memMetrics.Name);
            Assert.Greater(memMetrics.AllocationRate, 0);
            Assert.AreEqual(0, memMetrics.EvictionRate);
            Assert.AreEqual(0, memMetrics.LargeEntriesPagesPercentage);
            Assert.Greater(memMetrics.PageFillFactor, 0);
            Assert.Greater(memMetrics.TotalAllocatedPages, 1000);

            var sysMetrics = metrics[2];
            Assert.AreEqual("sysMemPlc", sysMetrics.Name);
            AssertMetricsAreEmpty(sysMetrics);

            // Metrics by name.
            emptyMetrics = ignite.GetMemoryMetrics(MemoryPolicyNoMetrics);
            Assert.AreEqual(MemoryPolicyNoMetrics, emptyMetrics.Name);
            AssertMetricsAreEmpty(emptyMetrics);

            memMetrics = ignite.GetMemoryMetrics(MemoryPolicyWithMetrics);
            Assert.AreEqual(MemoryPolicyWithMetrics, memMetrics.Name);
            Assert.Greater(memMetrics.AllocationRate, 0);
            Assert.AreEqual(0, memMetrics.EvictionRate);
            Assert.AreEqual(0, memMetrics.LargeEntriesPagesPercentage);
            Assert.Greater(memMetrics.PageFillFactor, 0);
            Assert.Greater(memMetrics.TotalAllocatedPages, 1000);

            sysMetrics = ignite.GetMemoryMetrics("sysMemPlc");
            Assert.AreEqual("sysMemPlc", sysMetrics.Name);
            AssertMetricsAreEmpty(sysMetrics);

            // Invalid name.
            Assert.IsNull(ignite.GetMemoryMetrics("boo"));
        }

        /// <summary>
        /// Asserts that metrics are empty.
        /// </summary>
        private static void AssertMetricsAreEmpty(IMemoryMetrics metrics)
        {
            Assert.AreEqual(0, metrics.AllocationRate);
            Assert.AreEqual(0, metrics.EvictionRate);
            Assert.AreEqual(0, metrics.LargeEntriesPagesPercentage);
            Assert.AreEqual(0, metrics.PageFillFactor);
        }

        /// <summary>
        /// Starts the ignite with two policies.
        /// </summary>
        private static IIgnite StartIgniteWithTwoPolicies()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = null,
                MemoryConfiguration = new MemoryConfiguration
                {
                    DefaultMemoryPolicyName = MemoryPolicyWithMetrics,
                    MemoryPolicies = new[]
                    {
                        new MemoryPolicyConfiguration
                        {
                            Name = MemoryPolicyWithMetrics,
                            MetricsEnabled = true
                        },
                        new MemoryPolicyConfiguration
                        {
                            Name = MemoryPolicyNoMetrics,
                            MetricsEnabled = false
                        }
                    }
                }
            };

            var ignite = Ignition.Start(cfg);

            // Create caches and do some things with them.
            var cacheNoMetrics = ignite.CreateCache<int, int>(new CacheConfiguration("cacheNoMetrics")
            {
                MemoryPolicyName = MemoryPolicyNoMetrics
            });

            cacheNoMetrics.Put(1, 1);
            cacheNoMetrics.Get(1);

            var cacheWithMetrics = ignite.CreateCache<int, int>(new CacheConfiguration("cacheWithMetrics")
            {
                MemoryPolicyName = MemoryPolicyWithMetrics
            });

            cacheWithMetrics.Put(1, 1);
            cacheWithMetrics.Get(1);

            return ignite;
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }
    }
}

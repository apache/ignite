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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Data region metrics test.
    /// </summary>
    public class DataRegionMetricsTest
    {
        /** */
        private const string RegionWithMetrics = "regWithMetrics";

        /** */
        private const string RegionNoMetrics = "regNoMetrics";

        /** System page size overhead, see PageMemoryNoStoreImpl.PAGE_OVERHEAD. */
        private const int PageOverhead = 24;

        /// <summary>
        /// Tests the memory metrics.
        /// </summary>
        [Test]
        public void TestMemoryMetrics()
        {
            var ignite = StartIgniteWithTwoDataRegions();

            // Verify metrics.
            var metrics = ignite.GetDataRegionMetrics().OrderBy(x => x.Name).ToArray();
            Assert.AreEqual(4, metrics.Length);  // two defined plus system and plus TxLog.

            var emptyMetrics = metrics[0];
            Assert.AreEqual(RegionNoMetrics, emptyMetrics.Name);
            AssertMetricsAreEmpty(emptyMetrics);

            var memMetrics = metrics[1];
            Assert.AreEqual(RegionWithMetrics, memMetrics.Name);
            Assert.Greater(memMetrics.AllocationRate, 0);
            Assert.AreEqual(0, memMetrics.EvictionRate);
            Assert.AreEqual(0, memMetrics.LargeEntriesPagesPercentage);
            Assert.Greater(memMetrics.PageFillFactor, 0);
            Assert.Greater(memMetrics.TotalAllocatedPages, 1000);
            Assert.Greater(memMetrics.PhysicalMemoryPages, 1000);
            Assert.AreEqual(memMetrics.TotalAllocatedSize,
                memMetrics.TotalAllocatedPages * (memMetrics.PageSize + PageOverhead));
            Assert.AreEqual(memMetrics.PhysicalMemorySize,
                memMetrics.PhysicalMemoryPages * (memMetrics.PageSize + PageOverhead));

            var sysMetrics = metrics[2];
            Assert.AreEqual("sysMemPlc", sysMetrics.Name);
            AssertMetricsAreEmpty(sysMetrics);

            // Metrics by name.
            emptyMetrics = ignite.GetDataRegionMetrics(RegionNoMetrics);
            Assert.AreEqual(RegionNoMetrics, emptyMetrics.Name);
            AssertMetricsAreEmpty(emptyMetrics);

            memMetrics = ignite.GetDataRegionMetrics(RegionWithMetrics);
            Assert.AreEqual(RegionWithMetrics, memMetrics.Name);
            Assert.Greater(memMetrics.AllocationRate, 0);
            Assert.AreEqual(0, memMetrics.EvictionRate);
            Assert.AreEqual(0, memMetrics.LargeEntriesPagesPercentage);
            Assert.Greater(memMetrics.PageFillFactor, 0);
            Assert.Greater(memMetrics.TotalAllocatedPages, 1000);
            Assert.Greater(memMetrics.PhysicalMemoryPages, 1000);
            Assert.AreEqual(memMetrics.TotalAllocatedSize,
                memMetrics.TotalAllocatedPages * (memMetrics.PageSize + PageOverhead));
            Assert.AreEqual(memMetrics.PhysicalMemorySize,
                memMetrics.PhysicalMemoryPages * (memMetrics.PageSize + PageOverhead));

            sysMetrics = ignite.GetDataRegionMetrics("sysMemPlc");
            Assert.AreEqual("sysMemPlc", sysMetrics.Name);
            AssertMetricsAreEmpty(sysMetrics);

            // Invalid name.
            Assert.IsNull(ignite.GetDataRegionMetrics("boo"));
        }

        /// <summary>
        /// Asserts that metrics are empty.
        /// </summary>
        private static void AssertMetricsAreEmpty(IDataRegionMetrics metrics)
        {
            Assert.AreEqual(0, metrics.AllocationRate);
            Assert.AreEqual(0, metrics.EvictionRate);
            Assert.AreEqual(0, metrics.LargeEntriesPagesPercentage);
            Assert.AreEqual(0, metrics.PageFillFactor);
        }

        /// <summary>
        /// Starts the ignite with two policies.
        /// </summary>
        private static IIgnite StartIgniteWithTwoDataRegions()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration()
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = RegionWithMetrics,
                        MetricsEnabled = true
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = RegionNoMetrics,
                            MetricsEnabled = false
                        }
                    }
                }
            };

            var ignite = Ignition.Start(cfg);

            // Create caches and do some things with them.
            var cacheNoMetrics = ignite.CreateCache<int, int>(new CacheConfiguration("cacheNoMetrics")
            {
                DataRegionName = RegionNoMetrics
            });

            cacheNoMetrics.Put(1, 1);
            cacheNoMetrics.Get(1);

            var cacheWithMetrics = ignite.CreateCache<int, int>(new CacheConfiguration("cacheWithMetrics")
            {
                DataRegionName = RegionWithMetrics
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

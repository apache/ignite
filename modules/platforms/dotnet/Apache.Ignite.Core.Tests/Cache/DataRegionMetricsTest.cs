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
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
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

        /** */
        private const string RegionWithMetricsAndPersistence = "regWithMetricsAndPersistence";

        /** System page size overhead, see PageMemoryNoStoreImpl.PAGE_OVERHEAD. */
        private const int PageOverhead = 24;

        /** Persistent page size overhead, see PageMemoryImpl.PAGE_OVERHEAD. */
        private const int PersistentPageOverhead = 48;

        /** */
        private static readonly TimeSpan CheckpointFrequency = TimeSpan.FromSeconds(5);

        /** Temp dir for PDS. */
        private static readonly string TempDir = PathUtils.GetTempDirectoryName();

        /// <summary>
        /// Tests the memory metrics.
        /// </summary>
        [Test]
        public void TestMemoryMetrics()
        {
            var ignite = StartIgniteWithThreeDataRegions();
            
            // Verify metrics.
            var metrics = ignite.GetDataRegionMetrics()
                .OrderBy(x => x.Name, StringComparer.InvariantCultureIgnoreCase).ToArray();

            var names = metrics.Select(x => x.Name).ToArray();

            Assert.AreEqual(
                new[]
                {
                    "metastoreMemPlc",
                    RegionNoMetrics,
                    RegionWithMetrics,
                    RegionWithMetricsAndPersistence,
                    "sysMemPlc",
                    "TxLog"
                },
                names,
                string.Join(", ", names));

            var emptyMetrics = metrics[1];
            Assert.AreEqual(RegionNoMetrics, emptyMetrics.Name);
            AssertMetricsAreEmpty(emptyMetrics);

            var memMetrics = metrics[2];
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
            Assert.Greater(memMetrics.OffHeapSize, memMetrics.PhysicalMemoryPages);
            Assert.Greater(memMetrics.OffheapUsedSize, memMetrics.PhysicalMemoryPages);
            
            var sysMetrics = metrics[4];
            Assert.AreEqual("sysMemPlc", sysMetrics.Name);
            AssertMetricsAreEmpty(sysMetrics);

            // Metrics by name.
            // In-memory region.
            emptyMetrics = ignite.GetDataRegionMetrics(RegionNoMetrics);
            Assert.AreEqual(RegionNoMetrics, emptyMetrics.Name);
            AssertMetricsAreEmpty(emptyMetrics);

            // Persistence region.
            memMetrics = ignite.GetDataRegionMetrics(RegionWithMetrics);
            Assert.AreEqual(RegionWithMetrics, memMetrics.Name);
            AssertMetrics(memMetrics, false);
            
            memMetrics = ignite.GetDataRegionMetrics(RegionWithMetricsAndPersistence);
            Assert.AreEqual(RegionWithMetricsAndPersistence, memMetrics.Name);
            AssertMetrics(memMetrics, true);

            sysMetrics = ignite.GetDataRegionMetrics("sysMemPlc");
            Assert.AreEqual("sysMemPlc", sysMetrics.Name);
            AssertMetricsAreEmpty(sysMetrics);

            // Invalid name.
            Assert.IsNull(ignite.GetDataRegionMetrics("boo"));
        }

        /// <summary>
        /// Check metrics values for data region.
        /// </summary>
        /// <param name="metrics">Data region metrics.</param>
        /// <param name="isPersistent">If data region is persistent.</param>
        private static void AssertMetrics(IDataRegionMetrics metrics, bool isPersistent)
        {
            Assert.Greater(metrics.AllocationRate, 0);
            Assert.AreEqual(0, metrics.EvictionRate);
            Assert.AreEqual(0, metrics.LargeEntriesPagesPercentage);
            Assert.Greater(metrics.PageFillFactor, 0);
            Assert.Greater(metrics.TotalAllocatedPages, isPersistent ? 0 : 1000);
            Assert.Greater(metrics.PhysicalMemoryPages, isPersistent ? 0 : 1000);
            Assert.AreEqual(metrics.TotalAllocatedSize,
                metrics.TotalAllocatedPages * (metrics.PageSize + (isPersistent ? 0 : PageOverhead)));
            Assert.AreEqual(metrics.PhysicalMemorySize,
                metrics.PhysicalMemoryPages * (metrics.PageSize + (isPersistent ? PersistentPageOverhead : PageOverhead)));
            Assert.Greater(metrics.OffHeapSize, metrics.PhysicalMemoryPages);
            Assert.Greater(metrics.OffheapUsedSize, metrics.PhysicalMemoryPages);

            if (isPersistent)
            {
                Assert.Greater(metrics.PagesRead, 0);
                Assert.Greater(metrics.PagesWritten, 0);
                Assert.AreEqual(0, metrics.PagesReplaced);
                Assert.AreEqual(0, metrics.UsedCheckpointBufferPages);
                Assert.AreEqual(0, metrics.UsedCheckpointBufferSize);
                Assert.Greater(metrics.CheckpointBufferSize, 0);
#pragma warning disable 618
                Assert.AreEqual(0, metrics.CheckpointBufferPages);
#pragma warning restore 618
            }
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
            Assert.AreEqual(0, metrics.OffheapUsedSize);
        }

        /// <summary>
        /// Starts the ignite with three policies (two in-memory and one persistent).
        /// </summary>
        private static IIgnite StartIgniteWithThreeDataRegions()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration()
                {
                    CheckpointFrequency = CheckpointFrequency,
                    MetricsEnabled = true,
                    WalMode = WalMode.LogOnly,
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = RegionWithMetrics,
                        PersistenceEnabled = false,
                        MetricsEnabled = true
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = RegionNoMetrics,
                            MetricsEnabled = false
                        },
                        new DataRegionConfiguration()
                        {
                            Name = RegionWithMetricsAndPersistence,
                            PersistenceEnabled = true,
                            MetricsEnabled = true
                        }
                    }
                },  
                WorkDirectory = TempDir
            };

            var ignite = Ignition.Start(cfg);
            
            ignite.GetCluster().SetActive(true);

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
            
            var cacheWithMetricsAndPersistence = 
                ignite.CreateCache<int, int>(new CacheConfiguration("cacheWithMetricsAndPersistence")
            {
                DataRegionName = RegionWithMetricsAndPersistence
            });

            cacheWithMetricsAndPersistence.Put(1, 1);
            cacheWithMetricsAndPersistence.Get(1);

            // Wait for checkpoint. Wait for two times than CheckpointFrequency.
            Thread.Sleep(CheckpointFrequency.Add(CheckpointFrequency));
            
            return ignite;
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
            
            Directory.Delete(TempDir, true);
        }
    }
}

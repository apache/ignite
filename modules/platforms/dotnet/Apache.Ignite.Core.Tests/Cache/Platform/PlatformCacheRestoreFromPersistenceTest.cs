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

namespace Apache.Ignite.Core.Tests.Cache.Platform
{
    using System.IO;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests platform cache with native persistence.
    /// </summary>
    public class PlatformCacheRestoreFromPersistenceTest
    {
        /** Temp dir for WAL. */
        private readonly string _tempDir = PathUtils.GetTempDirectoryName();

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.ClearWorkDir();
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);

            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, true);
            }

            TestUtils.ClearWorkDir();
        }

        /// <summary>
        /// Tests that platform cache data survives node restart.
        /// </summary>
        [Test]
        public void TestPlatformCacheDataRestoresOnNodeRestart()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    StoragePath = Path.Combine(_tempDir, "Store"),
                    WalPath = Path.Combine(_tempDir, "WalStore"),
                    WalArchivePath = Path.Combine(_tempDir, "WalArchive"),
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = DataStorageConfiguration.DefaultDataRegionName,
                        PersistenceEnabled = true
                    }
                }
            };

            const string cacheName = "persistentCache";

            // Start Ignite, put data, stop.
            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCluster().SetActive(true);

                // Create cache with default region (persistence enabled), add data.
                var cache = ignite.CreateCache<int, int>(new CacheConfiguration
                {
                    Name = cacheName,
                    PlatformCacheConfiguration = new PlatformCacheConfiguration()
                });

                cache[1] = 1;

                Assert.AreEqual(1, cache.GetSize());
                Assert.AreEqual(1, cache.GetLocalSize(CachePeekMode.Platform));
            }

            // Start Ignite, verify data survival.
            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCluster().SetActive(true);

                // Platform cache is empty initially, because all entries are only on disk.
                var cache = ignite.GetCache<int, int>(cacheName);
                Assert.AreEqual(1, cache.GetSize());
                Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.Platform));

                // Read an entry and it gets into platform cache.
                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(1, cache.GetLocalSize(CachePeekMode.Platform));
                Assert.AreEqual(1, cache.LocalPeek(1, CachePeekMode.Platform));
            }
        }
    }
}

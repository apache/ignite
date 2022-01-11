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
        /** Cache name. */
        private const string CacheName = "persistentCache";

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
            // Start Ignite, put data, stop.
            using (var ignite = StartServer())
            {
                var cache = ignite.GetCache<int, int>(CacheName);

                cache[1] = 1;

                Assert.AreEqual(1, cache.GetSize());
                Assert.AreEqual(1, cache.GetLocalSize(CachePeekMode.Platform));
            }

            // Start Ignite, verify data survival.
            using (var ignite = StartServer())
            {
                // Platform cache is empty initially, because all entries are only on disk.
                var cache = ignite.GetCache<int, int>(CacheName);
                Assert.AreEqual(1, cache.GetSize());
                Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.Platform));

                // Read an entry and it gets into platform cache.
                // TODO: Test all cache operations - put, get, getAll, etc.
                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(1, cache.GetLocalSize(CachePeekMode.Platform));
                Assert.AreEqual(1, cache.LocalPeek(1, CachePeekMode.Platform));
            }
        }

        /// <summary>
        /// Starts the node.
        /// </summary>
        private IIgnite StartServer()
        {
            var ignite = Ignition.Start(GetIgniteConfiguration());

            ignite.GetCluster().SetActive(true);

            return ignite;
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
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
                },
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = CacheName,
                        PlatformCacheConfiguration = new PlatformCacheConfiguration()
                    }
                }
            };
        }
    }
}

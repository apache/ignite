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
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests platform cache with native persistence.
    /// </summary>
    public class PlatformCacheWithPersistenceTest
    {
        /** Cache name. */
        private const string CacheName = "persistentCache";

        /** Entry count. */
        private const int Count = 100;

        /** Temp dir for WAL. */
        private readonly string _tempDir = PathUtils.GetTempDirectoryName();

        /** Cache. */
        private ICache<int,int> _cache;

        /** Current key. Every test needs a key that was not used before. */
        private int _key = 1;

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            TestUtils.ClearWorkDir();

            // Start Ignite, put data, stop.
            using (var ignite = StartServer())
            {
                var cache = ignite.GetCache<int, int>(CacheName);

                cache.PutAll(Enumerable.Range(1, Count).ToDictionary(x => x, x => x));

                Assert.AreEqual(Count, cache.GetSize());
                Assert.AreEqual(Count, cache.GetLocalSize(CachePeekMode.Platform));
            }

            // Start again to test persistent data restore.
            var ignite2 = StartServer();
            _cache = ignite2.GetCache<int, int>(CacheName);

            // Platform cache is empty initially, because all entries are only on disk.
            Assert.AreEqual(Count, _cache.GetSize());
            Assert.AreEqual(0, _cache.GetLocalSize(CachePeekMode.Platform));
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TestFixtureTearDown]
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
        /// Tests get operation.
        /// </summary>
        [Test]
        public void TestGetRestoresPlatformCacheDataFromPersistence()
        {
            TestCacheOperation(k => _cache.Get(k));
        }

        /// <summary>
        /// Tests getAll operation.
        /// </summary>
        [Test]
        public void TestGetAllRestoresPlatformCacheDataFromPersistence()
        {
            TestCacheOperation(k => _cache.GetAll(new[] { k }));
        }

        /// <summary>
        /// Tests containsKey operation.
        /// </summary>
        [Test]
        public void TestContainsKeyRestoresPlatformCacheDataFromPersistence()
        {
            TestCacheOperation(k => _cache.ContainsKey(k));
        }

        /// <summary>
        /// Tests containsKeys operation.
        /// </summary>
        [Test]
        public void TestContainsKeysRestoresPlatformCacheDataFromPersistence()
        {
            TestCacheOperation(k => _cache.ContainsKeys(new[] { k }));
        }

        /// <summary>
        /// Tests put operation.
        /// </summary>
        [Test]
        public void TestPutUpdatesPlatformCache()
        {
            TestCacheOperation(k => _cache.Put(k, -k), k => -k);
        }

        /// <summary>
        /// Tests getAndPut operation.
        /// </summary>
        [Test]
        public void TestGetAndPutUpdatesPlatformCache()
        {
            TestCacheOperation(k => _cache.GetAndPut(k, -k), k => -k);
        }

        /// <summary>
        /// Tests that local partition scan optimization is disabled when persistence is enabled.
        /// See <see cref="CacheImpl{TK,TV}.ScanPlatformCache"/>.
        /// </summary>
        [Test]
        public void TestScanQueryLocalPartitionScanOptimizationDisabledWithPersistence()
        {
            var res = _cache.Query(new ScanQuery<int, int>()).GetAll();
            Assert.AreEqual(Count, res.Count);

            var resLocal = _cache.Query(new ScanQuery<int, int> { Local = true }).GetAll();
            Assert.AreEqual(Count, resLocal.Count);

            var resPartition = _cache.Query(new ScanQuery<int, int> { Partition = 99 }).GetAll();
            Assert.AreEqual(1, resPartition.Count);

            var resLocalPartition = _cache.Query(new ScanQuery<int, int> { Local = true, Partition = 99 }).GetAll();
            Assert.AreEqual(1, resLocalPartition.Count);
        }

        /// <summary>
        /// Tests that scan query with filter does not need platform cache data to return correct results.
        /// </summary>
        [Test]
        public void TestScanQueryWithFilterUsesPersistentData()
        {
            var res = _cache.Query(new ScanQuery<int, int> { Filter = new EvenValueFilter() }).GetAll();
            Assert.AreEqual(Count / 2, res.Count);
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

        /// <summary>
        /// Tests that cache operation causes platform cache to be populated for the key.
        /// </summary>
        private void TestCacheOperation(Action<int> operation, Func<int, int> expectedFunc = null)
        {
            var k = _key++;
            operation(k);

            var expected = expectedFunc == null ? k : expectedFunc(k);
            Assert.AreEqual(expected, _cache.LocalPeek(k, CachePeekMode.Platform));
        }

        private class EvenValueFilter : ICacheEntryFilter<int, int>
        {
            public bool Invoke(ICacheEntry<int, int> entry)
            {
                return entry.Value % 2 == 0;
            }
        }
    }
}

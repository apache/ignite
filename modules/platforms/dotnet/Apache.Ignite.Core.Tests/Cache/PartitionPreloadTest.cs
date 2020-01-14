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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests partition preload API.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class PartitionPreloadTest
    {
        /** Temp dir for PDS. */
        private static readonly string TempDir = PathUtils.GetTempDirectoryName();

        /** */
        private const string MemoryRegionName = "mem-region";
        
        /** */
        private const string PersistenceRegionName = "pds-region";

        /** */
        private const int GridCount = 3;

        /** */
        private const string MemCacheName = "mem-cache";

        /** */
        private const string PersistenceCacheName = "pds-cache";

        /** */
        private const int EntriesCount = 1000;
        
        /** */
        private static readonly TimeSpan CheckpointFrequency = TimeSpan.FromSeconds(1);
        
        /** */
        private const string ExpectedErrorMessage = "Operation only applicable to caches with enabled persistence";

        /// <summary>
        /// Tests that preloading partition on client locally returns <code>false</code>.
        /// </summary>
        [Test]
        public void TestLocalPreloadPartitionClient()
        {          
            var cache = GetClient.GetCache<int, int>(MemCacheName);
            
            Assert.IsFalse(cache.LocalPreloadPartition(0));
        }
        
        /// <summary>
        /// Preload partitions synchronously.
        /// </summary>
        [Test]
        public void TestPreloadPartition()
        {
            PerformPreloadTest(GetGrid(0), GetClient, PreloadMode.Sync);
        }

        /// <summary>
        /// Tests that incorrect usage of API throws exceptions.
        /// </summary>
        [Test]
        public void TestPreloadPartitionInMemoryFail()
        {
            var cache = GetClient.GetCache<int, int>(MemCacheName);

            var ex = Assert.Throws<CacheException>(() => cache.PreloadPartition(0));
            
            Assert.True(ex.Message.Contains(ExpectedErrorMessage), ex.Message);

            var task = cache.PreloadPartitionAsync(0);

            ex = Assert.Throws<CacheException>(() => task.WaitResult());
            
            Assert.True(ex.Message.Contains(ExpectedErrorMessage), ex.Message);
        }
        
        /// <summary>
        /// Preload partitions asynchronously.
        /// </summary>
        [Test]
        public void TestPreloadPartitionAsync()
        {
            PerformPreloadTest(GetGrid(0), GetClient, PreloadMode.Async);
        }
        
        /// <summary>
        /// Preload partitions locally.
        /// </summary>
        [Test]
        public void TestLocalPreloadPartition()
        {
            PerformPreloadTest(GetGrid(0), GetClient, PreloadMode.Local);
        }

        /// <summary>
        /// Test set up.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
        }
        
        /// <summary>
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
            
            Directory.Delete(TempDir, true);
        }
        
        /// <summary>
        ///  Starts test grids.
        /// </summary>
        private static void StartGrids()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    WalMode = WalMode.Fsync,
                    WalSegmentSize = 16 * 1 << 20, // 16 MB.
                    PageSize = 1 << 10, // 1 KB.
                    MetricsEnabled = true,
                    CheckpointFrequency = CheckpointFrequency,
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = PersistenceRegionName,
                        PersistenceEnabled = true,
                        MetricsEnabled = true,
                        MaxSize = 50 * 1 << 20 // 50 MB.
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = MemoryRegionName,
                            MetricsEnabled = true,
                            PersistenceEnabled = false,
                            MaxSize = 10 * 1 << 20 // 10 MB.
                        }
                    },
                },
                WorkDirectory = TempDir,
                CacheConfiguration = new List<CacheConfiguration>
                {
                    new CacheConfiguration(MemCacheName)
                    {
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        AffinityFunction = new RendezvousAffinityFunction
                        {
                            ExcludeNeighbors = false,
                            Partitions = 32
                        },
                        Backups = 1,
                        DataRegionName = MemoryRegionName,
                    },
                    new CacheConfiguration(PersistenceCacheName) 
                    {
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        AffinityFunction = new RendezvousAffinityFunction
                        {
                            ExcludeNeighbors = false,
                            Partitions = 32
                        },
                        Backups = 1,
                        DataRegionName = PersistenceRegionName
                    }
                }
            };

            for (int i = 0; i < GridCount; i++)
            {
                cfg.IgniteInstanceName = "grid-" + i;
                cfg.ConsistentId = "grid-" + i;
                
                Ignition.Start(cfg);
            }

            cfg.IgniteInstanceName = "client";
            cfg.ClientMode = true;

            var client = Ignition.Start(cfg);
            
            client.GetCluster().SetActive(true);
        }
        
        private IIgnite GetGrid(int i)
        {
            return Ignition.GetIgnite("grid-" + i);
        }

        private IIgnite GetClient
        {
            get { return Ignition.GetIgnite("client"); }
        }

        /// <summary>
        /// Perform test operations.
        /// </summary>
        /// <param name="testNode">Owner of preloaded partition.</param>
        /// <param name="execNode">Node on which operation will be executed.</param>
        /// <param name="preloadMode">Preload mode <see cref="PreloadMode"/></param>
        private void PerformPreloadTest(IIgnite testNode, IIgnite execNode, PreloadMode preloadMode)
        {
            int key = TestUtils.GetPrimaryKey(testNode, PersistenceCacheName);
            var affinity = execNode.GetAffinity(PersistenceCacheName);
            int preloadPart = affinity.GetPartition(key);
            
            using (var streamer = execNode.GetDataStreamer<int, int>(PersistenceCacheName))
            {
                int cnt = EntriesCount;
                int k = 0;

                while (cnt > 0)
                {
                    if (affinity.GetPartition(k) == preloadPart)
                    {
                        streamer.AddData(k, k);
                        
                        cnt--;
                    }
                    
                    k++;
                }  
            }

            // Wait for checkpoint (wait for doubled CheckpointFrequency interval).
            Thread.Sleep(CheckpointFrequency.Add(CheckpointFrequency));
            
            switch (preloadMode)
            {
                case PreloadMode.Sync:
                    execNode.GetCache<int, int>(PersistenceCacheName).PreloadPartition(preloadPart);
                    
                    break;
                case PreloadMode.Async:
                    var task = execNode.GetCache<int, int>(PersistenceCacheName).PreloadPartitionAsync(preloadPart);
            
                    task.WaitResult();
                    
                    break;
                case PreloadMode.Local:
                    // In local mode we should load partition from testNode.
                    bool res = testNode.GetCache<int, int>(PersistenceCacheName).LocalPreloadPartition(preloadPart);
                    
                    Assert.IsTrue(res);
                    
                    break;
            }
            
            
            long pagesRead = testNode.GetDataRegionMetrics(PersistenceRegionName).PagesRead;

            var entries = testNode.GetCache<int, int>(PersistenceCacheName).GetLocalEntries().ToList();

            Assert.AreEqual(entries.Count, EntriesCount);
            Assert.AreEqual(pagesRead, testNode.GetDataRegionMetrics(PersistenceRegionName).PagesRead);  
        }

        /** */
        enum PreloadMode
        {
            /** Preload synchronously. */
            Sync,
            /** Preload asynchronously. */
            Async,
            /** Preload locally. */
            Local
        }
    }
}

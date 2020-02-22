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
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests cache metrics propagation.
    /// </summary>
    public class CacheMetricsTest
    {
        /** */
        private const string SecondGridName = "grid";

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = SecondGridName
            });
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the local metrics.
        /// </summary>
        [Test]
        public void TestLocalMetrics()
        {
            var metrics = GetLocalRemoteMetrics("localMetrics", c => c.GetLocalMetrics());

            var localMetrics = metrics.Item1;
            var remoteMetrics = metrics.Item2;

            Assert.AreEqual(1, localMetrics.Size);
            Assert.AreEqual(1, localMetrics.CacheSize);
            Assert.AreEqual(1, localMetrics.CacheGets);
            Assert.AreEqual(1, localMetrics.CachePuts);

            Assert.AreEqual(0, remoteMetrics.Size);
            Assert.AreEqual(0, remoteMetrics.CacheSize);
            Assert.AreEqual(0, remoteMetrics.CacheGets);
            Assert.AreEqual(0, remoteMetrics.CachePuts);
        }

        /// <summary>
        /// Tests the global metrics.
        /// </summary>
        [Test]
        public void TestGlobalMetrics()
        {
            var metrics = GetLocalRemoteMetrics("globalMetrics", c => c.GetMetrics());

            var localMetrics = metrics.Item1;
            var remoteMetrics = metrics.Item2;

            Assert.AreEqual(1, localMetrics.Size);
            Assert.AreEqual(1, localMetrics.CacheSize);
            Assert.AreEqual(1, localMetrics.CacheGets);
            Assert.AreEqual(1, localMetrics.CachePuts);

            Assert.AreEqual(1, remoteMetrics.Size);
            Assert.AreEqual(1, remoteMetrics.CacheSize);
            Assert.AreEqual(1, remoteMetrics.CacheGets);
            Assert.AreEqual(1, remoteMetrics.CachePuts);
        }

        /// <summary>
        /// Tests the cache metrics enable/disable 
        /// </summary>
        [Test]
        public void TestCacheEnableStatistics()
        {
            TestEnableStatistics("cacheEnableStatistics", (cache, b) => cache.EnableStatistics(b));
        }

        /// <summary>
        /// Tests the cache metrics enable/disable for cluster group
        /// </summary>
        [Test]
        public void TestClusterGroupEnableStatistics()
        {
            var cacheName = "clusterEnableStatistics";
            TestEnableStatistics(
                cacheName,
                (cache, b) => cache.Ignite.GetCluster().EnableStatistics(new[] {cacheName}, b));
        }

        /// <summary>
        /// Tests that empty cache names can be passed to <see cref="IClusterGroup.EnableStatistics"/>
        /// </summary>
        [Test]
        public void  TestClusterEnableStatisticsAllowsEmptyCacheNames()
        {
            var ignite = Ignition.GetIgnite();
            ignite.CreateCache<int, int>(new CacheConfiguration("clusterEnableStatisticsAllowsEmptyCacheNames"));
            var cluster = ignite.GetCluster();

            Assert.DoesNotThrow(() => cluster.EnableStatistics(Enumerable.Empty<string>(), true));
        }

        /// <summary>
        /// Tests exception when invalid cache name is passed to <see cref="IClusterGroup.EnableStatistics"/>
        /// </summary>
        [Test]
        public void TestClusterEnableStatisticsThrowsOnInvalidCacheName()
        {
            var ignite = Ignition.GetIgnite();
            ignite.CreateCache<int, int>(new CacheConfiguration("clusterEnableStatisticsThrowsOnInvalidCacheName"));
            var cluster = ignite.GetCluster();

            var invalidCacheName = "clusterEnableStatsInvalidName";

            var msg = Assert.Throws<IgniteException>(
                () => cluster.EnableStatistics(new[] {invalidCacheName}, true)).Message;
            Assert.IsTrue(msg.Contains(invalidCacheName));
            Assert.IsTrue(msg.Contains("One or more cache descriptors not found"));
        }

        /// <summary>
        /// Tests the cluster group metrics.
        /// </summary>
        [Test]
        public void TestClusterGroupMetrics()
        {
            var cluster = Ignition.GetIgnite().GetCluster();

            // Get metrics in reverse way, so that first item is for second node and vice versa.
            var metrics = GetLocalRemoteMetrics("clusterMetrics", c => c.GetMetrics(cluster.ForRemotes()), 
                c => c.GetMetrics(cluster.ForLocal()));

            var localMetrics = metrics.Item2;
            var remoteMetrics = metrics.Item1;

            Assert.AreEqual(1, localMetrics.Size);
            Assert.AreEqual(1, localMetrics.CacheSize);
            Assert.AreEqual(1, localMetrics.CacheGets);
            Assert.AreEqual(1, localMetrics.CachePuts);

            Assert.AreEqual(0, remoteMetrics.Size);
            Assert.AreEqual(0, remoteMetrics.CacheSize);
            Assert.AreEqual(0, remoteMetrics.CacheGets);
            Assert.AreEqual(0, remoteMetrics.CachePuts);
        }

        /// <summary>
        /// Tests the metrics propagation.
        /// </summary>
        [Test]
        public void TestMetricsPropagation()
        {
            var ignite = Ignition.GetIgnite();

            using (var inStream = IgniteManager.Memory.Allocate().GetStream())
            {
                var result = ignite.GetCompute().ExecuteJavaTask<bool>(
                    "org.apache.ignite.platform.PlatformCacheWriteMetricsTask", inStream.MemoryPointer);

                Assert.IsTrue(result);

                inStream.SynchronizeInput();

                var reader = ((Ignite) ignite).Marshaller.StartUnmarshal(inStream);

                ICacheMetrics metrics = new CacheMetricsImpl(reader);

                Assert.AreEqual(1, metrics.CacheHits);
                Assert.AreEqual(2, metrics.CacheHitPercentage);
                Assert.AreEqual(3, metrics.CacheMisses);
                Assert.AreEqual(4, metrics.CacheMissPercentage);
                Assert.AreEqual(5, metrics.CacheGets);
                Assert.AreEqual(6, metrics.CachePuts);
                Assert.AreEqual(7, metrics.CacheRemovals);
                Assert.AreEqual(8, metrics.CacheEvictions);
                Assert.AreEqual(9, metrics.AverageGetTime);
                Assert.AreEqual(10, metrics.AveragePutTime);
                Assert.AreEqual(11, metrics.AverageRemoveTime);
                Assert.AreEqual(12, metrics.AverageTxCommitTime);
                Assert.AreEqual(13, metrics.AverageTxRollbackTime);
                Assert.AreEqual(14, metrics.CacheTxCommits);
                Assert.AreEqual(15, metrics.CacheTxRollbacks);
                Assert.AreEqual("myCache", metrics.CacheName);
                Assert.AreEqual(16, metrics.OffHeapGets);
                Assert.AreEqual(17, metrics.OffHeapPuts);
                Assert.AreEqual(18, metrics.OffHeapRemovals);
                Assert.AreEqual(19, metrics.OffHeapEvictions);
                Assert.AreEqual(20, metrics.OffHeapHits);
                Assert.AreEqual(21, metrics.OffHeapHitPercentage);
                Assert.AreEqual(22, metrics.OffHeapMisses);
                Assert.AreEqual(23, metrics.OffHeapMissPercentage);
                Assert.AreEqual(24, metrics.OffHeapEntriesCount);
                Assert.AreEqual(25, metrics.OffHeapPrimaryEntriesCount);
                Assert.AreEqual(26, metrics.OffHeapBackupEntriesCount);
                Assert.AreEqual(27, metrics.OffHeapAllocatedSize);
                Assert.AreEqual(29, metrics.Size);
                Assert.AreEqual(30, metrics.KeySize);
                Assert.AreEqual(true, metrics.IsEmpty);
                Assert.AreEqual(31, metrics.DhtEvictQueueCurrentSize);
                Assert.AreEqual(32, metrics.TxThreadMapSize);
                Assert.AreEqual(33, metrics.TxXidMapSize);
                Assert.AreEqual(34, metrics.TxCommitQueueSize);
                Assert.AreEqual(35, metrics.TxPrepareQueueSize);
                Assert.AreEqual(36, metrics.TxStartVersionCountsSize);
                Assert.AreEqual(37, metrics.TxCommittedVersionsSize);
                Assert.AreEqual(38, metrics.TxRolledbackVersionsSize);
                Assert.AreEqual(39, metrics.TxDhtThreadMapSize);
                Assert.AreEqual(40, metrics.TxDhtXidMapSize);
                Assert.AreEqual(41, metrics.TxDhtCommitQueueSize);
                Assert.AreEqual(42, metrics.TxDhtPrepareQueueSize);
                Assert.AreEqual(43, metrics.TxDhtStartVersionCountsSize);
                Assert.AreEqual(44, metrics.TxDhtCommittedVersionsSize);
                Assert.AreEqual(45, metrics.TxDhtRolledbackVersionsSize);
                Assert.AreEqual(true, metrics.IsWriteBehindEnabled);
                Assert.AreEqual(46, metrics.WriteBehindFlushSize);
                Assert.AreEqual(47, metrics.WriteBehindFlushThreadCount);
                Assert.AreEqual(48, metrics.WriteBehindFlushFrequency);
                Assert.AreEqual(49, metrics.WriteBehindStoreBatchSize);
                Assert.AreEqual(50, metrics.WriteBehindTotalCriticalOverflowCount);
                Assert.AreEqual(51, metrics.WriteBehindCriticalOverflowCount);
                Assert.AreEqual(52, metrics.WriteBehindErrorRetryCount);
                Assert.AreEqual(53, metrics.WriteBehindBufferSize);
                Assert.AreEqual(54, metrics.TotalPartitionsCount);
                Assert.AreEqual(55, metrics.RebalancingPartitionsCount);
                Assert.AreEqual(56, metrics.KeysToRebalanceLeft);
                Assert.AreEqual(57, metrics.RebalancingKeysRate);
                Assert.AreEqual(58, metrics.RebalancingBytesRate);
                Assert.AreEqual(59, metrics.HeapEntriesCount);
                Assert.AreEqual(62, metrics.EstimatedRebalancingFinishTime);
                Assert.AreEqual(63, metrics.RebalancingStartTime);
                Assert.AreEqual(65, metrics.CacheSize);
                Assert.AreEqual("foo", metrics.KeyType);
                Assert.AreEqual("bar", metrics.ValueType);
                Assert.AreEqual(true, metrics.IsStoreByValue);
                Assert.AreEqual(true, metrics.IsStatisticsEnabled);
                Assert.AreEqual(true, metrics.IsManagementEnabled);
                Assert.AreEqual(true, metrics.IsReadThrough);
                Assert.AreEqual(true, metrics.IsWriteThrough);
                Assert.AreEqual(true, metrics.IsValidForReading);
                Assert.AreEqual(true, metrics.IsValidForWriting);
                Assert.AreEqual(68, metrics.EntryProcessorPuts);
                Assert.AreEqual(69, metrics.EntryProcessorReadOnlyInvocations);
                Assert.AreEqual(70, metrics.EntryProcessorInvocations);
                Assert.AreEqual(71, metrics.EntryProcessorHits);
                Assert.AreEqual(72, metrics.EntryProcessorHitPercentage);
                Assert.AreEqual(73, metrics.EntryProcessorMissPercentage);
                Assert.AreEqual(74, metrics.EntryProcessorMisses);
                Assert.AreEqual(75, metrics.EntryProcessorAverageInvocationTime);
                Assert.AreEqual(76, metrics.EntryProcessorMinInvocationTime);
                Assert.AreEqual(77, metrics.EntryProcessorMaxInvocationTime);
                Assert.AreEqual(78, metrics.EntryProcessorRemovals);
            }
        }

        /// <summary>
        /// Creates a cache, performs put-get, returns metrics from both nodes.
        /// </summary>
        private static Tuple<ICacheMetrics, ICacheMetrics> GetLocalRemoteMetrics(string cacheName,
                    Func<ICache<int, int>, ICacheMetrics> func, Func<ICache<int, int>, ICacheMetrics> func2 = null)
        {
            func2 = func2 ?? func;

            var localIgnite = Ignition.GetIgnite();
            var localCache = localIgnite.CreateCache<int, int>(new CacheConfiguration(cacheName)
            {
                EnableStatistics = true
            });

            var remoteCache = Ignition.GetIgnite(SecondGridName).GetCache<int, int>(cacheName);

            Assert.IsTrue(localCache.GetConfiguration().EnableStatistics);
            Assert.IsTrue(remoteCache.GetConfiguration().EnableStatistics);

            var localKey = TestUtils.GetPrimaryKey(localIgnite, cacheName);

            localCache.Put(localKey, 1);
            localCache.Get(localKey);
            // Wait for metrics to propagate.
            Thread.Sleep(IgniteConfiguration.DefaultMetricsUpdateFrequency);

            var localMetrics = func(localCache);
            Assert.IsTrue(localMetrics.IsStatisticsEnabled);
            Assert.AreEqual(cacheName, localMetrics.CacheName);

            var remoteMetrics = func2(remoteCache);
            Assert.IsTrue(remoteMetrics.IsStatisticsEnabled);
            Assert.AreEqual(cacheName, remoteMetrics.CacheName);

            return Tuple.Create(localMetrics, remoteMetrics);
        }

        /// <summary>
        /// Creates a cache, enables/disables statistics, validates
        /// </summary>
        private static void TestEnableStatistics(string cacheName, Action<ICache<int, int>, bool> enableStatistics)
        {
            var ignite = Ignition.GetIgnite();
            var cache = ignite.CreateCache<int, int>(new CacheConfiguration(cacheName)
            {
                EnableStatistics = false
            });

            var key = 1;

            enableStatistics(cache, true);
            Thread.Sleep(IgniteConfiguration.DefaultMetricsUpdateFrequency);
            cache.Put(key, 1);
            cache.Get(key);
            Thread.Sleep(IgniteConfiguration.DefaultMetricsUpdateFrequency);
            var metrics = cache.GetMetrics();
            Assert.AreEqual(1, metrics.Size);
            Assert.AreEqual(1, metrics.CacheSize);
            Assert.AreEqual(1, metrics.CacheGets);
            Assert.AreEqual(1, metrics.CachePuts);

            enableStatistics(cache, false);
            Thread.Sleep(IgniteConfiguration.DefaultMetricsUpdateFrequency);
            metrics = cache.GetMetrics();
            Assert.AreEqual(0, metrics.Size);
            Assert.AreEqual(0, metrics.CacheSize);
            Assert.AreEqual(0, metrics.CacheGets);
            Assert.AreEqual(0, metrics.CachePuts);
        }
    }
}

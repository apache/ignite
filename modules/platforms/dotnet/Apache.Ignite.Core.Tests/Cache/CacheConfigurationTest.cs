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
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests cache configuration propagation.
    /// </summary>
    public class CacheConfigurationTest
    {
        /** */
        private IIgnite _ignite;

        /** */
        private const string CacheName = "cacheName";

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new List<CacheConfiguration>
                {
                    new CacheConfiguration(),
                     GetCustomCacheConfiguration()
                },
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            _ignite = Ignition.Start(cfg);
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void TestDefaultConfiguration()
        {
            AssertConfigIsDefault(new CacheConfiguration());

            AssertConfigIsDefault(_ignite.GetCache<int, int>(null).GetConfiguration());
        }

        [Test]
        public void TestCustomConfiguration()
        {
            AssertConfigsAreEqual(GetCustomCacheConfiguration(),
                _ignite.GetCache<int, int>(CacheName).GetConfiguration());
        }

        [Test]
        public void TestCreateFromConfiguration()
        {
            var cacheName = Guid.NewGuid().ToString();
            var cfg = GetCustomCacheConfiguration(cacheName);

            var cache = _ignite.CreateCache<int, int>(cfg);
            AssertConfigsAreEqual(cfg, cache.GetConfiguration());
        }

        [Test]
        public void TestGetOrCreateFromConfiguration()
        {
            var cacheName = Guid.NewGuid().ToString();
            var cfg = GetCustomCacheConfiguration(cacheName);

            var cache = _ignite.GetOrCreateCache<int, int>(cfg);
            AssertConfigsAreEqual(cfg, cache.GetConfiguration());

            var cache2 = _ignite.GetOrCreateCache<int, int>(cfg);
            AssertConfigsAreEqual(cfg, cache2.GetConfiguration());
        }

        /// <summary>
        /// Asserts the configuration is default.
        /// </summary>
        private static void AssertConfigIsDefault(CacheConfiguration cfg)
        {
            Assert.AreEqual(CacheConfiguration.DefaultBackups, cfg.Backups);
            Assert.AreEqual(CacheConfiguration.DefaultAtomicityMode, cfg.AtomicityMode);
            Assert.AreEqual(CacheConfiguration.DefaultCacheMode, cfg.CacheMode);
            Assert.AreEqual(CacheConfiguration.DefaultCopyOnRead, cfg.CopyOnRead);
            Assert.AreEqual(CacheConfiguration.DefaultStartSize, cfg.StartSize);
            Assert.AreEqual(CacheConfiguration.DefaultEagerTtl, cfg.EagerTtl);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronizedKeyBufferSize, cfg.EvictSynchronizedKeyBufferSize);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronized, cfg.EvictSynchronized);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronizedConcurrencyLevel, cfg.EvictSynchronizedConcurrencyLevel);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronizedTimeout, cfg.EvictSynchronizedTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultInvalidate, cfg.Invalidate);
            Assert.AreEqual(CacheConfiguration.DefaultKeepPortableInStore, cfg.KeepBinaryInStore);
            Assert.AreEqual(CacheConfiguration.DefaultLoadPreviousValue, cfg.LoadPreviousValue);
            Assert.AreEqual(CacheConfiguration.DefaultLockTimeout, cfg.LockTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultLongQueryWarningTimeout, cfg.LongQueryWarningTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultMaxConcurrentAsyncOperations, cfg.MaxConcurrentAsyncOperations);
            Assert.AreEqual(CacheConfiguration.DefaultMaxEvictionOverflowRatio, cfg.MaxEvictionOverflowRatio);
            Assert.AreEqual(CacheConfiguration.DefaultMemoryMode, cfg.MemoryMode);
            Assert.AreEqual(CacheConfiguration.DefaultOffHeapMaxMemory, cfg.OffHeapMaxMemory);
            Assert.AreEqual(CacheConfiguration.DefaultReadFromBackup, cfg.ReadFromBackup);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceBatchSize, cfg.RebalanceBatchSize);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceMode, cfg.RebalanceMode);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceThreadPoolSize, cfg.RebalanceThreadPoolSize);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceThrottle, cfg.RebalanceThrottle);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceTimeout, cfg.RebalanceTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultSqlOnheapRowCacheSize, cfg.SqlOnheapRowCacheSize);
            Assert.AreEqual(CacheConfiguration.DefaultStartSize, cfg.StartSize);
            Assert.AreEqual(CacheConfiguration.DefaultStartSize, cfg.StartSize);
            Assert.AreEqual(CacheConfiguration.DefaultEnableSwap, cfg.EnableSwap);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindBatchSize, cfg.WriteBehindBatchSize);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindEnabled, cfg.WriteBehindEnabled);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindFlushFrequency, cfg.WriteBehindFlushFrequency);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindFlushSize, cfg.WriteBehindFlushSize);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(CacheConfiguration x, CacheConfiguration y)
        {
            Assert.AreEqual(x.Backups, y.Backups);
            Assert.AreEqual(x.AtomicityMode, y.AtomicityMode);
            Assert.AreEqual(x.CacheMode, y.CacheMode);
            Assert.AreEqual(x.CopyOnRead, y.CopyOnRead);
            Assert.AreEqual(x.StartSize, y.StartSize);
            Assert.AreEqual(x.EagerTtl, y.EagerTtl);
            Assert.AreEqual(x.EvictSynchronizedKeyBufferSize, y.EvictSynchronizedKeyBufferSize);
            Assert.AreEqual(x.EvictSynchronized, y.EvictSynchronized);
            Assert.AreEqual(x.EvictSynchronizedConcurrencyLevel, y.EvictSynchronizedConcurrencyLevel);
            Assert.AreEqual(x.EvictSynchronizedTimeout, y.EvictSynchronizedTimeout);
            Assert.AreEqual(x.Invalidate, y.Invalidate);
            Assert.AreEqual(x.KeepBinaryInStore, y.KeepBinaryInStore);
            Assert.AreEqual(x.LoadPreviousValue, y.LoadPreviousValue);
            Assert.AreEqual(x.LockTimeout, y.LockTimeout);
            Assert.AreEqual(x.LongQueryWarningTimeout, y.LongQueryWarningTimeout);
            Assert.AreEqual(x.MaxConcurrentAsyncOperations, y.MaxConcurrentAsyncOperations);
            Assert.AreEqual(x.MaxEvictionOverflowRatio, y.MaxEvictionOverflowRatio);
            Assert.AreEqual(x.MemoryMode, y.MemoryMode);
            Assert.AreEqual(x.OffHeapMaxMemory, y.OffHeapMaxMemory);
            Assert.AreEqual(x.ReadFromBackup, y.ReadFromBackup);
            Assert.AreEqual(x.RebalanceBatchSize, y.RebalanceBatchSize);
            Assert.AreEqual(x.RebalanceMode, y.RebalanceMode);
            Assert.AreEqual(x.RebalanceThreadPoolSize, y.RebalanceThreadPoolSize);
            Assert.AreEqual(x.RebalanceThrottle, y.RebalanceThrottle);
            Assert.AreEqual(x.RebalanceTimeout, y.RebalanceTimeout);
            Assert.AreEqual(x.SqlOnheapRowCacheSize, y.SqlOnheapRowCacheSize);
            Assert.AreEqual(x.StartSize, y.StartSize);
            Assert.AreEqual(x.StartSize, y.StartSize);
            Assert.AreEqual(x.EnableSwap, y.EnableSwap);
            Assert.AreEqual(x.WriteBehindBatchSize, y.WriteBehindBatchSize);
            Assert.AreEqual(x.WriteBehindEnabled, y.WriteBehindEnabled);
            Assert.AreEqual(x.WriteBehindFlushFrequency, y.WriteBehindFlushFrequency);
            Assert.AreEqual(x.WriteBehindFlushSize, y.WriteBehindFlushSize);
        }

        /// <summary>
        /// Gets the custom cache configuration.
        /// </summary>
        private static CacheConfiguration GetCustomCacheConfiguration(string name = null)
        {
            return new CacheConfiguration
            {
                Name = name ?? CacheName,
                OffHeapMaxMemory = 1,
                StartSize = 2,
                MaxConcurrentAsyncOperations = 3,
                WriteBehindFlushThreadCount = 4,
                LongQueryWarningTimeout = TimeSpan.FromSeconds(5),
                LoadPreviousValue = true,
                EvictSynchronizedKeyBufferSize = 6,
                CopyOnRead = true,
                WriteBehindFlushFrequency = TimeSpan.FromSeconds(6),
                WriteBehindFlushSize = 7,
                EvictSynchronized = true,
                AtomicWriteOrderMode = CacheAtomicWriteOrderMode.Primary,
                AtomicityMode = CacheAtomicityMode.Atomic,
                Backups = 8,
                CacheMode = CacheMode.Partitioned,
                EagerTtl = true,
                EnableSwap = true,
                EvictSynchronizedConcurrencyLevel = 9,
                EvictSynchronizedTimeout = TimeSpan.FromSeconds(10),
                Invalidate = true,
                KeepBinaryInStore = true,
                LockTimeout = TimeSpan.FromSeconds(11),
                MaxEvictionOverflowRatio = 0.5f,
                MemoryMode = CacheMemoryMode.OffheapValues,
                ReadFromBackup = true,
                RebalanceBatchSize = 12,
                RebalanceDelay = TimeSpan.FromSeconds(13),
                RebalanceMode = CacheRebalanceMode.Async,
                RebalanceThreadPoolSize = 14,
                RebalanceThrottle = TimeSpan.FromSeconds(15),
                RebalanceTimeout = TimeSpan.FromSeconds(16),
                SqlEscapeAll = true,
                SqlOnheapRowCacheSize = 17,
                WriteBehindBatchSize = 18,
                WriteBehindEnabled = false,
                WriteSynchronizationMode = CacheWriteSynchronizationMode.PrimarySync
            };
        }
    }
}

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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests cache configuration propagation.
    /// </summary>
    public class CacheConfigurationTest
    {
        private IIgnite _ignite;

        private const string CacheName = "cacheName";

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new List<CacheConfiguration>
                {
                    new CacheConfiguration(),
                    new CacheConfiguration(CacheName),
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

        /// <summary>
        /// Asserts the configuration is default.
        /// </summary>
        private static void AssertConfigIsDefault(CacheConfiguration cfg)
        {
            Assert.AreEqual(CacheConfiguration.DefaultBackups, cfg.Backups);
            Assert.AreEqual(CacheConfiguration.DefaultCacheAtomicityMode, cfg.AtomicityMode);
            Assert.AreEqual(CacheConfiguration.DefaultCacheMode, cfg.CacheMode);
            Assert.AreEqual(CacheConfiguration.DefaultCopyOnRead, cfg.CopyOnRead);
            Assert.AreEqual(CacheConfiguration.DefaultCacheSize, cfg.StartSize);
            Assert.AreEqual(CacheConfiguration.DefaultEagerTtl, cfg.EagerTtl);
            Assert.AreEqual(CacheConfiguration.DefaultEvictKeyBufferSize, cfg.EvictSynchronizedKeyBufferSize);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronized, cfg.EvictSynchronized);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronizedConcurrencyLevel, cfg.EvictSynchronizedConcurrencyLevel);
            Assert.AreEqual(CacheConfiguration.DefaultEvictSynchronizedTimeout, cfg.EvictSynchronizedTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultInvalidate, cfg.Invalidate);
            Assert.AreEqual(CacheConfiguration.DefaultKeepPortableInStore, cfg.KeepPortableInStore);
            Assert.AreEqual(CacheConfiguration.DefaultLoadPreviousValue, cfg.LoadPreviousValue);
            Assert.AreEqual(CacheConfiguration.DefaultLockTimeout, cfg.LockTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultLongQueryWarningTimeout, cfg.LongQueryWarningTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultMaxConcurrentAsyncOperations, cfg.MaxConcurrentAsyncOperations);
            Assert.AreEqual(CacheConfiguration.DefaultMaxEvictionOverflowRatio, cfg.MaxEvictionOverflowRatio);
            Assert.AreEqual(CacheConfiguration.DefaultMemoryMode, cfg.MemoryMode);
            Assert.AreEqual(CacheConfiguration.DefaultOffheapMemory, cfg.OffHeapMaxMemory);
            Assert.AreEqual(CacheConfiguration.DefaultReadFromBackup, cfg.ReadFromBackup);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceBatchSize, cfg.RebalanceBatchSize);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceMode, cfg.RebalanceMode);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceThreadPoolSize, cfg.RebalanceThreadPoolSize);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceThrottle, cfg.RebalanceThrottle);
            Assert.AreEqual(CacheConfiguration.DefaultRebalanceTimeout, cfg.RebalanceTimeout);
            Assert.AreEqual(CacheConfiguration.DefaultSqlOnheapRowCacheSize, cfg.SqlOnheapRowCacheSize);
            Assert.AreEqual(CacheConfiguration.DefaultStartSize, cfg.StartSize);
            Assert.AreEqual(CacheConfiguration.DefaultStartSize, cfg.StartSize);
            Assert.AreEqual(CacheConfiguration.DefaultSwapEnabled, cfg.EnableSwap);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindBatchSize, cfg.WriteBehindBatchSize);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindEnabled, cfg.WriteBehindEnabled);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindFlushFrequency, cfg.WriteBehindFlushFrequency);
            Assert.AreEqual(CacheConfiguration.DefaultWriteBehindFlushSize, cfg.WriteBehindFlushSize);
        }
    }
}

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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cache.Affinity;
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

        /** */
        private const string CacheName2 = "cacheName2";

        /** */
        private static int _factoryProp;


        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new List<CacheConfiguration>
                {
                    new CacheConfiguration(),
                    GetCustomCacheConfiguration(),
                    GetCustomCacheConfiguration2()
                },
                GridName = CacheName,
                BinaryConfiguration = new BinaryConfiguration(typeof (Entity))
            };

            _ignite = Ignition.Start(cfg);
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
        /// Tests the default configuration.
        /// </summary>
        [Test]
        public void TestDefaultConfiguration()
        {
            AssertConfigIsDefault(new CacheConfiguration());

            AssertConfigIsDefault(_ignite.GetCache<int, int>(null).GetConfiguration());

            AssertConfigIsDefault(_ignite.GetConfiguration().CacheConfiguration.Single(c => c.Name == null));
        }

        /// <summary>
        /// Tests the custom configuration.
        /// </summary>
        [Test]
        public void TestCustomConfiguration()
        {
            AssertConfigsAreEqual(GetCustomCacheConfiguration(),
                _ignite.GetCache<int, int>(CacheName).GetConfiguration());

            AssertConfigsAreEqual(GetCustomCacheConfiguration(),
                _ignite.GetConfiguration().CacheConfiguration.Single(c => c.Name == CacheName));

            AssertConfigsAreEqual(GetCustomCacheConfiguration2(),
                _ignite.GetCache<int, int>(CacheName2).GetConfiguration());

            AssertConfigsAreEqual(GetCustomCacheConfiguration2(),
                _ignite.GetConfiguration().CacheConfiguration.Single(c => c.Name == CacheName2));
        }

        /// <summary>
        /// Tests the create from configuration.
        /// </summary>
        [Test]
        public void TestCreateFromConfiguration()
        {
            var cacheName = Guid.NewGuid().ToString();
            var cfg = GetCustomCacheConfiguration(cacheName);

            var cache = _ignite.CreateCache<int, Entity>(cfg);
            AssertConfigsAreEqual(cfg, cache.GetConfiguration());

            // Can't create existing cache
            Assert.Throws<IgniteException>(() => _ignite.CreateCache<int, int>(cfg));
            
            // Check put-get
            cache[1] = new Entity { Foo = 1 };
            Assert.AreEqual(1, cache[1].Foo);
        }

        /// <summary>
        /// Tests the get or create from configuration.
        /// </summary>
        [Test]
        public void TestGetOrCreateFromConfiguration()
        {
            var cacheName = Guid.NewGuid().ToString();
            var cfg = GetCustomCacheConfiguration(cacheName);

            var cache = _ignite.GetOrCreateCache<int, Entity>(cfg);
            AssertConfigsAreEqual(cfg, cache.GetConfiguration());

            var cache2 = _ignite.GetOrCreateCache<int, Entity>(cfg);
            AssertConfigsAreEqual(cfg, cache2.GetConfiguration());

            // Check put-get
            cache[1] = new Entity { Foo = 1 };
            Assert.AreEqual(1, cache[1].Foo);
        }

        /// <summary>
        /// Tests the cache store.
        /// </summary>
        [Test]
        public void TestCacheStore()
        {
            _factoryProp = 0;

            var factory = new CacheStoreFactoryTest {TestProperty = 15};

            var cache = _ignite.CreateCache<int, int>(new CacheConfiguration("cacheWithStore")
            {
                CacheStoreFactory = factory
            });

            Assert.AreEqual(factory.TestProperty, _factoryProp);

            var factory0 = cache.GetConfiguration().CacheStoreFactory as CacheStoreFactoryTest;

            Assert.IsNotNull(factory0);
            Assert.AreEqual(factory.TestProperty, factory0.TestProperty);
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
            Assert.AreEqual(CacheConfiguration.DefaultKeepVinaryInStore, cfg.KeepBinaryInStore);
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
            Assert.AreEqual(x.EnableStatistics, y.EnableStatistics);

            if (x.ExpiryPolicyFactory != null)
                Assert.AreEqual(x.ExpiryPolicyFactory.CreateInstance().GetType(),
                    y.ExpiryPolicyFactory.CreateInstance().GetType());
            else
                Assert.IsNull(y.ExpiryPolicyFactory);

            AssertConfigsAreEqual(x.QueryEntities, y.QueryEntities);
            AssertConfigsAreEqual(x.NearConfiguration, y.NearConfiguration);
            AssertConfigsAreEqual(x.EvictionPolicy, y.EvictionPolicy);
            AssertConfigsAreEqual(x.AffinityFunction, y.AffinityFunction);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(IAffinityFunction x, IAffinityFunction y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            var px = (AffinityFunctionBase) x;
            var py = (AffinityFunctionBase) y;

            Assert.AreEqual(px.GetType(), py.GetType());
            Assert.AreEqual(px.Partitions, py.Partitions);
            Assert.AreEqual(px.ExcludeNeighbors, py.ExcludeNeighbors);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(NearCacheConfiguration x, NearCacheConfiguration y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.NearStartSize, y.NearStartSize);

            AssertConfigsAreEqual(x.EvictionPolicy, y.EvictionPolicy);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(IEvictionPolicy x, IEvictionPolicy y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.GetType(), y.GetType());

            var px = (EvictionPolicyBase) x;
            var py = (EvictionPolicyBase) y;

            Assert.AreEqual(px.BatchSize, py.BatchSize);
            Assert.AreEqual(px.MaxSize, py.MaxSize);
            Assert.AreEqual(px.MaxMemorySize, py.MaxMemorySize);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(ICollection<QueryEntity> x, ICollection<QueryEntity> y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.Count, y.Count);

            for (var i = 0; i < x.Count; i++)
                AssertConfigsAreEqual(x.ElementAt(i), y.ElementAt(i));
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(QueryEntity x, QueryEntity y)
        {
            Assert.IsNotNull(x);
            Assert.IsNotNull(y);

            Assert.AreEqual(x.KeyTypeName, y.KeyTypeName);
            Assert.AreEqual(x.ValueTypeName, y.ValueTypeName);

            AssertConfigsAreEqual(x.Fields, y.Fields);
            AssertConfigsAreEqual(x.Aliases, y.Aliases);

            AssertConfigsAreEqual(x.Indexes, y.Indexes);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(ICollection<QueryIndex> x, ICollection<QueryIndex> y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.Count, y.Count);

            for (var i = 0; i < x.Count; i++)
                AssertConfigsAreEqual(x.ElementAt(i), y.ElementAt(i));
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(ICollection<QueryField> x, ICollection<QueryField> y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.Count, y.Count);

            for (var i = 0; i < x.Count; i++)
                AssertConfigsAreEqual(x.ElementAt(i), y.ElementAt(i));
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(ICollection<QueryAlias> x, ICollection<QueryAlias> y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.Count, y.Count);

            for (var i = 0; i < x.Count; i++)
                AssertConfigsAreEqual(x.ElementAt(i), y.ElementAt(i));
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(ICollection<QueryIndexField> x, ICollection<QueryIndexField> y)
        {
            if (x == null)
            {
                Assert.IsNull(y);
                return;
            }

            Assert.AreEqual(x.Count, y.Count);

            for (var i = 0; i < x.Count; i++)
                AssertConfigsAreEqual(x.ElementAt(i), y.ElementAt(i));
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(QueryIndex x, QueryIndex y)
        {
            Assert.IsNotNull(x);
            Assert.IsNotNull(y);

            Assert.AreEqual(x.Name, y.Name);
            Assert.AreEqual(x.IndexType, y.IndexType);

            AssertConfigsAreEqual(x.Fields, y.Fields);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(QueryField x, QueryField y)
        {
            Assert.IsNotNull(x);
            Assert.IsNotNull(y);

            Assert.AreEqual(x.Name, y.Name);
            Assert.AreEqual(x.FieldTypeName, y.FieldTypeName);
            Assert.AreEqual(x.IsKeyField, y.IsKeyField);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(QueryAlias x, QueryAlias y)
        {
            Assert.IsNotNull(x);
            Assert.IsNotNull(y);

            Assert.AreEqual(x.FullName, y.FullName);
            Assert.AreEqual(x.Alias, y.Alias);
        }

        /// <summary>
        /// Asserts that two configurations have the same properties.
        /// </summary>
        private static void AssertConfigsAreEqual(QueryIndexField x, QueryIndexField y)
        {
            Assert.IsNotNull(x);
            Assert.IsNotNull(y);

            Assert.AreEqual(x.Name, y.Name);
            Assert.AreEqual(x.IsDescending, y.IsDescending);
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
                MemoryMode = CacheMemoryMode.OnheapTiered,
                ReadFromBackup = true,
                RebalanceBatchSize = 12,
                RebalanceDelay = TimeSpan.FromSeconds(13),
                RebalanceMode = CacheRebalanceMode.Async,
                RebalanceThrottle = TimeSpan.FromSeconds(15),
                RebalanceTimeout = TimeSpan.FromSeconds(16),
                SqlEscapeAll = true,
                SqlOnheapRowCacheSize = 17,
                WriteBehindBatchSize = 18,
                WriteBehindEnabled = false,
                WriteSynchronizationMode = CacheWriteSynchronizationMode.PrimarySync,
                CacheStoreFactory = new CacheStoreFactoryTest(),
                ReadThrough = true,
                WriteThrough = true,
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyTypeName = "Integer",
                        ValueTypeName = "java.lang.String",
                        Fields = new[]
                        {
                            new QueryField("length", typeof(int)), 
                            new QueryField("name", typeof(string)) {IsKeyField = true}, 
                            new QueryField("location", typeof(string)),
                        },
                        Aliases = new [] {new QueryAlias("length", "len") },
                        Indexes = new[]
                        {
                            new QueryIndex("name") {Name = "index1" },
                            new QueryIndex(new QueryIndexField("location", true))
                            {
                                Name= "index2",
                                IndexType = QueryIndexType.FullText
                            }
                        }
                    }
                },
                NearConfiguration = new NearCacheConfiguration
                {
                    NearStartSize = 456,
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = 25,
                        MaxMemorySize = 2500,
                        BatchSize = 3
                    }
                },
                EvictionPolicy = new FifoEvictionPolicy
                {
                    MaxSize = 26,
                    MaxMemorySize = 2501,
                    BatchSize = 33
                },
                AffinityFunction = new RendezvousAffinityFunction
                {
                    Partitions = 513,
                    ExcludeNeighbors = true
                },
                ExpiryPolicyFactory = new ExpiryFactory(),
                EnableStatistics = true
            };
        }
        /// <summary>
        /// Gets the custom cache configuration with some variations.
        /// </summary>
        private static CacheConfiguration GetCustomCacheConfiguration2(string name = null)
        {
            return new CacheConfiguration
            {
                Name = name ?? CacheName2,
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
                AtomicWriteOrderMode = CacheAtomicWriteOrderMode.Clock,
                AtomicityMode = CacheAtomicityMode.Transactional,
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
                MemoryMode = CacheMemoryMode.OnheapTiered,
                ReadFromBackup = true,
                RebalanceBatchSize = 12,
                RebalanceDelay = TimeSpan.FromSeconds(13),
                RebalanceMode = CacheRebalanceMode.Sync,
                RebalanceThrottle = TimeSpan.FromSeconds(15),
                RebalanceTimeout = TimeSpan.FromSeconds(16),
                SqlEscapeAll = true,
                SqlOnheapRowCacheSize = 17,
                WriteBehindBatchSize = 18,
                WriteBehindEnabled = false,
                WriteSynchronizationMode = CacheWriteSynchronizationMode.PrimarySync,
                CacheStoreFactory = new CacheStoreFactoryTest(),
                ReadThrough = true,
                WriteThrough = true,
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyTypeName = "Integer",
                        ValueTypeName = "java.lang.String",
                        Fields = new[]
                        {
                            new QueryField("length", typeof(int)), 
                            new QueryField("name", typeof(string)), 
                            new QueryField("location", typeof(string)) {IsKeyField = true}
                        },
                        Aliases = new [] {new QueryAlias("length", "len") },
                        Indexes = new[]
                        {
                            new QueryIndex("name") {Name = "index1" },
                            new QueryIndex(new QueryIndexField("location", true))
                            {
                                Name= "index2",
                                IndexType = QueryIndexType.Sorted
                            }
                        }
                    }
                },
                NearConfiguration = new NearCacheConfiguration
                {
                    NearStartSize = 456,
                    EvictionPolicy = new FifoEvictionPolicy
                    {
                        MaxSize = 25,
                        MaxMemorySize = 2500,
                        BatchSize = 3
                    }
                },
                EvictionPolicy = new LruEvictionPolicy
                {
                    MaxSize = 26,
                    MaxMemorySize = 2501,
                    BatchSize = 33
                },
                AffinityFunction = new FairAffinityFunction
                {
                    Partitions = 113,
                    ExcludeNeighbors = false
                }
            };
        }

        /// <summary>
        /// Test factory.
        /// </summary>
        [Serializable]
        private class CacheStoreFactoryTest : IFactory<ICacheStore>
        {
            /// <summary>
            /// Gets or sets the test property.
            /// </summary>
            /// <value>
            /// The test property.
            /// </value>
            public int TestProperty { get; set; }

            /// <summary>
            /// Creates an instance of the cache store.
            /// </summary>
            /// <returns>
            /// New instance of the cache store.
            /// </returns>
            public ICacheStore CreateInstance()
            {
                _factoryProp = TestProperty;

                return new CacheStoreTest();
            }
        }

        /// <summary>
        /// Test store.
        /// </summary>
        private class CacheStoreTest : CacheStoreAdapter
        {
            /** <inheritdoc /> */
            public override object Load(object key)
            {
                return null;
            }

            /** <inheritdoc /> */
            public override void Write(object key, object val)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public override void Delete(object key)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Test entity.
        /// </summary>
        private class Entity
        {
            /// <summary>
            /// Gets or sets the foo.
            /// </summary>
            public int Foo { get; set; }
        }


        /// <summary>
        /// Expiry policy factory.
        /// </summary>
        private class ExpiryFactory : IFactory<IExpiryPolicy>
        {
            /** <inheritdoc /> */
            public IExpiryPolicy CreateInstance()
            {
                return new ExpiryPolicy(null, null, null);
            }
        }
    }
}

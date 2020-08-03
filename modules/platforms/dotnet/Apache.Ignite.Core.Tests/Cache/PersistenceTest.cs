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
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;
    using DataPageEvictionMode = Apache.Ignite.Core.Configuration.DataPageEvictionMode;

    /// <summary>
    /// Tests disk persistence.
    /// </summary>
    public class PersistenceTest
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
        /// Tests that cache data survives node restart.
        /// </summary>
        [Test]
        public void TestCacheDataSurvivesNodeRestart(
            [Values(true, false)] bool withCacheStore,
            [Values(true, false)] bool withCustomAffinity)
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    StoragePath = Path.Combine(_tempDir, "Store"),
                    WalPath = Path.Combine(_tempDir, "WalStore"),
                    WalArchivePath = Path.Combine(_tempDir, "WalArchive"),
                    MetricsEnabled = true,
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        PageEvictionMode = DataPageEvictionMode.Disabled,
                        Name = DataStorageConfiguration.DefaultDataRegionName,
                        PersistenceEnabled = true
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "volatileRegion",
                            PersistenceEnabled = false
                        } 
                    }
                }
            };

            const string cacheName = "persistentCache";
            const string volatileCacheName = "volatileCache";

            // Start Ignite, put data, stop.
            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCluster().SetActive(true);

                // Create cache with default region (persistence enabled), add data.
                var cache = ignite.CreateCache<int, int>(new CacheConfiguration
                {
                    Name = cacheName,
                    CacheStoreFactory = withCacheStore ? new CustomStoreFactory() : null,
                    AffinityFunction = withCustomAffinity ? new CustomAffinityFunction() : null
                });
                cache[1] = 1;

                // Check some metrics.
                CheckDataStorageMetrics(ignite);

                // Create cache with non-persistent region.
                var volatileCache = ignite.CreateCache<int, int>(new CacheConfiguration
                {
                    Name = volatileCacheName,
                    DataRegionName = "volatileRegion"
                });
                volatileCache[2] = 2;
            }

            // Verify directories.
            Assert.IsTrue(Directory.Exists(cfg.DataStorageConfiguration.StoragePath));
            Assert.IsTrue(Directory.Exists(cfg.DataStorageConfiguration.WalPath));
            Assert.IsTrue(Directory.Exists(cfg.DataStorageConfiguration.WalArchivePath));

            // Start Ignite, verify data survival.
            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCluster().SetActive(true);

                // Persistent cache already exists and contains data.
                var cache = ignite.GetCache<int, int>(cacheName);
                Assert.AreEqual(1, cache[1]);

                // Non-persistent cache does not exist.
                var ex = Assert.Throws<ArgumentException>(() => ignite.GetCache<int, int>(volatileCacheName));
                Assert.AreEqual("Cache doesn't exist: volatileCache", ex.Message);
            }

            // Delete store directory.
            Directory.Delete(_tempDir, true);

            // Start Ignite, verify data loss.
            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCluster().SetActive(true);

                Assert.IsFalse(ignite.GetCacheNames().Contains(cacheName));
            }
        }

        /// <summary>
        /// Checks that data storage metrics reflect some write operations.
        /// </summary>
        private static void CheckDataStorageMetrics(IIgnite ignite)
        {
            var metrics = ignite.GetDataStorageMetrics();
            Assert.Greater(metrics.WalLoggingRate, 0);
        }

        /// <summary>
        /// Tests the grid activation with persistence (inactive by default).
        /// </summary>
        [Test]
        public void TestGridActivationWithPersistence()
        {
            var cfg = GetPersistentConfiguration();

            // Default config, inactive by default (IsActiveOnStart is ignored when persistence is enabled).
            using (var ignite = Ignition.Start(cfg))
            {
                CheckIsActive(ignite, false);

                ignite.GetCluster().SetActive(true);
                CheckIsActive(ignite, true);

                ignite.GetCluster().SetActive(false);
                CheckIsActive(ignite, false);
            }
        }

        /// <summary>
        /// Tests the grid activation without persistence (active by default).
        /// </summary>
        [Test]
        public void TestGridActivationNoPersistence()
        {
            var cfg = TestUtils.GetTestConfiguration();
            Assert.IsTrue(cfg.IsActiveOnStart);

            using (var ignite = Ignition.Start(cfg))
            {
                CheckIsActive(ignite, true);

                ignite.GetCluster().SetActive(false);
                CheckIsActive(ignite, false);

                ignite.GetCluster().SetActive(true);
                CheckIsActive(ignite, true);
            }

            cfg.IsActiveOnStart = false;

            using (var ignite = Ignition.Start(cfg))
            {
                CheckIsActive(ignite, false);

                ignite.GetCluster().SetActive(true);
                CheckIsActive(ignite, true);

                ignite.GetCluster().SetActive(false);
                CheckIsActive(ignite, false);
            }
        }

        /// <summary>
        /// Tests the baseline topology.
        /// </summary>
        [Test]
        [Ignore("SetBaselineAutoAdjustEnabledFlag is not supported in 8.7, and IGNITE_BASELINE_AUTO_ADJUST_ENABLED " +
                "can't be set reliably with environment variables.'")]
        public void TestBaselineTopology()
        {
            using (EnvVar.Set("IGNITE_BASELINE_AUTO_ADJUST_ENABLED", "false"))
            {
                var cfg1 = new IgniteConfiguration(GetPersistentConfiguration())
                {
                    ConsistentId = "node1"
                };
                var cfg2 = new IgniteConfiguration(GetPersistentConfiguration())
                {
                    ConsistentId = "node2",
                    IgniteInstanceName = "2"
                };

                using (var ignite = Ignition.Start(cfg1))
                {
                    // Start and stop to bump topology version.
                    Ignition.Start(cfg2);
                    Ignition.Stop(cfg2.IgniteInstanceName, true);

                    var cluster = ignite.GetCluster();
                    Assert.AreEqual(3, cluster.TopologyVersion);

                    // Can not set baseline while inactive.
                    var ex = Assert.Throws<IgniteException>(() => cluster.SetBaselineTopology(2));
                    Assert.AreEqual("Changing BaselineTopology on inactive cluster is not allowed.", ex.Message);

                    cluster.SetActive(true);

                    // Can not set baseline with offline node.
                    ex = Assert.Throws<IgniteException>(() => cluster.SetBaselineTopology(2));
                    Assert.AreEqual("Check arguments. Node with consistent ID [node2] not found in server nodes.",
                        ex.Message);

                    cluster.SetBaselineTopology(1);
                    Assert.AreEqual("node1", cluster.GetBaselineTopology().Single().ConsistentId);

                    // Set with node.
                    cluster.SetBaselineTopology(cluster.GetBaselineTopology());

                    var res = cluster.GetBaselineTopology();
                    CollectionAssert.AreEquivalent(new[] {"node1"}, res.Select(x => x.ConsistentId));

                    cluster.SetBaselineTopology(cluster.GetTopology(1));
                    Assert.AreEqual("node1", cluster.GetBaselineTopology().Single().ConsistentId);

                    // Can not set baseline with offline node.
                    ex = Assert.Throws<IgniteException>(() => cluster.SetBaselineTopology(cluster.GetTopology(2)));
                    Assert.AreEqual("Check arguments. Node with consistent ID [node2] not found in server nodes.",
                        ex.Message);
                }

                // Check auto activation on cluster restart.
                using (var ignite = Ignition.Start(cfg1))
                using (Ignition.Start(cfg2))
                {
                    var cluster = ignite.GetCluster();
                    Assert.IsTrue(cluster.IsActive());

                    var res = cluster.GetBaselineTopology();
                    CollectionAssert.AreEquivalent(new[] {"node1"}, res.Select(x => x.ConsistentId));
                }
            }
        }

        /// <summary>
        /// Tests the wal disable/enable functionality.
        /// </summary>
        [Test]
        public void TestWalDisableEnable()
        {
            using (var ignite = Ignition.Start(GetPersistentConfiguration()))
            {
                var cluster = ignite.GetCluster();
                cluster.SetActive(true);

                var cache = ignite.CreateCache<int, int>("foo");
                Assert.IsTrue(cluster.IsWalEnabled(cache.Name));
                cache[1] = 1;

                cluster.DisableWal(cache.Name);
                Assert.IsFalse(cluster.IsWalEnabled(cache.Name));
                cache[2] = 2;

                cluster.EnableWal(cache.Name);
                Assert.IsTrue(cluster.IsWalEnabled(cache.Name));

                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(2, cache[2]);

                // Check exceptions.
                var ex = Assert.Throws<IgniteException>(() => cluster.IsWalEnabled("bar"));
                Assert.AreEqual("Cache not found: bar", ex.Message);

                ex = Assert.Throws<IgniteException>(() => cluster.DisableWal("bar"));
                Assert.AreEqual("Cache doesn't exist: bar", ex.Message);

                ex = Assert.Throws<IgniteException>(() => cluster.EnableWal("bar"));
                Assert.AreEqual("Cache doesn't exist: bar", ex.Message);
            }
        }

        /// <summary>
        /// Test the configuration of IsBaselineAutoAdjustEnabled flag
        /// </summary>
        [Test]
        public void TestBaselineTopologyAutoAdjustEnabledDisabled()
        {
            using (var ignite = Ignition.Start(GetPersistentConfiguration()))
            {
                ICluster cluster = ignite.GetCluster();
                cluster.SetActive(true);

                bool isEnabled = cluster.IsBaselineAutoAdjustEnabled();
                cluster.SetBaselineAutoAdjustEnabledFlag(!isEnabled);

                Assert.AreNotEqual(isEnabled, cluster.IsBaselineAutoAdjustEnabled());
            }
        }

        /// <summary>
        /// Test the configuration of BaselineAutoAdjustTimeout property
        /// </summary>
        [Test]
        public void TestBaselineTopologyAutoAdjustTimeoutWriteRead()
        {
            const long newTimeout = 333000;
            using (var ignite = Ignition.Start(GetPersistentConfiguration()))
            {
                ICluster cluster = ignite.GetCluster();
                cluster.SetActive(true);

                cluster.SetBaselineAutoAdjustTimeout(newTimeout);

                Assert.AreEqual(newTimeout, cluster.GetBaselineAutoAdjustTimeout());
            }
        }

        /// <summary>
        /// Checks active state.
        /// </summary>
        private static void CheckIsActive(IIgnite ignite, bool isActive)
        {
            Assert.AreEqual(isActive, ignite.GetCluster().IsActive());

            if (isActive)
            {
                var cache = ignite.GetOrCreateCache<int, int>("default");
                cache[1] = 1;
                Assert.AreEqual(1, cache[1]);
            }
            else
            {
                var ex = Assert.Throws<IgniteException>(() => ignite.GetOrCreateCache<int, int>("default"));
                Assert.AreEqual("Can not perform the operation because the cluster is inactive.",
                    ex.Message.Substring(0, 62));
            }
        }

        /// <summary>
        /// Gets the persistent configuration.
        /// </summary>
        private static IgniteConfiguration GetPersistentConfiguration()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        PersistenceEnabled = true,
                        Name = "foo"
                    }
                }
            };
        }

        private class CustomStoreFactory : IFactory<ICacheStore>
        {
            public ICacheStore CreateInstance()
            {
                return new CustomStore();
            }
        }

        private class CustomStore : CacheStoreAdapter<object, object>
        {
            public override object Load(object key)
            {
                return null;
            }

            public override void Write(object key, object val)
            {
                // No-op.
            }

            public override void Delete(object key)
            {
                // No-op.
            }
        }

        private class CustomAffinityFunction : RendezvousAffinityFunction
        {
            public override int Partitions
            {
                get { return 10; }
                set { throw new NotSupportedException(); }
            }
        }
    }
}

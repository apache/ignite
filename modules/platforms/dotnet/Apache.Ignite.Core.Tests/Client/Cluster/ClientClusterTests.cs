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

namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using System;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    ///  Cluster API tests for thin client.
    /// </summary>
    public class ClientClusterTests : ClientTestBase
    {
        /** Cache name. */
        private const string PersistentCache = "persistentCache";

        /** Persistence data region name. */
        private const string DataRegionName = "persistenceRegion";

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public override void TestSetUp()
        {
            var cacheCfg = new CacheConfiguration
            {
                Name = PersistentCache,
                DataRegionName = DataRegionName
            };

            var ignite = Ignition.GetIgnite();
            GetClientCluster().SetActive(true);

            // To make sure there is no persisted cache from previous runs.
            ignite.DestroyCache(PersistentCache);
            ignite.GetOrCreateCache<int, int>(cacheCfg);
        }

        /** <inheritDoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var baseConfig = base.GetIgniteConfiguration();

            baseConfig.DataStorageConfiguration = new DataStorageConfiguration
            {
                DataRegionConfigurations = new[]
                {
                    new DataRegionConfiguration
                    {
                        Name = DataRegionName,
                        PersistenceEnabled = true
                    }
                }
            };

            return baseConfig;
        }

        /// <summary>
        /// Test cluster activation.
        /// </summary>
        [Test]
        public void TestClusterActivation()
        {
            var clientCluster = GetClientCluster();
            clientCluster.SetActive(true);

            Assert.IsTrue(clientCluster.IsActive());
            Assert.IsTrue(GetCluster().IsActive());
        }

        /// <summary>
        /// Test cluster deactivation.
        /// </summary>
        [Test]
        public void TestClusterDeactivation()
        {
            var clientCluster = GetClientCluster();
            clientCluster.SetActive(false);

            Assert.IsFalse(clientCluster.IsActive());
            Assert.IsFalse(GetCluster().IsActive());
        }

        /// <summary>
        /// Test enable WAL.
        /// </summary>
        [Test]
        public void TestEnableWal()
        {
            var clientCluster = GetClientCluster();
            clientCluster.DisableWal(PersistentCache);

            Assert.IsTrue(clientCluster.EnableWal(PersistentCache));
            Assert.IsTrue(clientCluster.IsWalEnabled(PersistentCache));
            Assert.IsTrue(GetCluster().IsWalEnabled(PersistentCache));
        }

        /// <summary>
        /// Test enable WAL multiple times.
        /// </summary>
        [Test]
        public void TestEnableWalReturnsFalseIfWalWasEnabledBefore()
        {
            var clientCluster = GetClientCluster();
            clientCluster.DisableWal(PersistentCache);

            Assert.IsTrue(clientCluster.EnableWal(PersistentCache));
            Assert.IsFalse(clientCluster.EnableWal(PersistentCache));
        }

        /// <summary>
        /// Test enable WAL validates cache name.
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestEnableWalValidatesCacheNameArgument(string cacheName)
        {
            Assert.Throws<ArgumentException>(() => GetClientCluster().EnableWal(cacheName),
                "'cacheName' argument should not be null or empty.");
        }

        /// <summary>
        /// Test WAL enabled method validates cache name.
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestIsWalEnabledValidatesCacheNameArgument(string cacheName)
        {
            TestDelegate action = () => GetClientCluster().IsWalEnabled(cacheName);
            var ex = Assert.Throws<ArgumentException>(action);
            Assert.IsTrue(ex.Message.StartsWith("'cacheName' argument should not be null or empty."));
        }

        /// <summary>
        /// Test disable WAL.
        /// </summary>
        [Test]
        public void TestDisableWal()
        {
            var clientCluster = GetClientCluster();
            clientCluster.EnableWal(PersistentCache);

            Assert.IsTrue(clientCluster.DisableWal(PersistentCache));
            Assert.IsFalse(clientCluster.IsWalEnabled(PersistentCache));
            Assert.IsFalse(GetCluster().IsWalEnabled(PersistentCache));
        }

        /// <summary>
        /// Test disable WAL multiple times.
        /// </summary>
        [Test]
        public void TestDisableWalReturnsFalseIfWalWasDisabledBefore()
        {
            var clientCluster = GetClientCluster();
            clientCluster.EnableWal(PersistentCache);

            Assert.IsTrue(clientCluster.DisableWal(PersistentCache));
            Assert.IsFalse(clientCluster.DisableWal(PersistentCache));
        }

        /// <summary>
        /// Test disable WAL validates cache name.
        /// </summary>
        [TestCase(null)]
        [TestCase("")]
        public void TestDisableWalValidatesCacheNameArgument(string cacheName)
        {
            TestDelegate action = () => GetClientCluster().DisableWal(cacheName);
            var ex = Assert.Throws<ArgumentException>(action);
            Assert.IsTrue(ex.Message.StartsWith("'cacheName' argument should not be null or empty."));
        }

        /// <summary>
        /// Test invalid cache name triggers IgniteClientException.
        /// </summary>
        [Test]
        public void TestInvalidCacheNameTriggersIgniteClientException()
        {
            const string invalidCacheName = "invalidCacheName";
            TestDelegate action = () => GetClientCluster().EnableWal(invalidCacheName);
            var ex = Assert.Throws<IgniteClientException>(action);
            Assert.AreEqual("Cache doesn't exist: " + invalidCacheName, ex.Message);
        }

        /// <summary>
        /// Returns Ignite client cluster.
        /// </summary>
        private IClientCluster GetClientCluster()
        {
            return Client.GetCluster();
        }

        /// <summary>
        /// Returns Ignite cluster.
        /// </summary>
        private ICluster GetCluster()
        {
            return Ignition.GetIgnite().GetCluster();
        }
    }
}

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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Tests.Query;
    using NUnit.Framework;

    /// <summary>
    /// Tests for dynamic a cache start.
    /// </summary>
    public class CacheDynamicStartTest
    {
        /** Grid name: data. */
        private const string GridData = "d";

        /** Grid name: data, no configuration. */
        private const string GridDataNoCfg = "dnc";

        /** Grid name: client. */
        private const string GridClient = "c";

        /** Cache name: partitioned, transactional. */
        private const string CacheTx = "p";

        /** Cache name: atomic. */
        private const string CacheAtomic = "pa";

        /** Cache name: dummy. */
        private const string CacheDummy = "dummy";
        
        /// <summary>
        /// Set up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();
            Ignition.StopAll(true);

            Ignition.Start(CreateConfiguration(GridData, @"config/dynamic/dynamic-data.xml"));
            Ignition.Start(CreateConfiguration(GridDataNoCfg, @"config/dynamic/dynamic-data-no-cfg.xml"));
            Ignition.Start(CreateConfiguration(GridClient, @"config/dynamic/dynamic-client.xml"));
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TearDown]
        public void StopGrids()
        {
            Ignition.Stop(GridData, true);
            Ignition.Stop(GridDataNoCfg, true);
            Ignition.Stop(GridClient, true);
        }

        /// <summary>
        /// Create configuration.
        /// </summary>
        /// <param name="name">Grid name.</param>
        /// <param name="springCfg">Spring configuration.</param>
        /// <returns>Configuration.</returns>
        private static IgniteConfiguration CreateConfiguration(string name, string springCfg)
        {
            var cfg = new IgniteConfiguration();

            BinaryConfiguration portCfg = new BinaryConfiguration();

            ICollection<BinaryTypeConfiguration> portTypeCfgs = new List<BinaryTypeConfiguration>();

            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(DynamicTestKey)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(DynamicTestValue)));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.GridName = name;
            cfg.BinaryConfiguration = portCfg;
            cfg.JvmClasspath = TestUtils.CreateTestClasspath();
            cfg.JvmOptions = TestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = springCfg;

            return cfg;
        }

        /// <summary>
        /// Try getting not configured cache.
        /// </summary>
        [Test]
        public void TestNoStarted()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                Ignition.GetIgnite(GridData).GetCache<CacheTestKey, BinarizablePerson>(CacheDummy);
            });

            Assert.Throws<ArgumentException>(() =>
            {
                Ignition.GetIgnite(GridDataNoCfg).GetCache<CacheTestKey, BinarizablePerson>(CacheDummy);
            });

            Assert.Throws<ArgumentException>(() =>
            {
                Ignition.GetIgnite(GridClient).GetCache<CacheTestKey, BinarizablePerson>(CacheDummy);
            });
        }

        /// <summary>
        /// Test TX cache.
        /// </summary>
        [Test]
        public void TestTransactional()
        {
            Check(CacheTx);
        }

        /// <summary>
        /// Test ATOMIC cache.
        /// </summary>
        [Test]
        public void TestAtomic()
        {
            Check(CacheAtomic);
        }

        /// <summary>
        /// Check routine.
        /// </summary>
        /// <param name="cacheName">Cache name.</param>
        private void Check(string cacheName)
        {
            ICache<DynamicTestKey, DynamicTestValue> cacheData =
                Ignition.GetIgnite(GridData).GetCache<DynamicTestKey, DynamicTestValue>(cacheName);

            ICache<DynamicTestKey, DynamicTestValue> cacheDataNoCfg =
                Ignition.GetIgnite(GridDataNoCfg).GetCache<DynamicTestKey, DynamicTestValue>(cacheName);

            ICache<DynamicTestKey, DynamicTestValue> cacheClient =
                Ignition.GetIgnite(GridClient).GetCache<DynamicTestKey, DynamicTestValue>(cacheName);

            DynamicTestKey key1 = new DynamicTestKey(1);
            DynamicTestKey key2 = new DynamicTestKey(2);
            DynamicTestKey key3 = new DynamicTestKey(3);

            DynamicTestValue val1 = new DynamicTestValue(1);
            DynamicTestValue val2 = new DynamicTestValue(2);
            DynamicTestValue val3 = new DynamicTestValue(3);

            cacheData.Put(key1, val1);
            Assert.AreEqual(val1, cacheData.Get(key1));
            Assert.AreEqual(val1, cacheDataNoCfg.Get(key1));
            Assert.AreEqual(val1, cacheClient.Get(key1));

            cacheDataNoCfg.Put(key2, val2);
            Assert.AreEqual(val2, cacheData.Get(key2));
            Assert.AreEqual(val2, cacheDataNoCfg.Get(key2));
            Assert.AreEqual(val2, cacheClient.Get(key2));

            cacheClient.Put(key3, val3);
            Assert.AreEqual(val3, cacheData.Get(key3));
            Assert.AreEqual(val3, cacheDataNoCfg.Get(key3));
            Assert.AreEqual(val3, cacheClient.Get(key3));

            for (int i = 0; i < 10000; i++)
                cacheClient.Put(new DynamicTestKey(i), new DynamicTestValue(1));

            int sizeClient = cacheClient.GetLocalSize();

            Assert.AreEqual(0, sizeClient);
        }
    }

    /// <summary>
    /// Key for dynamic cache start tests.
    /// </summary>
    class DynamicTestKey
    {
        /// <summary>
        /// Default constructor.
        /// </summary>
        public DynamicTestKey()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        public DynamicTestKey(int id)
        {
            Id = id;
        }

        /// <summary>
        /// ID.
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            DynamicTestKey other = obj as DynamicTestKey;

            return other != null && Id == other.Id;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return Id;
        }
    }

    /// <summary>
    /// Value for dynamic cache start tests.
    /// </summary>
    class DynamicTestValue
    {
        /// <summary>
        /// Default constructor.
        /// </summary>
        public DynamicTestValue()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        public DynamicTestValue(int id)
        {
            Id = id;
        }

        /// <summary>
        /// ID.
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            DynamicTestValue other = obj as DynamicTestValue;

            return other != null && Id == other.Id;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return Id;
        }
    }
}

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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Tests.Query;
    using NUnit.Framework;

    /// <summary>
    /// Tests for dynamic cache start.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
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
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Create configuration.
        /// </summary>
        /// <param name="name">Grid name.</param>
        /// <param name="springCfg">Spring configuration.</param>
        /// <returns>Configuration.</returns>
        private static IgniteConfiguration CreateConfiguration(string name, string springCfg)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = name,
                BinaryConfiguration = new BinaryConfiguration(typeof(DynamicTestKey), typeof(DynamicTestValue)),
                SpringConfigUrl = springCfg
            };
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
        private static void Check(string cacheName)
        {
            var cacheData = Ignition.GetIgnite(GridData).GetCache<DynamicTestKey, DynamicTestValue>(cacheName);

            var cacheDataNoCfg = 
                Ignition.GetIgnite(GridDataNoCfg).GetCache<DynamicTestKey, DynamicTestValue>(cacheName);

            var cacheClient = Ignition.GetIgnite(GridClient).GetCache<DynamicTestKey, DynamicTestValue>(cacheName);

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

        /// <summary>
        /// Key for dynamic cache start tests.
        /// </summary>
        private class DynamicTestKey
        {
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
            public int Id { get; set; }

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
        private class DynamicTestValue
        {
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
            public int Id { get; set; }

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
}

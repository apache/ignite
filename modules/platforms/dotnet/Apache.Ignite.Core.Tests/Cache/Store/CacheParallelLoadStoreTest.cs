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

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for GridCacheParallelLoadStoreAdapter.
    /// </summary>
    public sealed class CacheParallelLoadStoreTest
    {
        // object store name
        private const string ObjectStoreCacheName = "object_store_parallel";

        /// <summary>
        /// Set up test class.
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTests()
        {
            Ignition.Start(new IgniteConfiguration
            {
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                SpringConfigUrl = "config\\native-client-test-cache-parallel-store.xml",
                BinaryConfiguration = new BinaryConfiguration
                {
                    Types = new[] {typeof (CacheTestParallelLoadStore.Record).FullName}
                }
            });
        }

        /// <summary>
        /// Tear down test class.
        /// </summary>
        [TestFixtureTearDown]
        public void AfterTests()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Test setup.
        /// </summary>
        [SetUp]
        public void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        /// Tests the LoadCache.
        /// </summary>
        [Test]
        public void TestLoadCache()
        {
            var cache = GetCache();

            Assert.AreEqual(0, cache.GetSize());

            const int minId = 113;
            const int expectedItemCount = CacheTestParallelLoadStore.InputDataLength - minId;

            CacheTestParallelLoadStore.ResetCounters();

            cache.LocalLoadCache(null, minId);

            Assert.AreEqual(expectedItemCount, cache.GetSize());

            // check items presence; increment by 100 to speed up the test
            for (var i = minId; i < expectedItemCount; i += 100)
            {
                var rec = cache.Get(i);
                Assert.AreEqual(i, rec.Id);
            }

            // check that items were processed in parallel
            Assert.GreaterOrEqual(CacheTestParallelLoadStore.UniqueThreadCount, Environment.ProcessorCount - 1);
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private static ICache<int, CacheTestParallelLoadStore.Record> GetCache()
        {
            return Ignition.GetIgnite().GetCache<int, CacheTestParallelLoadStore.Record>(ObjectStoreCacheName);
        }
    }
}

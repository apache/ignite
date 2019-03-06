/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System;
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
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = "config\\native-client-test-cache-parallel-store.xml",
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

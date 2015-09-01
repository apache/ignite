/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Store
{
    using System;

    using NUnit.Framework;

    using GridGain.Portable;
    using GridGain.Client;

    /// <summary>
    /// Tests for GridCacheParallelLoadStoreAdapter.
    /// </summary>
    public class GridCacheParallelLoadStoreTest
    {
        // object store name
        private const string OBJECT_STORE_CACHE_NAME = "object_store_parallel";

        /// <summary>
        /// Set up test class.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void BeforeTests()
        {
            GridTestUtils.KillProcesses();
            GridTestUtils.JVM_DEBUG = true;

            GridFactory.Start(new GridConfiguration
            {
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                SpringConfigUrl = "config\\native-client-test-cache-parallel-store.xml",
                PortableConfiguration = new PortableConfiguration
                {
                    Types = new[] {typeof (GridCacheTestParallelLoadStore.Record).FullName}
                }
            });
        }

        /// <summary>
        /// Tear down test class.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void AfterTests()
        {
            GridFactory.StopAll(true);
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

            Assert.AreEqual(0, cache.Size());

            const int minId = 113;
            const int expectedItemCount = GridCacheTestParallelLoadStore.INPUT_DATA_LENGTH - minId;

            GridCacheTestParallelLoadStore.ResetCounters();

            cache.LocalLoadCache(null, minId);

            Assert.AreEqual(expectedItemCount, cache.Size());

            // check items presence; increment by 100 to speed up the test
            for (var i = minId; i < expectedItemCount; i += 100)
            {
                var rec = cache.Get(i);
                Assert.AreEqual(i, rec.Id);
            }

            // check that items were processed in parallel
            Assert.GreaterOrEqual(GridCacheTestParallelLoadStore.UniqueThreadCount, Environment.ProcessorCount);
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private static ICache<int, GridCacheTestParallelLoadStore.Record> GetCache()
        {
            return GridFactory.Grid().Cache<int, GridCacheTestParallelLoadStore.Record>(OBJECT_STORE_CACHE_NAME);
        }
    }
}

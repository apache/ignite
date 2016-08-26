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

namespace Apache.Ignite.Core.Tests.EntityFramework
{
    using System;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Tests the EF cache provider.
    /// </summary>
    public class SecondLevelCacheTest
    {
        // TODO: Integrated test with EF code first! This is a must! With multithreading, etc.
        // https://www.stevefenton.co.uk/2015/11/using-an-in-memory-database-as-a-test-double-with-entity-framework/

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Environment.SetEnvironmentVariable(Classpath.EnvIgniteNativeTestClasspath, "true");
        }

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the IgniteDbConfiguration.
        /// </summary>
        [Test]
        public void TestConfigurationAndStartup()
        {
            Assert.IsNull(Ignition.TryGetIgnite());

            // Test default config (picks up app.config section)
            CheckCacheAndStop("myGrid1", IgniteDbConfiguration.DefaultCacheName, new IgniteDbConfiguration());

            // Specific config section
            CheckCacheAndStop("myGrid2", "cacheName2", 
                new IgniteDbConfiguration("igniteConfiguration2", "cacheName2", null));

            // Specific config section, nonexistent cache
            CheckCacheAndStop("myGrid2", "newCache", 
                new IgniteDbConfiguration("igniteConfiguration2", "newCache", null));

            // In-code configuration
            CheckCacheAndStop("myGrid3", "myCache",
                new IgniteDbConfiguration(new IgniteConfiguration
                {
                    GridName = "myGrid3",
                    CacheConfiguration = new[]
                    {
                        new CacheConfiguration
                        {
                            Name = "myCache",
                            CacheMode = CacheMode.Replicated,
                            AtomicityMode = CacheAtomicityMode.Transactional
                        }
                    }
                }, "myCache", null), CacheMode.Replicated);

            // Non-transactional cache
            var ex = Assert.Throws<ArgumentException>(() =>
                CheckCacheAndStop("myGrid5", "myCache",
                    new IgniteDbConfiguration(new IgniteConfiguration
                    {
                        GridName = "myGrid5",
                        CacheConfiguration = new[] {new CacheConfiguration("myCache")}
                    }, "myCache", null)));
            Ignition.StopAll(true);
            Assert.IsTrue(ex.Message.Contains(
                    "IgniteEntityFrameworkCache requires Transactional cache. Specified 'myCache' cache is Atomic."));

            // Existing instance
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());
            CheckCacheAndStop(null, "123", new IgniteDbConfiguration(ignite, "123", null));
        }

        /// <summary>
        /// Checks that specified cache exists and stops all Ignite instances.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private static void CheckCacheAndStop(string gridName, string cacheName, IgniteDbConfiguration cfg,
            CacheMode cacheMode = CacheMode.Partitioned)
        {
            Assert.IsNotNull(cfg);

            var ignite = Ignition.TryGetIgnite(gridName);
            Assert.IsNotNull(ignite);

            var cache = ignite.GetCache<object, object>(cacheName);
            Assert.IsNotNull(cache);
            Assert.AreEqual(cacheMode, cache.GetConfiguration().CacheMode);

            Ignition.StopAll(true);
        }

        /*
        /// <summary>
        /// Tests put/get/invalidate operations.
        /// </summary>
        [Test]
        public void TestPutGetInvalidate()
        {
            var cache = CreateEfCache();

            // Missing value
            object val;
            Assert.IsFalse(cache.GetItem("1", out val));
            Assert.IsNull(val);

            // Put
            cache.PutItem("1", "val", new [] {"persons"}, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            Assert.IsTrue(cache.GetItem("1", out val));
            Assert.AreEqual("val", val);

            // Overwrite
            cache.PutItem("1", "val1", new[] { "persons" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            Assert.IsTrue(cache.GetItem("1", out val));
            Assert.AreEqual("val1", val);

            // Invalidate
            cache.InvalidateItem("1");
            Assert.IsFalse(cache.GetItem("1", out val));

            // Invalidate sets
            cache.PutItem("1", "val1", new[] { "persons" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("2", "val2", new[] { "address" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.PutItem("3", "val2", new[] { "companies", "persons" }, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            cache.InvalidateSets(new[] {"cars", "persons"});

            Assert.IsFalse(cache.GetItem("1", out val));
            Assert.IsTrue(cache.GetItem("2", out val));
            Assert.IsFalse(cache.GetItem("3", out val));
        }

        /// <summary>
        /// Tests expiration logic.
        /// </summary>
        [Test]
        public void TestExpiration()
        {
            var cache = CreateEfCache();
            object val;

            // Absolute expiration
            cache.PutItem("1", "val", new[] { "persons" }, TimeSpan.MaxValue, DateTimeOffset.Now.AddMilliseconds(300));
            Assert.IsTrue(cache.GetItem("1", out val));
            Thread.Sleep(150);
            Assert.IsTrue(cache.GetItem("1", out val));
            Thread.Sleep(150);
            Assert.IsFalse(cache.GetItem("1", out val));

            // Sliding expiration: not supported
            Assert.Throws<NotSupportedException>(
                () => cache.PutItem("2", "val", new[] {"persons"}, TimeSpan.FromMilliseconds(300),
                    DateTimeOffset.Now.AddMilliseconds(300)));
        }

        /// <summary>
        /// Creates the EntityFramework cache.
        /// </summary>
        private static ICache CreateEfCache()
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());
            var cache = ignite.CreateCache<string, object>(new CacheConfiguration("efCache")
            {
                AtomicityMode = CacheAtomicityMode.Transactional
            });

            return new IgniteEntityFrameworkCache(cache);
        }
        */
    }
}

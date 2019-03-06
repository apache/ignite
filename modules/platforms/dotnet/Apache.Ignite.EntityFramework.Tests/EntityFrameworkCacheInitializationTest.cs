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

namespace Apache.Ignite.EntityFramework.Tests
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Tests;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Tests the EF cache provider.
    /// </summary>
    public class EntityFrameworkCacheInitializationTest
    {
        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the IgniteDbConfiguration.
        /// </summary>
        [Test]
        public void TestConfigurationAndStartup()
        {
            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");

            Assert.IsNull(Ignition.TryGetIgnite());

            // Test default config (picks up app.config section).
            CheckCacheAndStop("myGrid1", IgniteDbConfiguration.DefaultCacheNamePrefix, new IgniteDbConfiguration());

            // Specific config section.
            CheckCacheAndStop("myGrid2", "cacheName2",
                new IgniteDbConfiguration("igniteConfiguration2", "cacheName2", null));

            // Specific config section, nonexistent cache.
            CheckCacheAndStop("myGrid2", "newCache",
                new IgniteDbConfiguration("igniteConfiguration2", "newCache", null));

            // In-code configuration.
            CheckCacheAndStop("myGrid3", "myCache",
                new IgniteDbConfiguration(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                    {
                        IgniteInstanceName = "myGrid3"
                    }, new CacheConfiguration("myCache_metadata")
                    {
                        CacheMode = CacheMode.Replicated,
                        AtomicityMode = CacheAtomicityMode.Transactional
                    },
                    new CacheConfiguration("myCache_data") {CacheMode = CacheMode.Replicated}, null),
                CacheMode.Replicated);

            // Existing instance.
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());
            CheckCacheAndStop(null, "123", new IgniteDbConfiguration(ignite,
                new CacheConfiguration("123_metadata")
                {
                    Backups = 1,
                    AtomicityMode = CacheAtomicityMode.Transactional
                },
                new CacheConfiguration("123_data"), null));

            // Non-tx meta cache.
            var ex = Assert.Throws<IgniteException>(() => CheckCacheAndStop(null, "123",
                new IgniteDbConfiguration(TestUtils.GetTestConfiguration(), 
                    new CacheConfiguration("123_metadata"),
                    new CacheConfiguration("123_data"), null)));

            Assert.AreEqual("EntityFramework meta cache should be Transactional.", ex.Message);

            // Same cache names.
            var ex2 = Assert.Throws<ArgumentException>(() => CheckCacheAndStop(null, "abc",
                new IgniteDbConfiguration(TestUtils.GetTestConfiguration(),
                    new CacheConfiguration("abc"),
                    new CacheConfiguration("abc"), null)));

            Assert.IsTrue(ex2.Message.Contains("Meta and Data cache can't have the same name."));
        }

        /// <summary>
        /// Checks that specified cache exists and stops all Ignite instances.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private static void CheckCacheAndStop(string gridName, string cacheName, IgniteDbConfiguration cfg,
            CacheMode cacheMode = CacheMode.Partitioned)
        {
            try
            {
                Assert.IsNotNull(cfg);

                var ignite = Ignition.TryGetIgnite(gridName);
                Assert.IsNotNull(ignite);

                var metaCache = ignite.GetCache<object, object>(cacheName + "_metadata");
                Assert.IsNotNull(metaCache);
                Assert.AreEqual(cacheMode, metaCache.GetConfiguration().CacheMode);

                if (cacheMode == CacheMode.Partitioned)
                    Assert.AreEqual(1, metaCache.GetConfiguration().Backups);

                var dataCache = ignite.GetCache<object, object>(cacheName + "_data");
                Assert.IsNotNull(dataCache);
                Assert.AreEqual(cacheMode, dataCache.GetConfiguration().CacheMode);

                if (cacheMode == CacheMode.Partitioned)
                    Assert.AreEqual(0, dataCache.GetConfiguration().Backups);
            }
            finally
            {
                Ignition.StopAll(true);
            }
        }
    }
}

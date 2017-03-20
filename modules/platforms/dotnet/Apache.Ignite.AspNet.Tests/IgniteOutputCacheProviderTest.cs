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

namespace Apache.Ignite.AspNet.Tests
{
    using System;
    using System.Collections.Specialized;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Tests;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteOutputCacheProvider"/>
    /// </summary>
    public class IgniteOutputCacheProviderTest
    {
        /** Grid name XML config attribute. */
        private const string GridNameAttr = "gridName";

        /** Cache name XML config attribute. */
        private const string CacheNameAttr = "cacheName";

        /** Cache name XML config attribute. */
        private const string SectionNameAttr = "igniteConfigurationSectionName";

        /** Grid name. */
        private const string GridName = "grid1";

        /** Cache name. */
        private const string CacheName = "myCache";

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()) {IgniteInstanceName = GridName});
        }

        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests provider initialization.
        /// </summary>
        [Test]
        public void TestInitialization()
        {
            var cacheProvider = new IgniteOutputCacheProvider();

            // Not initialized.
            Assert.Throws<InvalidOperationException>(() => cacheProvider.Get("1"));

            // Invalid section.
            Assert.Throws<IgniteException>(() =>
                cacheProvider.Initialize("testName", new NameValueCollection
                {
                    {SectionNameAttr, "invalidSection"},
                }));

            // Valid grid.
            cacheProvider = GetProvider();

            cacheProvider.Set("1", 1, DateTime.MaxValue);
            Assert.AreEqual(1, cacheProvider.Get("1"));
        }

        /// <summary>
        /// Tests autostart from web configuration section.
        /// </summary>
        [Test]
        public void TestStartFromWebConfigSection()
        {
            var cacheProvider = new IgniteOutputCacheProvider();

            cacheProvider.Initialize("testName2", new NameValueCollection
            {
                {SectionNameAttr, "igniteConfiguration2"},
                {CacheNameAttr, "cacheName2"}
            });

            cacheProvider.Set("1", 3, DateTime.MaxValue);
            Assert.AreEqual(3, cacheProvider.Get("1"));
        }

        /// <summary>
        /// Tests provider caching.
        /// </summary>
        [Test]
        public void TestCaching()
        {
            var cacheProvider = GetProvider();

            Assert.AreEqual(null, cacheProvider.Get("1"));
            cacheProvider.Set("1", 1, DateTime.MaxValue);
            Assert.AreEqual(1, cacheProvider.Get("1"));

            cacheProvider.Remove("1");
            Assert.AreEqual(null, cacheProvider.Get("1"));

            Assert.AreEqual(null, cacheProvider.Add("2", 2, DateTime.MaxValue));
            Assert.AreEqual(2, cacheProvider.Add("2", 5, DateTime.MaxValue));
        }

        /// <summary>
        /// Tests cache expiration.
        /// </summary>
        [Test]
        public void TestExpiry()
        {
            var cacheProvider = GetProvider();
            cacheProvider.Remove("1");

            // Set
            cacheProvider.Set("1", 1, DateTime.UtcNow.AddSeconds(1.3));
            Assert.AreEqual(1, cacheProvider.Get("1"));
            Thread.Sleep(2000);
            Assert.AreEqual(null, cacheProvider.Get("1"));

            cacheProvider.Set("1", 1, DateTime.UtcNow);
            Assert.AreEqual(null, cacheProvider.Get("1"));

            // Add
            cacheProvider.Add("1", 1, DateTime.UtcNow.AddSeconds(0.7));
            Assert.AreEqual(1, cacheProvider.Get("1"));
            Thread.Sleep(2000);
            Assert.AreEqual(null, cacheProvider.Get("1"));

            cacheProvider.Add("1", 1, DateTime.UtcNow);
            Assert.AreEqual(null, cacheProvider.Get("1"));
        }

        /// <summary>
        /// Gets the initialized provider.
        /// </summary>
        private static IgniteOutputCacheProvider GetProvider()
        {
            var cacheProvider = new IgniteOutputCacheProvider();

            cacheProvider.Initialize("testName", new NameValueCollection
            {
                {GridNameAttr, GridName},
                {CacheNameAttr, CacheName}
            });

            return cacheProvider;
        }
    }
}

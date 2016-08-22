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

namespace Apache.Ignite.Core.Tests.AspNet
{
    using System;
    using System.Collections.Specialized;
    using Apache.Ignite.AspNet;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteSessionStateStoreProvider"/>.
    /// </summary>
    public class IgniteSessionStateStoreProviderTest
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
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()) { GridName = GridName });
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
            var stateProvider = new IgniteSessionStateStoreProvider();

            // Not initialized
            Assert.Throws<InvalidOperationException>(() => stateProvider.Get("1"));

            // Grid not started
            Assert.Throws<IgniteException>(() =>
                stateProvider.Initialize("testName", new NameValueCollection
                {
                    {GridNameAttr, "invalidGridName"},
                    {CacheNameAttr, CacheName}
                }));

            // Valid grid
            stateProvider = GetProvider();

            stateProvider.Set("1", 1, DateTime.MaxValue);
            Assert.AreEqual(1, stateProvider.Get("1"));
        }


        /// <summary>
        /// Gets the initialized provider.
        /// </summary>
        private static IgniteSessionStateStoreProvider GetProvider()
        {
            var stateProvider = new IgniteSessionStateStoreProvider();

            stateProvider.Initialize("testName", new NameValueCollection
            {
                {GridNameAttr, GridName},
                {CacheNameAttr, CacheName}
            });

            return stateProvider;
        }

    }
}

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
    using System.Web;
    using System.Web.SessionState;
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

            SessionStateActions actions;
            bool locked;
            TimeSpan lockAge;
            object lockId;


            // Not initialized
            Assert.Throws<InvalidOperationException>(() =>
                    stateProvider.GetItem(GetHttpContext(), "1", out locked, out lockAge, out lockId, out actions));

            // Grid not started
            Assert.Throws<IgniteException>(() =>
                stateProvider.Initialize("testName", new NameValueCollection
                {
                    {GridNameAttr, "invalidGridName"},
                    {CacheNameAttr, CacheName}
                }));

            // Valid grid
            stateProvider = GetProvider();

            CheckProvider(stateProvider);
        }

        /// <summary>
        /// Tests autostart from web configuration section.
        /// </summary>
        [Test]
        public void TestStartFromWebConfigSection()
        {
            var provider = new IgniteSessionStateStoreProvider();

            provider.Initialize("testName3", new NameValueCollection
            {
                {SectionNameAttr, "igniteConfiguration3"},
                {CacheNameAttr, "cacheName3"}
            });

            CheckProvider(provider);
        }

        /// <summary>
        /// Tests the transactional requirement.
        /// </summary>
        [Test]
        public void TestTransactionalRequirement()
        {
            var provider = new IgniteSessionStateStoreProvider();

            var ex = Assert.Throws<IgniteException>(() =>
                provider.Initialize("testName2", new NameValueCollection
                {
                    {SectionNameAttr, "igniteConfiguration2"},
                    {CacheNameAttr, "cacheName2"}
                }));

            Assert.IsTrue(ex.Message.Contains("Transactional mode is required."));
        }

        /// <summary>
        /// Tests the caching.
        /// </summary>
        [Test]
        public void TestCaching()
        {
            var provider = GetProvider();

        }

        /// <summary>
        /// Tests the caching in read-only scenario.
        /// </summary>
        [Test]
        public void TestCachingReadOnly()
        {
            var provider = GetProvider();

            //provider.GetItem(GetHttpContext(), "1")

        }

        /// <summary>
        /// Tests the expiry.
        /// </summary>
        [Test]
        public void TestExpiry()
        {
            Assert.IsFalse(GetProvider().SetItemExpireCallback(null));
        }

        /// <summary>
        /// Tests the locking.
        /// </summary>
        [Test]
        public void TestLocking()
        {
            
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

        /// <summary>
        /// Checks the provider.
        /// </summary>
        private static void CheckProvider(SessionStateStoreProviderBase provider)
        {
            bool locked;
            TimeSpan lockAge;
            object lockId;
            SessionStateActions actions;

            provider.InitializeRequest(GetHttpContext());

            var data = provider.GetItemExclusive(GetHttpContext(), "1", out locked, out lockAge,
                out lockId, out actions);
            Assert.IsNull(data);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.IsNotNull(lockId);
            Assert.AreEqual(SessionStateActions.None, actions);

            data = provider.CreateNewStoreData(GetHttpContext(), 42);
            Assert.IsNotNull(data);

            provider.SetAndReleaseItemExclusive(GetHttpContext(), "1", data, lockId, false);

            data = provider.GetItem(GetHttpContext(), "1", out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(data);
            Assert.AreEqual(42, data.Timeout);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.IsNotNull(lockId);
            Assert.AreEqual(SessionStateActions.None, actions);

            provider.ResetItemTimeout(GetHttpContext(), "1");
            provider.EndRequest(GetHttpContext());
            provider.Dispose();
        }

        /// <summary>
        /// Gets the HTTP context.
        /// </summary>
        private static HttpContext GetHttpContext()
        {
            return new HttpContext(new HttpRequest(null, "http://tempuri.org", null), new HttpResponse(null));
        }
    }
}

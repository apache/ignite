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
    using System.Configuration;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web;
    using System.Web.SessionState;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Tests;
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

        /** Section name XML config attribute. */
        private const string SectionNameAttr = "igniteConfigurationSectionName";

        /** Grid name. */
        private const string GridName = "grid1";

        /** Cache name. */
        private const string CacheName = "myCache";

        /** Session id. */
        private const string Id = "1";

        /** Test context. */
        private static readonly HttpContext HttpContext = 
            new HttpContext(new HttpRequest(null, "http://tempuri.org", null), new HttpResponse(null));

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()) { IgniteInstanceName = GridName });
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
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            // Clear all caches.
            var ignite = Ignition.GetIgnite(GridName);
            ignite.GetCacheNames().ToList().ForEach(x => ignite.GetCache<object, object>(x).RemoveAll());
        }
        
        /// <summary>
        /// Test setup.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            // Make sure caches are empty.
            var ignite = Ignition.GetIgnite(GridName);

            foreach (var cache in ignite.GetCacheNames().Select(x => ignite.GetCache<object, object>(x)))
                CollectionAssert.IsEmpty(cache.ToArray());
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

            // Not initialized.
            Assert.Throws<InvalidOperationException>(() =>
                    stateProvider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions));

            // Missing section.
            var ex = Assert.Throws<IgniteException>(() =>
                stateProvider.Initialize("testName", new NameValueCollection
                {
                    {SectionNameAttr, "missingSection"},
                    {CacheNameAttr, CacheName}
                }));

            Assert.IsInstanceOf<ConfigurationErrorsException>(ex.InnerException);

            // Invalid section with missing content.
            stateProvider = new IgniteSessionStateStoreProvider();

            ex = Assert.Throws<IgniteException>(() =>
                stateProvider.Initialize("testName", new NameValueCollection
                {
                    {SectionNameAttr, "igniteConfigurationInvalid"},
                    {CacheNameAttr, CacheName}
                }));

            Assert.IsInstanceOf<ConfigurationErrorsException>(ex.InnerException);

            // Valid grid.
            stateProvider = GetProvider();

            CheckProvider(stateProvider);

            // Same grid once again.
            stateProvider = GetProvider();

            CheckProvider(stateProvider);

            // Omitted cache name results in default cache name (not null).
            stateProvider = new IgniteSessionStateStoreProvider();

            stateProvider.Initialize("testName", new NameValueCollection
            {
                {GridNameAttr, GridName}
            });

            var cacheNames = Ignition.GetIgnite(GridName).GetCacheNames();

            Assert.IsFalse(cacheNames.Contains(null));
            Assert.IsTrue(cacheNames.Contains(IgniteSessionStateStoreProvider.DefaultCacheName));
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
        /// Tests the caching.
        /// </summary>
        [Test]
        public void TestCaching()
        {
            bool locked;
            TimeSpan lockAge;
            object lockId;
            SessionStateActions actions;

            var provider = GetProvider();

            // Not locked, no item.
            var res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);
            Assert.IsNull(lockId);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // Exclusive: not locked, no item.
            res = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);
            Assert.IsNull(lockId);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // SetAndRelease with no lock. This happens with certain versions of ASP.NET.
            var item = provider.CreateNewStoreData(HttpContext, 7);
            // ReSharper disable once AssignNullToNotNullAttribute (lockId is not supposed to be null, but it can be).
            provider.SetAndReleaseItemExclusive(HttpContext, Id, item, null, true);

            // Check added item.
            res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(res);
            Assert.IsNull(lockId);
            Assert.AreEqual(7, res.Timeout);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // Remove item.
            // ReSharper disable once AssignNullToNotNullAttribute (lockId is not supposed to be null, but it can be).
            provider.RemoveItem(HttpContext, Id, null, null);

            // Add item.
            provider.CreateUninitializedItem(HttpContext, Id, 7);
            
            // Check added item.
            res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(res);
            Assert.IsNull(lockId);
            Assert.AreEqual(7, res.Timeout);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // Lock and update.
            res = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(res);
            Assert.IsNotNull(lockId);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);
            provider.SetAndReleaseItemExclusive(HttpContext, Id, UpdateStoreData(res), lockId, false);

            // Not locked, item present.
            res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            CheckStoreData(res);
            Assert.IsNull(lockId);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // Lock item.
            res = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            CheckStoreData(res);
            Assert.IsNotNull(lockId);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // Try to get it in a different thread.
            Task.Factory.StartNew(() =>
            {
                object lockId1;   // do not overwrite lockId
                res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId1, out actions);
                Assert.IsNull(res);
                Assert.IsNotNull(lockId1);
                Assert.IsTrue(locked);
                Assert.Greater(lockAge, TimeSpan.Zero);
                Assert.AreEqual(SessionStateActions.None, actions);
            }).Wait();

            // Try to get it in a different thread.
            Task.Factory.StartNew(() =>
            {
                object lockId1;   // do not overwrite lockId
                res = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge, out lockId1, out actions);
                Assert.IsNull(res);
                Assert.IsNotNull(lockId1);
                Assert.IsTrue(locked);
                Assert.Greater(lockAge, TimeSpan.Zero);
                Assert.AreEqual(SessionStateActions.None, actions);
            }).Wait();

            // Release item.
            provider.ReleaseItemExclusive(HttpContext, Id, lockId);

            // Make sure it is accessible in a different thread.
            Task.Factory.StartNew(() =>
            {
                res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
                Assert.IsNotNull(res);
                Assert.IsFalse(locked);
                Assert.AreEqual(TimeSpan.Zero, lockAge);
                Assert.AreEqual(SessionStateActions.None, actions);
            }).Wait();

            // Remove item.
            provider.RemoveItem(HttpContext, Id, lockId, null);

            // Check removal.
            res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);
        }

        /// <summary>
        /// Tests the create new store data.
        /// </summary>
        [Test]
        public void TestCreateNewStoreData()
        {
            var provider = GetProvider();

            var data = provider.CreateNewStoreData(HttpContext, 56);

            Assert.AreEqual(56, data.Timeout);
            Assert.IsEmpty(data.Items);
            Assert.IsEmpty(data.StaticObjects);

            // Check that caches are empty.
            var ignite = Ignition.GetIgnite(GridName);
            Assert.IsFalse(ignite.GetCacheNames().SelectMany(x => ignite.GetCache<int, int>(x)).Any());
        }

        /// <summary>
        /// Tests the expiry.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]  // Minimum expiration is 1 minute
        public void TestExpiry()
        {
            var provider = GetProvider();

            bool locked;
            TimeSpan lockAge;
            object lockId;
            SessionStateActions actions;

            // Callbacks are not supported for now.
            Assert.IsFalse(GetProvider().SetItemExpireCallback(null));

            // Check there is no item.
            var res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);

            // Put an item with CreateUninitializedItem and check.
            provider.CreateUninitializedItem(HttpContext, Id, 1);
            CheckExpiry(provider);

            // Put an item with SetAndReleaseItemExclusive and check.
            var data = provider.CreateNewStoreData(HttpContext, 1);
            provider.SetAndReleaseItemExclusive(HttpContext, Id, data, lockId, true);
            CheckExpiry(provider);
        }

        /// <summary>
        /// Checks item expiration.
        /// </summary>
        private static void CheckExpiry(SessionStateStoreProviderBase provider)
        {
            bool locked;
            TimeSpan lockAge;
            object lockId;
            SessionStateActions actions;

            // Check that item is present.
            var res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(res);

            // Wait a minute and check again.
            Thread.Sleep(TimeSpan.FromMinutes(1.05));

            res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);
        }

        /// <summary>
        /// Tests the create uninitialized item.
        /// </summary>
        [Test]
        public void TestCreateUninitializedItem()
        {
            bool locked;
            TimeSpan lockAge;
            object lockId;
            SessionStateActions actions;

            var provider = GetProvider();
            provider.CreateUninitializedItem(HttpContext, "myId", 45);

            var res = provider.GetItem(HttpContext, "myId", out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(res);
            Assert.AreEqual(45, res.Timeout);
            Assert.AreEqual(0, res.Items.Count);
            Assert.AreEqual(0, res.StaticObjects.Count);
        }

        /// <summary>
        /// Updates the store data.
        /// </summary>
        private static SessionStateStoreData UpdateStoreData(SessionStateStoreData data)
        {
            data.Timeout = 8;

            data.Items["name1"] = 1;
            data.Items["name2"] = "2";

            var statics = data.StaticObjects;

            // Modification method is internal.
            statics.GetType().GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(statics, new object[] {"int", typeof(int), false});

            CheckStoreData(data);

            return data;
        }

        /// <summary>
        /// Checks that store data is the same as <see cref="UpdateStoreData"/> returns.
        /// </summary>
        private static void CheckStoreData(SessionStateStoreData data)
        {
            Assert.IsNotNull(data);

            Assert.AreEqual(8, data.Timeout);

            Assert.AreEqual(1, data.Items["name1"]);
            Assert.AreEqual(1, data.Items[0]);

            Assert.AreEqual("2", data.Items["name2"]);
            Assert.AreEqual("2", data.Items[1]);

            Assert.AreEqual(0, data.StaticObjects["int"]);
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

            provider.InitializeRequest(HttpContext);
            provider.CreateUninitializedItem(HttpContext, Id, 42);

            var data = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(data);
            Assert.AreEqual(42, data.Timeout);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.IsNull(lockId);
            Assert.AreEqual(SessionStateActions.None, actions);

            provider.ResetItemTimeout(HttpContext, Id);
            provider.EndRequest(HttpContext);
            provider.Dispose();
        }
    }
}

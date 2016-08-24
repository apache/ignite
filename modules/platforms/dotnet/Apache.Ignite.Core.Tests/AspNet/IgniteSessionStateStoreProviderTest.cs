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
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using System.Web;
    using System.Web.SessionState;
    using Apache.Ignite.AspNet;
    using Apache.Ignite.Core.Cache;
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

        /** Section name XML config attribute. */
        private const string SectionNameAttr = "igniteConfigurationSectionName";

        /** Grid name. */
        private const string GridName = "grid1";

        /** Cache name. */
        private const string CacheName = "myCache";

        /** Session id. */
        private const string Id = "1";

        /** Test context */
        private static readonly HttpContext HttpContext = 
            new HttpContext(new HttpRequest(null, "http://tempuri.org", null), new HttpResponse(null));

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


            // Not initialized
            Assert.Throws<InvalidOperationException>(() =>
                    stateProvider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions));

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
            bool locked;
            TimeSpan lockAge;
            object lockId;
            SessionStateActions actions;

            var provider = GetProvider();

            // Not locked, no item.
            var res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);
            Assert.Throws<ObjectDisposedException>(() => ((ICacheLock) lockId).Enter());

            // Add item.
            res = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNull(res);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            provider.SetAndReleaseItemExclusive(HttpContext, Id, CreateStoreData(), lockId, true);

            // Not locked, item present.
            res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            CheckStoreData(res);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);
            Assert.Throws<ObjectDisposedException>(() => ((ICacheLock)lockId).Enter());

            // Lock item.
            res = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            CheckStoreData(res);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.AreEqual(SessionStateActions.None, actions);

            // Try to get it in a different thread.
            Task.Factory.StartNew(() =>
            {
                object lockId1;   // do not overwrite lockId
                res = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId1, out actions);
                Assert.IsNull(res);
                Assert.IsNull(lockId1);
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
        }

        /// <summary>
        /// Creates the store data.
        /// </summary>
        private static SessionStateStoreData CreateStoreData()
        {
            var items = new SessionStateItemCollection();

            items["name1"] = 1;
            items["name2"] = "2";

            var statics = new HttpStaticObjectsCollection();

            // Modification method is internal.
            statics.GetType().GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(statics, new object[] {"int", typeof(int), false});

            var data = new SessionStateStoreData(items, statics, 8);

            CheckStoreData(data);

            return data;
        }

        /// <summary>
        /// Checks that store data is the same as <see cref="CreateStoreData"/> returns.
        /// </summary>
        private static void CheckStoreData(SessionStateStoreData data)
        {
            Assert.IsNotNull(data);

            Assert.AreEqual(8, data.Timeout);

            Assert.AreEqual(1, data.Items["name1"]);
            Assert.AreEqual("2", data.Items["name2"]);

            Assert.AreEqual(0, data.StaticObjects["int"]);
        }

        /// <summary>
        /// Tests the store data serializer.
        /// </summary>
        [Test]
        public void TestStoreDataSerializer()
        {
            // TODO
        }

        /// <summary>
        /// Tests the expiry.
        /// </summary>
        [Test]
        public void TestExpiry()
        {
            // TODO
            Assert.IsFalse(GetProvider().SetItemExpireCallback(null));
        }

        /// <summary>
        /// Tests the locking.
        /// </summary>
        [Test]
        public void TestLocking()
        {
            // TODO: ?
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

            var data = provider.GetItemExclusive(HttpContext, Id, out locked, out lockAge,
                out lockId, out actions);
            Assert.IsNull(data);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.IsNotNull(lockId);
            Assert.AreEqual(SessionStateActions.None, actions);

            data = provider.CreateNewStoreData(HttpContext, 42);
            Assert.IsNotNull(data);

            provider.SetAndReleaseItemExclusive(HttpContext, Id, data, lockId, false);

            data = provider.GetItem(HttpContext, Id, out locked, out lockAge, out lockId, out actions);
            Assert.IsNotNull(data);
            Assert.AreEqual(42, data.Timeout);
            Assert.IsFalse(locked);
            Assert.AreEqual(TimeSpan.Zero, lockAge);
            Assert.IsNotNull(lockId);
            Assert.AreEqual(SessionStateActions.None, actions);

            provider.ResetItemTimeout(HttpContext, Id);
            provider.EndRequest(HttpContext);
            provider.Dispose();
        }
    }
}

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
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    ///
    /// </summary>
    class Key
    {
        private readonly int _idx;

        public Key(int idx)
        {
            _idx = idx;
        }

        public int Index()
        {
            return _idx;
        }

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != GetType())
                return false;

            Key key = (Key)obj;

            return key._idx == _idx;
        }

        public override int GetHashCode()
        {
            return _idx;
        }
    }

    /// <summary>
    ///
    /// </summary>
    class Value
    {
        private int _idx;

        public Value(int idx)
        {
            _idx = idx;
        }

        public int Index()
        {
            return _idx;
        }
    }

    /// <summary>
    /// Cache entry predicate.
    /// </summary>
    [Serializable]
    public class CacheEntryFilter : ICacheEntryFilter<int, string>
    {
        /** <inheritdoc /> */
        public bool Invoke(ICacheEntry<int, string> entry)
        {
            return entry.Key >= 105;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class CacheStoreTest
    {
        /** */
        private const string PortableStoreCacheName = "portable_store";

        /** */
        private const string ObjectStoreCacheName = "object_store";

        /** */
        private const string CustomStoreCacheName = "custom_store";

        /** */
        private const string TemplateStoreCacheName = "template_store*";

        /// <summary>
        ///
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTests()
        {
            //TestUtils.JVM_DEBUG = true;

            TestUtils.KillProcesses();

            TestUtils.JvmDebug = true;

            IgniteConfigurationEx cfg = new IgniteConfigurationEx();

            cfg.GridName = GridName();
            cfg.JvmClasspath = TestUtils.CreateTestClasspath();
            cfg.JvmOptions = TestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = "config\\native-client-test-cache-store.xml";

            PortableConfiguration portCfg = new PortableConfiguration();

            portCfg.Types = new List<string> { typeof(Key).FullName, typeof(Value).FullName };

            cfg.PortableConfiguration = portCfg;

            Ignition.Start(cfg);
        }

        /// <summary>
        ///
        /// </summary>
        [TestFixtureTearDown]
        public virtual void AfterTests()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        ///
        /// </summary>
        [SetUp]
        public void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        ///
        /// </summary>
        [TearDown]
        public void AfterTest()
        {
            var cache = Cache();

            cache.Clear();

            Assert.IsTrue(cache.IsEmpty, "Cache is not empty: " + cache.Size());

            CacheTestStore.Reset();

            Console.WriteLine("Test finished: " + TestContext.CurrentContext.Test.Name);
        }

        [Test]
        public void TestLoadCache()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Size());

            cache.LoadCache(new CacheEntryFilter(), 100, 10);

            Assert.AreEqual(5, cache.Size());

            for (int i = 105; i < 110; i++)
                Assert.AreEqual("val_" + i, cache.Get(i));
        }

        [Test]
        public void TestLocalLoadCache()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Size());

            cache.LocalLoadCache(new CacheEntryFilter(), 100, 10);

            Assert.AreEqual(5, cache.Size());

            for (int i = 105; i < 110; i++)
                Assert.AreEqual("val_" + i, cache.Get(i));
        }

        [Test]
        public void TestLoadCacheMetadata()
        {
            CacheTestStore.LoadObjects = true;

            var cache = Cache();

            Assert.AreEqual(0, cache.Size());

            cache.LocalLoadCache(null, 0, 3);

            Assert.AreEqual(3, cache.Size());

            var meta = cache.WithKeepPortable<Key, IPortableObject>().Get(new Key(0)).Metadata();

            Assert.NotNull(meta);

            Assert.AreEqual("Value", meta.TypeName);
        }

        [Test]
        public void TestLoadCacheAsync()
        {
            var cache = Cache().WithAsync();

            Assert.AreEqual(0, cache.Size());

            cache.LocalLoadCache(new CacheEntryFilter(), 100, 10);

            var fut = cache.GetFuture<object>();

            fut.Get();

            Assert.IsTrue(fut.IsDone);

            cache.Size();
            Assert.AreEqual(5, cache.GetFuture<int>().ToTask().Result);

            for (int i = 105; i < 110; i++)
            {
                cache.Get(i);

                Assert.AreEqual("val_" + i, cache.GetFuture<string>().ToTask().Result);
            }
        }

        [Test]
        public void TestPutLoad()
        {
            var cache = Cache();

            cache.Put(1, "val");

            IDictionary map = StoreMap();

            Assert.AreEqual(1, map.Count);

            cache.LocalEvict(new[] { 1 });

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual("val", cache.Get(1));

            Assert.AreEqual(1, cache.Size());
        }

        [Test]
        public void TestPutLoadPortables()
        {
            var cache = PortableStoreCache<int, Value>();

            cache.Put(1, new Value(1));

            IDictionary map = StoreMap();

            Assert.AreEqual(1, map.Count);

            IPortableObject v = (IPortableObject)map[1];

            Assert.AreEqual(1, v.Field<int>("_idx"));

            cache.LocalEvict(new[] { 1 });

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(1, cache.Get(1).Index());

            Assert.AreEqual(1, cache.Size());
        }

        [Test]
        public void TestPutLoadObjects()
        {
            var cache = ObjectStoreCache<int, Value>();

            cache.Put(1, new Value(1));

            IDictionary map = StoreMap();

            Assert.AreEqual(1, map.Count);

            Value v = (Value)map[1];

            Assert.AreEqual(1, v.Index());

            cache.LocalEvict(new[] { 1 });

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(1, cache.Get(1).Index());

            Assert.AreEqual(1, cache.Size());
        }

        [Test]
        public void TestPutLoadAll()
        {
            var putMap = new Dictionary<int, string>();

            for (int i = 0; i < 10; i++)
                putMap.Add(i, "val_" + i);

            var cache = Cache();

            cache.PutAll(putMap);

            IDictionary map = StoreMap();

            Assert.AreEqual(10, map.Count);

            for (int i = 0; i < 10; i++)
                Assert.AreEqual("val_" + i, map[i]);

            cache.Clear();

            Assert.AreEqual(0, cache.Size());

            ICollection<int> keys = new List<int>();

            for (int i = 0; i < 10; i++)
                keys.Add(i);

            IDictionary<int, string> loaded = cache.GetAll(keys);

            Assert.AreEqual(10, loaded.Count);

            for (int i = 0; i < 10; i++)
                Assert.AreEqual("val_" + i, loaded[i]);

            Assert.AreEqual(10, cache.Size());
        }

        [Test]
        public void TestRemove()
        {
            var cache = Cache();

            for (int i = 0; i < 10; i++)
                cache.Put(i, "val_" + i);

            IDictionary map = StoreMap();

            Assert.AreEqual(10, map.Count);

            for (int i = 0; i < 5; i++)
                cache.Remove(i);

            Assert.AreEqual(5, map.Count);

            for (int i = 5; i < 10; i++)
                Assert.AreEqual("val_" + i, map[i]);
        }

        [Test]
        public void TestRemoveAll()
        {
            var cache = Cache();

            for (int i = 0; i < 10; i++)
                cache.Put(i, "val_" + i);

            IDictionary map = StoreMap();

            Assert.AreEqual(10, map.Count);

            cache.RemoveAll(new List<int> { 0, 1, 2, 3, 4 });

            Assert.AreEqual(5, map.Count);

            for (int i = 5; i < 10; i++)
                Assert.AreEqual("val_" + i, map[i]);
        }

        [Test]
        public void TestTx()
        {
            var cache = Cache();

            using (var tx = cache.Ignite.Transactions.TxStart())
            {
                CacheTestStore.ExpCommit = true;

                tx.AddMeta("meta", 100);

                cache.Put(1, "val");

                tx.Commit();
            }

            IDictionary map = StoreMap();

            Assert.AreEqual(1, map.Count);

            Assert.AreEqual("val", map[1]);
        }

        [Test]
        public void TestLoadCacheMultithreaded()
        {
            CacheTestStore.LoadMultithreaded = true;

            var cache = Cache();

            Assert.AreEqual(0, cache.Size());

            cache.LocalLoadCache(null, 0, null);

            Assert.AreEqual(1000, cache.Size());

            for (int i = 0; i < 1000; i++)
                Assert.AreEqual("val_" + i, cache.Get(i));
        }

        [Test]
        public void TestCustomStoreProperties()
        {
            var cache = CustomStoreCache();
            Assert.IsNotNull(cache);

            Assert.AreEqual(42, CacheTestStore.intProperty);
            Assert.AreEqual("String value", CacheTestStore.stringProperty);
        }

        [Test]
        public void TestDynamicStoreStart()
        {
            var cache = TemplateStoreCache();

            Assert.IsNotNull(cache);

            cache.Put(1, cache.Name);

            Assert.AreEqual(cache.Name, CacheTestStore.Map[1]);
        }

        /// <summary>
        /// Get's grid name for this test.
        /// </summary>
        /// <returns>Grid name.</returns>
        protected virtual string GridName()
        {
            return null;
        }

        private IDictionary StoreMap()
        {
            return CacheTestStore.Map;
        }

        private ICache<int, string> Cache()
        {
            return PortableStoreCache<int, string>();
        }

        private ICache<TK, TV> PortableStoreCache<TK, TV>()
        {
            return Ignition.GetIgnite(GridName()).Cache<TK, TV>(PortableStoreCacheName);
        }

        private ICache<TK, TV> ObjectStoreCache<TK, TV>()
        {
            return Ignition.GetIgnite(GridName()).Cache<TK, TV>(ObjectStoreCacheName);
        }

        private ICache<int, string> CustomStoreCache()
        {
            return Ignition.GetIgnite(GridName()).Cache<int, string>(CustomStoreCacheName);
        }

        private ICache<int, string> TemplateStoreCache()
        {
            var cacheName = TemplateStoreCacheName.Replace("*", Guid.NewGuid().ToString());
            
            return Ignition.GetIgnite(GridName()).GetOrCreateCache<int, string>(cacheName);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class NamedNodeCacheStoreTest : CacheStoreTest
    {
        /** <inheritDoc /> */
        protected override string GridName()
        {
            return "name";
        }
    }
}

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

// ReSharper disable MissingSerializationAttribute
namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Tests.Query;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Base cache test.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedVariable")]
    public abstract class CacheAbstractTest
    {
        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void StartGrids()
        {
            IgniteConfiguration cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(
                    typeof(BinarizablePerson),
                    typeof(CacheTestKey),
                    typeof(TestReferenceObject),
                    typeof(BinarizableAddArgCacheEntryProcessor),
                    typeof(BinarizableTestException)),
                SpringConfigUrl = "Config\\native-client-test-cache.xml"
            };

            for (int i = 0; i < GridCount(); i++)
            {
                cfg.IgniteInstanceName = "grid-" + i;

                Ignition.Start(cfg);
            }

            Assert.AreEqual(GridCount(), GetIgnite(0).GetCluster().GetNodes().Count);
        }

        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void StopGrids()
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
        public void AfterTest() {
            for (int i = 0; i < GridCount(); i++)
                Cache(i).WithKeepBinary<object, object>().RemoveAll();

            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);
                var entries = cache.Select(pair => pair + GetKeyAffinity(cache, pair.Key)).ToArray();

                if (entries.Any())
                    Assert.Fail("Cache '{0}' is not empty in grid [{1}]: ({2})", CacheName(), i,
                        entries.Aggregate((acc, val) => string.Format("{0}, {1}", acc, val)));

                var size = cache.GetSize();
                Assert.AreEqual(0, size,
                    "Cache enumerator returned no entries, but cache '{0}' size is {1} in grid [{2}]",
                    CacheName(), size, i);
            }

            Console.WriteLine("Test finished: " + TestContext.CurrentContext.Test.Name);
        }

        protected static IIgnite GetIgnite(int idx)
        {
            return Ignition.GetIgnite("grid-" + idx);
        }

        protected ICache<int, int> Cache(int idx, bool async = false) 
        {
            return Cache<int, int>(idx, async);
        }

        private ICache<TK, TV> Cache<TK, TV>(int idx, bool async = false) {
            var cache = GetIgnite(idx).GetCache<TK, TV>(CacheName());

            return async ? cache.WrapAsync() : cache;
        }

        protected ICache<int, int> Cache(bool async = false)
        {
            return Cache<int, int>(0, async);
        }

        private ICache<TK, TV> Cache<TK, TV>(bool async = false)
        {
            return Cache<TK, TV>(0, async);
        }

        private ICacheAffinity Affinity()
        {
            return GetIgnite(0).GetAffinity(CacheName());
        }

        protected virtual ITransactions Transactions
        {
            get { return GetIgnite(0).GetTransactions(); }
        }

        [Test]
        public void TestCircularReference()
        {
            var cache = Cache().WithKeepBinary<int, object>();

            TestReferenceObject obj1 = new TestReferenceObject();

            obj1.Obj = new TestReferenceObject(obj1);

            cache.Put(1, obj1);

            var po = (IBinaryObject) cache.Get(1);

            Assert.IsNotNull(po);

            TestReferenceObject objRef = po.Deserialize<TestReferenceObject>();

            Assert.IsNotNull(objRef);
        }

        [Test]
        public void TestName()
        {
            for (int i = 0; i < GridCount(); i++ )
                Assert.AreEqual(CacheName(), Cache(i).Name);
        }

        [Test]
        public void TestIsEmpty()
        {
            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                Assert.IsTrue(cache.IsEmpty());
            }

            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                cache.Put(GetPrimaryKeyForCache(cache), 1);
            }

            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                Assert.IsFalse(cache.IsEmpty());
            }
        }

        [Test]
        public void TestContainsKey()
        {
            var cache = Cache();

            int key = GetPrimaryKeyForCache(cache);

            cache.Put(key, 1);

            Assert.IsTrue(cache.ContainsKey(key));
            Assert.IsFalse(cache.ContainsKey(-1));
        }

        [Test]
        public void TestContainsKeys()
        {
            var cache = Cache();

            var keys = GetPrimaryKeysForCache(cache, 5);

            Assert.IsFalse(cache.ContainsKeys(keys));

            cache.PutAll(keys.ToDictionary(k => k, k => k));

            Assert.IsTrue(cache.ContainsKeys(keys));

            Assert.IsFalse(cache.ContainsKeys(keys.Concat(new[] {int.MaxValue})));
        }

        [Test]
        public void TestPeek()
        {
            var cache = Cache();

            int key1 = GetPrimaryKeyForCache(cache);

            cache.Put(key1, 1);

            int val;

            Assert.AreEqual(1, cache.LocalPeek(key1));
            Assert.Throws<KeyNotFoundException>(() => cache.LocalPeek(-1));
            Assert.IsFalse(cache.TryLocalPeek(-1, out val));

            Assert.AreEqual(1, cache.LocalPeek(key1, CachePeekMode.All));
            Assert.Throws<KeyNotFoundException>(() => cache.LocalPeek(-1, CachePeekMode.All));
            Assert.AreEqual(false, cache.TryLocalPeek(-1, out val, CachePeekMode.All));
        }

        [Test]
        public void TestGet()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            Assert.Throws<KeyNotFoundException>(() => cache.Get(3));

            int value;

            Assert.IsTrue(cache.TryGet(1, out value));
            Assert.AreEqual(1, value);

            Assert.IsTrue(cache.TryGet(2, out value));
            Assert.AreEqual(2, value);

            Assert.IsFalse(cache.TryGet(3, out value));
            Assert.AreEqual(0, value);
        }

        [Test]
        public void TestGetAsync()
        {
            var cache = Cache().WrapAsync();

            cache.Put(1, 1);
            cache.Put(2, 2);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.IsFalse(cache.ContainsKey(3));
            Assert.Throws<KeyNotFoundException>(() => cache.Get(3));

            Assert.AreEqual(1, Cache().TryGetAsync(1).Result.Value);
            Assert.IsFalse(Cache().TryGetAsync(3).Result.Success);
        }

        [Test]
        public void TestGetAll()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);
            cache.Put(4, 4);
            cache.Put(5, 5);

            var map = cache.GetAll(new List<int> {0, 1, 2, 5}).ToDictionary(x => x.Key, x => x.Value);

            Assert.AreEqual(3, map.Count);

            Assert.AreEqual(1, map[1]);
            Assert.AreEqual(2, map[2]);
        }

        [Test]
        public void TestGetAllAsync()
        {
            var cache = Cache().WrapAsync();

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);

            var map = cache.GetAll(new List<int> {0, 1, 2}).ToDictionary(x => x.Key, x => x.Value);

            Assert.AreEqual(2, map.Count);

            Assert.AreEqual(1, map[1]);
            Assert.AreEqual(2, map[2]);
        }

        [Test]
        public void TestGetAndPut()
        {
            var cache = Cache();

            Assert.AreEqual(false, cache.ContainsKey(1));

            var old = cache.GetAndPut(1, 1);

            Assert.IsFalse(old.Success);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndPut(1, 2);

            Assert.AreEqual(1, old.Value);

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestGetAndReplace()
        {
            var cache = Cache();

            cache.Put(1, 10);

            Assert.AreEqual(10, cache.GetAndReplace(1, 100).Value);

            Assert.AreEqual(false, cache.GetAndReplace(2, 2).Success);

            Assert.AreEqual(false, cache.ContainsKey(2));

            Assert.AreEqual(100, cache.Get(1));

            Assert.IsTrue(cache.Remove(1));
        }

        [Test]
        public void TestGetAndRemove()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.GetAndRemove(0).Success);

            Assert.AreEqual(1, cache.GetAndRemove(1).Value);

            Assert.IsFalse(cache.GetAndRemove(1).Success);

            Assert.IsFalse(cache.ContainsKey(1));
        }

        [Test]
        public void TestGetAndPutAsync()
        {
            var cache = Cache().WrapAsync();

            Assert.AreEqual(false, cache.ContainsKey(1));

            var old = cache.GetAndPut(1, 1);

            Assert.IsFalse(old.Success);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndPut(1, 2);

            Assert.AreEqual(1, old.Value);

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestPut([Values(true, false)] bool async)
        {
            var cache = Cache(async);

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            // Objects.
            var cache2 = Cache<Container, Container>(async);

            var obj1 = new Container {Id = 1};
            var obj2 = new Container {Id = 2};

            obj1.Inner = obj2;
            obj2.Inner = obj1;

            cache2[obj1] = obj2;

            Assert.AreEqual(2, cache2[obj1].Id);
        }

        [Test]
        public void TestGetAndPutIfAbsent()
        {
            var cache = Cache();

            Assert.IsFalse(cache.ContainsKey(1));

            Assert.IsFalse(cache.GetAndPutIfAbsent(1, 1).Success);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(1, cache.GetAndPutIfAbsent(1, 2).Value);

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestGetAndPutIfAbsentAsync()
        {
            var cache = Cache(true);

            Assert.IsFalse(cache.ContainsKey(1));

            var old = cache.GetAndPutIfAbsent(1, 1);

            Assert.IsFalse(old.Success);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndPutIfAbsent(1, 2);

            Assert.AreEqual(1, old.Value);

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestPutIfAbsent([Values(true, false)] bool async)
        {
            var cache = Cache(async);

            Assert.Throws<KeyNotFoundException>(() => cache.Get(1));
            Assert.IsFalse(cache.ContainsKey(1));

            Assert.IsTrue(cache.PutIfAbsent(1, 1));

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.PutIfAbsent(1, 2));

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestReplace()
        {
            var cache = Cache();

            Assert.IsFalse(cache.ContainsKey(1));

            bool success = cache.Replace(1, 1);

            Assert.AreEqual(false, success);

            Assert.IsFalse(cache.ContainsKey(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            success = cache.Replace(1, 2);

            Assert.AreEqual(true, success);

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsFalse(cache.Replace(1, -1, 3));

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2, 3));

            Assert.AreEqual(3, cache.Get(1));
        }

        [Test]
        [Ignore("IGNITE-7072")]
        public void TestReplaceBinary()
        {
            var cache = Cache<object, object>();
            var key = new {Foo = "bar"};
            var val = new {Bar = "baz", Id = 1};
            var val2 = new {Bar = "baz2", Id = 2};
            var val3 = new {Bar = "baz3", Id = 3};

            Assert.IsFalse(cache.ContainsKey(key));
            Assert.AreEqual(false, cache.Replace(key, val));
            Assert.IsFalse(cache.ContainsKey(key));

            cache.Put(key, val);
            Assert.AreEqual(val, cache.Get(key));
            Assert.IsTrue(cache.Replace(key, val2));
            Assert.AreEqual(val2, cache.Get(key));

            Assert.IsFalse(cache.Replace(key, -1, 3));
            Assert.AreEqual(val2, cache.Get(key));

            Assert.IsTrue(cache.Replace(key, val2, val3));
            Assert.AreEqual(val3, cache.Get(key));
        }

        [Test]
        public void TestGetAndReplaceAsync()
        {
            var cache = Cache().WrapAsync();

            Assert.IsFalse(cache.ContainsKey(1));

            var old = cache.GetAndReplace(1, 1);

            Assert.IsFalse(old.Success);

            Assert.IsFalse(cache.ContainsKey(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndReplace(1, 2);

            Assert.AreEqual(1, old.Value);

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsFalse(cache.Replace(1, -1, 3));

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2, 3));

            Assert.AreEqual(3, cache.Get(1));
        }

        [Test]
        public void TestReplacex()
        {
            var cache = Cache();

            Assert.IsFalse(cache.ContainsKey(1));

            Assert.IsFalse(cache.Replace(1, 1));

            Assert.IsFalse(cache.ContainsKey(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2));

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestReplaceAsync()
        {
            var cache = Cache().WrapAsync();

            Assert.IsFalse(cache.ContainsKey(1));

            Assert.IsFalse(cache.Replace(1, 1));

            Assert.IsFalse(cache.ContainsKey(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2));

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestPutAll([Values(true, false)] bool async)
        {
            var cache = Cache(async);

            // Primitives.
            cache.PutAll(new Dictionary<int, int> { { 1, 1 }, { 2, 2 }, { 3, 3 } });

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));

            // Objects.
            var cache2 = Cache<int, Container>(async);

            var obj1 = new Container();
            var obj2 = new Container();
            var obj3 = new Container();

            obj1.Inner = obj2;
            obj2.Inner = obj1;
            obj3.Inner = obj2;

            cache2.PutAll(new Dictionary<int, Container>
            {
                {1, obj1},
                {2, obj2},
                {3, obj3}
            });

            var res1 = cache2[1];
            var res2 = cache2[2];
            var res3 = cache2[3];

            Assert.AreEqual(res1, res1.Inner.Inner);
            Assert.AreEqual(res2, res2.Inner.Inner);
            Assert.IsNotNull(res3.Inner.Inner.Inner);
        }

        /// <summary>
        /// Expiry policy tests.
        /// </summary>
        [Test]
        [Ignore("https://issues.apache.org/jira/browse/IGNITE-8983")]
        public void TestWithExpiryPolicy()
        {
            TestWithExpiryPolicy((cache, policy) => cache.WithExpiryPolicy(policy), true);
        }

        /// <summary>
        /// Expiry policy tests.
        /// </summary>
        [Test]
        public void TestCacheConfigurationExpiryPolicy()
        {
            TestWithExpiryPolicy((cache, policy) =>
            {
                var cfg = cache.GetConfiguration();

                cfg.Name = string.Format("expiryPolicyCache_{0}_{1}", GetType().Name, policy.GetHashCode());
                cfg.ExpiryPolicyFactory = new ExpiryPolicyFactory(policy);

                return cache.Ignite.CreateCache<int, int>(cfg);
            }, false);
        }

        /// <summary>
        /// Expiry policy tests.
        /// </summary>
        private void TestWithExpiryPolicy(Func<ICache<int, int>, IExpiryPolicy, ICache<int, int>> withPolicyFunc,
            bool origCache)
        {
            ICache<int, int> cache0 = Cache(0);

            int key0;
            int key1;

            if (LocalCache())
            {
                key0 = 0;
                key1 = 1;
            }
            else
            {
                key0 = GetPrimaryKeyForCache(cache0);
                key1 = GetPrimaryKeyForCache(Cache(1));
            }

            // Test unchanged expiration.
            ICache<int, int> cache = withPolicyFunc(cache0, new ExpiryPolicy(null, null, null));
            cache0 = origCache ? cache0 : cache;

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Get(key0);
            cache.Get(key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test eternal expiration.
            cache = withPolicyFunc(cache0, new ExpiryPolicy(TimeSpan.MaxValue, TimeSpan.MaxValue, TimeSpan.MaxValue));
            cache0 = origCache ? cache0 : cache;

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Get(key0);
            cache.Get(key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test regular expiration.
            cache = withPolicyFunc(cache0, new ExpiryPolicy(TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100)));
            cache0 = origCache ? cache0 : cache;

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            Thread.Sleep(200);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            Thread.Sleep(200);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            // Test sliding expiration
            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            for (var i = 0; i < 3; i++)
            {
                Thread.Sleep(50);

                // Prolong expiration by touching the entry
                cache.Get(key0);
                cache.Get(key1);
            }
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            Thread.Sleep(200);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));
        }

        /// <summary>
        /// Expiry policy tests for zero and negative expiry values.
        /// </summary>
        [Test]
        [Ignore("IGNITE-1423")]
        public void TestWithExpiryPolicyZeroNegative()
        {
            ICache<int, int> cache0 = Cache(0);

            int key0;
            int key1;

            if (LocalCache())
            {
                key0 = 0;
                key1 = 1;
            }
            else
            {
                key0 = GetPrimaryKeyForCache(cache0);
                key1 = GetPrimaryKeyForCache(Cache(1));
            }

            // Test zero expiration.
            var cache = cache0.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Get(key0);
            cache.Get(key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test negative expiration.
            cache = cache0.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromMilliseconds(-100),
                TimeSpan.FromMilliseconds(-100), TimeSpan.FromMilliseconds(-100)));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Get(key0);
            cache.Get(key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });
        }

        [Test]
        [Ignore("IGNITE-4535")]
        public void TestEvict()
        {
            var cache = Cache();

            int key = GetPrimaryKeyForCache(cache);

            cache.Put(key, 1);

            Assert.AreEqual(1, PeekInt(cache, key));

            cache.LocalEvict(new[] {key});

            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(0, PeekInt(cache, key));

            Assert.AreEqual(1, cache.Get(key));

            Assert.AreEqual(1, cache.GetLocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(1, PeekInt(cache, key));
        }

        [Test]
        [Ignore("IGNITE-4535")]
        public void TestEvictAllKeys()
        {
            var cache = Cache();

            List<int> keys = GetPrimaryKeysForCache(cache, 3);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);
            cache.Put(keys[2], 3);

            Assert.AreEqual(1, PeekInt(cache, keys[0]));
            Assert.AreEqual(2, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));

            cache.LocalEvict(new List<int> { -1, keys[0], keys[1] });

            Assert.AreEqual(1, cache.GetLocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(0, PeekInt(cache, keys[0]));
            Assert.AreEqual(0, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));

            Assert.AreEqual(1, cache.Get(keys[0]));
            Assert.AreEqual(2, cache.Get(keys[1]));

            Assert.AreEqual(3, cache.GetLocalSize());

            Assert.AreEqual(1, PeekInt(cache, keys[0]));
            Assert.AreEqual(2, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));
        }

        [Test]
        public void TestClear()
        {
            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                cache.Put(GetPrimaryKeyForCache(cache, 500), 1);

                Assert.IsFalse(cache.IsEmpty());
            }

            Cache().Clear();

            for (int i = 0; i < GridCount(); i++)
                Assert.IsTrue(Cache(i).IsEmpty());
        }

        [Test]
        public void TestClearKey()
        {
            var cache = Cache();
            var keys = GetPrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            var i = cache.GetSize();

            foreach (var key in keys)
            {
                cache.Clear(key);

                Assert.IsFalse(cache.ContainsKey(key));
                Assert.Throws<KeyNotFoundException>(() => cache.Get(key));

                Assert.Less(cache.GetSize(), i);

                i = cache.GetSize();
            }
        }

        [Test]
        public void TestClearKeys()
        {
            var cache = Cache();
            var keys = GetPrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            cache.ClearAll(keys);

            Assert.IsFalse(cache.ContainsKeys(keys));
        }

        [Test]
        public void TestLocalClearKey()
        {
            var cache = Cache();
            var keys = GetPrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            var i = cache.GetSize();

            foreach (var key in keys)
            {
                cache.LocalClear(key);

                int val;
                Assert.IsFalse(cache.TryLocalPeek(key, out val));

                Assert.Less(cache.GetSize(), i);

                i = cache.GetSize();
            }

            cache.Clear();
        }

        [Test]
        public void TestLocalClearKeys()
        {
            var cache = Cache();
            var keys = GetPrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            cache.LocalClearAll(keys);

            int val;

            foreach (var key in keys)
                Assert.IsFalse(cache.TryLocalPeek(key, out val));

            cache.Clear();
        }

        [Test]
        public void TestRemove()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(true, cache.Remove(1));

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKey(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(1, -1));
            Assert.IsTrue(cache.Remove(1, 1));

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKey(1));
        }

        [Test]
        public void TestGetAndRemoveAsync()
        {
            var cache = Cache().WrapAsync();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(1, cache.GetAndRemove(1).Value);

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKey(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(1, -1));
            Assert.IsTrue(cache.Remove(1, 1));

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKey(1));
        }

        [Test]
        public void TestRemovex()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(-1));
            Assert.IsTrue(cache.Remove(1));

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKey(1));
        }

        [Test]
        public void TestRemoveAsync()
        {
            var cache = Cache().WrapAsync();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(-1));
            Assert.IsTrue(cache.Remove(1));

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKey(1));
        }

        [Test]
        public void TestRemoveAll()
        {
            var cache = Cache();

            var keys = GetPrimaryKeysForCache(cache, 2);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);

            Assert.AreEqual(1, cache.Get(keys[0]));
            Assert.AreEqual(2, cache.Get(keys[1]));

            cache.RemoveAll();

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKeys(keys));
        }

        [Test]
        public void TestRemoveAllAsync()
        {
            var cache = Cache().WrapAsync();

            List<int> keys = GetPrimaryKeysForCache(cache, 2);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);

            Assert.AreEqual(1, cache.Get(keys[0]));
            Assert.AreEqual(2, cache.Get(keys[1]));

            cache.RemoveAll();

            Assert.AreEqual(0, cache.GetSize());

            Assert.IsFalse(cache.ContainsKeys(keys));
        }

        [Test]
        public void TestRemoveAllKeys()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.GetSize());

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));

            cache.RemoveAll(new List<int> { 0, 1, 2 });

            Assert.AreEqual(1, cache.GetSize(CachePeekMode.Primary));

            Assert.IsFalse(cache.ContainsKeys(new[] {1, 2}));
            Assert.AreEqual(3, cache.Get(3));
        }

        [Test]
        public void TestRemoveAllKeysAsync()
        {
            var cache = Cache().WrapAsync();

            Assert.AreEqual(0, cache.GetSize());

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));

            cache.RemoveAll(new List<int> { 0, 1, 2 });

            Assert.AreEqual(1, cache.GetSize(CachePeekMode.Primary));

            Assert.IsFalse(cache.ContainsKeys(new[] {1, 2}));
            Assert.AreEqual(3, cache.Get(3));
        }

        [Test]
        public void TestSizes([Values(true, false)] bool async)
        {
            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i, async);

                List<int> keys = GetPrimaryKeysForCache(cache, 2);

                foreach (int key in keys)
                    cache.Put(key, 1);

                Assert.IsTrue(cache.GetSize() >= 2);
                Assert.AreEqual(cache.GetSize(), cache.GetSizeLong());
                Assert.AreEqual(2, cache.GetLocalSizeLong(CachePeekMode.Primary));
                Assert.AreEqual(2, cache.GetLocalSize(CachePeekMode.Primary));

                foreach (var key in keys)
                {
                    var p = GetIgnite(i).GetAffinity(cache.Name).GetPartition(key);
                    
                    Assert.GreaterOrEqual(cache.GetSizeLong(p, CachePeekMode.Primary), 1);
                }
            }

            ICache<int, int> cache0 = Cache(async);

            Assert.AreEqual(GridCount() * 2, cache0.GetSize(CachePeekMode.Primary));
            Assert.AreEqual(GridCount() * 2, cache0.GetSizeLong(CachePeekMode.Primary));

            if (!LocalCache() && !ReplicatedCache())
            {
                int nearKey = NearKeyForCache(cache0);

                cache0.Put(nearKey, 1);

                Assert.AreEqual(NearEnabled() ? 1 : 0, cache0.GetSize(CachePeekMode.Near));
                Assert.AreEqual(NearEnabled() ? 1 : 0, cache0.GetSizeLong(CachePeekMode.Near));
            }
        }

        [Test]
        public void TestLocalSize()
        {
            var cache = Cache();
            var keys = GetPrimaryKeysForCache(cache, 3);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);

            var localSize = cache.GetLocalSize();
            Assert.AreEqual(localSize, cache.GetLocalSizeLong());

            cache.LocalEvict(keys.Take(2).ToArray());

            Assert.AreEqual(0, cache.GetLocalSize(CachePeekMode.Onheap));
            Assert.AreEqual(0, cache.GetLocalSizeLong(CachePeekMode.Onheap));
            Assert.AreEqual(localSize, cache.GetLocalSize(CachePeekMode.All));
            Assert.AreEqual(localSize, cache.GetLocalSizeLong(CachePeekMode.All));
            
            cache.Put(keys[2], 3);

            Assert.AreEqual(localSize + 1, cache.GetLocalSize(CachePeekMode.All));
            Assert.AreEqual(localSize + 1, cache.GetLocalSizeLong(CachePeekMode.All));
            
            foreach (var key in keys)
            {
                var p = Affinity().GetPartition(key);
                    
                Assert.GreaterOrEqual(cache.GetLocalSizeLong(p, CachePeekMode.All), 1);
            }

            cache.RemoveAll(keys.Take(2).ToArray());
        }

        /// <summary>
        /// Test enumerators.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public void TestEnumerators()
        {
            var cache = Cache();
            var keys = GetPrimaryKeysForCache(cache, 2);

            cache.Put(keys[0], keys[0] + 1);
            cache.Put(keys[1], keys[1] + 1);

            // Check distributed enumerator.
            IEnumerable<ICacheEntry<int, int>> e = cache;

            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            // Check local enumerator.
            e = cache.GetLocalEntries();

            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            // Evict and check peek modes.
            cache.LocalEvict(new List<int> { keys[0] } );

            // TODO: IGNITE-4535
            //e = cache.GetLocalEntries(CachePeekMode.Onheap);
            //CheckEnumerator(e.GetEnumerator(), new List<int> { keys[1] });
            //CheckEnumerator(e.GetEnumerator(), new List<int> { keys[1] });

            e = cache.GetLocalEntries(CachePeekMode.All);
            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            cache.Remove(keys[0]);
        }

        /// <summary>
        /// Check enumerator content.
        /// </summary>
        /// <param name="e">Enumerator.</param>
        /// <param name="keys">Keys.</param>
        private static void CheckEnumerator(IEnumerator<ICacheEntry<int, int>> e, IList<int> keys)
        {
            CheckEnumerator0(e, keys);

            e.Reset();

            CheckEnumerator0(e, keys);

            e.Dispose();

            Assert.Throws<ObjectDisposedException>(() => { e.MoveNext(); });
            Assert.Throws<ObjectDisposedException>(() => { var entry = e.Current; });
            Assert.Throws<ObjectDisposedException>(e.Reset);

            e.Dispose();
        }

        /// <summary>
        /// Check enumerator content.
        /// </summary>
        /// <param name="e">Enumerator.</param>
        /// <param name="keys">Keys.</param>
        private static void CheckEnumerator0(IEnumerator<ICacheEntry<int, int>> e, IList<int> keys)
        {
            Assert.Throws<InvalidOperationException>(() => { var entry = e.Current; });

            int cnt = 0;

            while (e.MoveNext())
            {
                ICacheEntry<int, int> entry = e.Current;

                Assert.IsNotNull(entry);

                Assert.IsTrue(keys.Contains(entry.Key), "Unexpected entry: " + entry);

                Assert.AreEqual(entry.Key + 1, entry.Value);

                cnt++;
            }

            Assert.AreEqual(keys.Count, cnt);

            Assert.IsFalse(e.MoveNext());

            Assert.Throws<InvalidOperationException>(() => { var entry = e.Current; });
        }

        [Test]
        public void TestPutGetBinary()
        {
            var cache = Cache<int, BinarizablePerson>();

            BinarizablePerson obj1 = new BinarizablePerson("obj1", 1);

            cache.Put(1, obj1);

            obj1 = cache.Get(1);

            Assert.AreEqual("obj1", obj1.Name);
            Assert.AreEqual(1, obj1.Age);
        }

        [Test]
        public void TestPutGetBinaryAsync()
        {
            var cache = Cache<int, BinarizablePerson>().WrapAsync();

            BinarizablePerson obj1 = new BinarizablePerson("obj1", 1);

            cache.Put(1, obj1);

            obj1 = cache.Get(1);

            Assert.AreEqual("obj1", obj1.Name);
            Assert.AreEqual(1, obj1.Age);
        }

        [Test]
        public void TestPutGetBinaryKey()
        {
            var cache = Cache<CacheTestKey, string>();

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache.Put(new CacheTestKey(i), "val-" + i);

            for (int i = 0; i < cnt; i++)
                Assert.AreEqual("val-" + i, cache.Get(new CacheTestKey(i)));
        }

        [Test]
        public void TestGetAsync2()
        {
            var cache = Cache();

            for (int i = 0; i < 100; i++)
                cache.Put(i, i);

            var futs = new List<Task<int>>();

            for (int i = 0; i < 1000; i++)
            {
                futs.Add(cache.GetAsync(i % 100));
            }

            for (int i = 0; i < 1000; i++) {
                Assert.AreEqual(i % 100, futs[i].Result, "Unexpected result: " + i);

                Assert.IsTrue(futs[i].IsCompleted);
            }
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestGetAsyncMultithreaded()
        {
            var cache = Cache();

            for (int i = 0; i < 100; i++)
                cache.Put(i, i);

            TestUtils.RunMultiThreaded(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    var futs = new List<Task<int>>();

                    for (int j = 0; j < 100; j++)
                        futs.Add(cache.GetAsync(j));

                    for (int j = 0; j < 100; j++)
                        Assert.AreEqual(j, futs[j].Result);
                }
            }, 10);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPutxAsyncMultithreaded()
        {
            var cache = Cache();

            TestUtils.RunMultiThreaded(() =>
            {
                Random rnd = new Random();

                for (int i = 0; i < 50; i++)
                {
                    var futs = new List<Task>();

                    for (int j = 0; j < 10; j++)
                        futs.Add(cache.PutAsync(rnd.Next(1000), i));

                    foreach (var fut in futs)
                        fut.Wait();
                }
            }, 5);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPutGetAsyncMultithreaded()
        {
            var cache = Cache<CacheTestKey, BinarizablePerson>();

            const int threads = 10;
            const int objPerThread = 1000;

            int cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<Task>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    futs.Add(cache.PutAsync(new CacheTestKey(key), new BinarizablePerson("Person-" + key, key)));
                }

                foreach (var fut in futs)
                {
                    fut.Wait();

                    Assert.IsTrue(fut.IsCompleted);
                }
            }, threads);

            for (int i = 0; i < threads; i++)
            {
                int threadIdx = i + 1;

                for (int j = 0; j < objPerThread; j++)
                {
                    int key = threadIdx * objPerThread + i;

                    var p = cache.GetAsync(new CacheTestKey(key)).Result;

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.Age);
                    Assert.AreEqual("Person-" + key, p.Name);
                }
            }

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.PutAsync(new CacheTestKey(key), new BinarizablePerson("Person-" + key, key)).Wait();
                }
            }, threads);

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<Task<BinarizablePerson>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    futs.Add(cache.GetAsync(new CacheTestKey(key)));
                }

                for (int i = 0; i < objPerThread; i++)
                {
                    var fut = futs[i];

                    int key = threadIdx * objPerThread + i;

                    var p = fut.Result;

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.Age);
                    Assert.AreEqual("Person-" + key, p.Name);
                }
            }, threads);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestAsyncMultithreadedKeepBinary()
        {
            var cache = Cache().WithKeepBinary<CacheTestKey, BinarizablePerson>();
            var portCache = Cache().WithKeepBinary<CacheTestKey, IBinaryObject>();

            const int threads = 10;
            const int objPerThread = 1000;

            int cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<Task>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    var task = cache.PutAsync(new CacheTestKey(key), new BinarizablePerson("Person-" + key, key));

                    futs.Add(task);
                }

                foreach (var fut in futs)
                    fut.Wait();
            }, threads);

            for (int i = 0; i < threads; i++)
            {
                int threadIdx = i + 1;

                for (int j = 0; j < objPerThread; j++)
                {
                    int key = threadIdx * objPerThread + i;

                    IBinaryObject p = portCache.Get(new CacheTestKey(key));

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.GetField<int>("age"));
                    Assert.AreEqual("Person-" + key, p.GetField<string>("name"));
                }
            }

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<Task<IBinaryObject>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    futs.Add(portCache.GetAsync(new CacheTestKey(key)));
                }

                for (int i = 0; i < objPerThread; i++)
                {
                    var fut = futs[i];

                    int key = threadIdx * objPerThread + i;

                    var p = fut.Result;

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.GetField<int>("age"));
                    Assert.AreEqual("Person-" + key, p.GetField<string>("name"));
                }
            }, threads);

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<Task<bool>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    futs.Add(cache.RemoveAsync(new CacheTestKey(key)));
                }

                for (int i = 0; i < objPerThread; i++)
                {
                    var fut = futs[i];

                    Assert.IsTrue(fut.Result);
                }
            }, threads);
        }

        /// <summary>
        /// Test thraed-locals leak.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestThreadLocalLeak()
        {
            var cache = Cache<string, string>();

            Exception err = null;

            const int threadCnt = 10;

            Thread[] threads = new Thread[threadCnt];

            ThreadStart[] threadStarts = new ThreadStart[threadCnt];

            for (int j = 0; j < threadCnt; j++)
            {
                string key = "key" + j;

                threadStarts[j] = () =>
                {
                    try
                    {
                        cache.Put(key, key);

                        Assert.AreEqual(key, cache.Get(key));
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref err, e, null);

                        Assert.Fail("Unexpected error: " + e);
                    }
                };
            }

            for (int i = 0; i < 100 && err == null; i++)
            {
                for (int j = 0 ; j < threadCnt; j++) {
                    Thread t = new Thread(threadStarts[j]);

                    threads[j] = t;
                }

                foreach (Thread t in threads)
                    t.Start();

                foreach (Thread t in threads)
                    t.Join();

                if (i % 500 == 0)
                {
                    Console.WriteLine("Iteration: " + i);

                    GC.Collect();
                }
            }

            Assert.IsNull(err);
        }

        /**
         * Test tries to provoke garbage collection for .Net future before it was completed to verify
         * futures pinning works.
         */
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestFuturesGc()
        {
            var cache = Cache();

            cache.PutAsync(1, 1);

            for (int i = 0; i < 10; i++)
            {
                TestUtils.RunMultiThreaded(() =>
                {
                    for (int j = 0; j < 1000; j++)
                        cache.GetAsync(1);
                }, 5);

                GC.Collect();

                Assert.AreEqual(1, cache.GetAsync(1).Result);
            }

            Thread.Sleep(2000);
        }

        [Test]
        public void TestPartitions()
        {
            ICacheAffinity aff = Affinity();

            for (int i = 0; i < 5; i++ )
                Assert.AreEqual(CachePartitions(), aff.Partitions);
        }

        [Test]
        public void TestKeyPartition()
        {
            ICacheAffinity aff = Affinity();

            {
                ISet<int> parts = new HashSet<int>();

                for (int i = 0; i < 1000; i++)
                    parts.Add(aff.GetPartition(i));

                if (LocalCache())
                    Assert.AreEqual(1, parts.Count);
                else
                    Assert.IsTrue(parts.Count > 10);
            }

            {
                ISet<int> parts = new HashSet<int>();

                for (int i = 0; i < 1000; i++)
                    parts.Add(aff.GetPartition("key" + i));

                if (LocalCache())
                    Assert.AreEqual(1, parts.Count);
                else
                    Assert.IsTrue(parts.Count > 10);
            }
        }

        [Test]
        public void TestIsPrimaryOrBackup()
        {
            ICacheAffinity aff = Affinity();

            ICollection<IClusterNode> nodes = GetIgnite(0).GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count > 0);

            IClusterNode node = nodes.First();

            {
                bool found = false;

                for (int i = 0; i < 1000; i++)
                {
                    if (aff.IsPrimary(node, i))
                    {
                        Assert.IsTrue(aff.IsPrimaryOrBackup(node, i));

                        found = true;

                        if (nodes.Count > 1)
                            Assert.IsFalse(aff.IsPrimary(nodes.Last(), i));

                        break;
                    }
                }

                Assert.IsTrue(found, "Failed to find primary key for node " + node);
            }

            if (nodes.Count > 1)
            {
                bool found = false;

                for (int i = 0; i < 1000; i++)
                {
                    if (aff.IsBackup(node, i))
                    {
                        Assert.IsTrue(aff.IsPrimaryOrBackup(node, i));

                        found = true;

                        break;
                    }
                }

                Assert.IsTrue(found, "Failed to find backup key for node " + node);
            }
        }

        [Test]
        public void TestNodePartitions()
        {
            ICacheAffinity aff = Affinity();

            ICollection<IClusterNode> nodes = GetIgnite(0).GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count > 0);

            if (nodes.Count == 1)
            {
                IClusterNode node = nodes.First();

                int[] parts = aff.GetBackupPartitions(node);

                Assert.AreEqual(0, parts.Length);

                parts = aff.GetAllPartitions(node);

                Assert.AreEqual(CachePartitions(), parts.Length);
            }
            else
            {
                IList<int> allPrimaryParts = new List<int>();
                IList<int> allBackupParts = new List<int>();
                IList<int> allParts = new List<int>();

                foreach(IClusterNode node in nodes) {
                    int[] parts = aff.GetPrimaryPartitions(node);

                    foreach (int part in parts)
                        allPrimaryParts.Add(part);

                    parts = aff.GetBackupPartitions(node);

                    foreach (int part in parts)
                        allBackupParts.Add(part);

                    parts = aff.GetAllPartitions(node);

                    foreach (int part in parts)
                        allParts.Add(part);
                }

                Assert.AreEqual(CachePartitions(), allPrimaryParts.Count);
                Assert.AreEqual(CachePartitions() * Backups(), allBackupParts.Count);
                Assert.AreEqual(CachePartitions() * (Backups() + 1), allParts.Count);
            }
        }

        [Test]
        public void TestAffinityKey()
        {
            ICacheAffinity aff = Affinity();

            Assert.AreEqual(10, aff.GetAffinityKey<int, int>(10));

            Assert.AreEqual("string", aff.GetAffinityKey<string, string>("string"));
        }

        [Test]
        public void TestMapToNode()
        {
            ICacheAffinity aff = Affinity();

            const int key = 1;

            IClusterNode node = aff.MapKeyToNode(key);

            Assert.IsNotNull(node);

            Assert.IsTrue(GetIgnite(0).GetCluster().GetNodes().Contains(node));

            Assert.IsTrue(aff.IsPrimary(node, key));

            Assert.IsTrue(aff.IsPrimaryOrBackup(node, key));

            Assert.IsFalse(aff.IsBackup(node, key));

            int part = aff.GetPartition(key);

            IClusterNode partNode = aff.MapPartitionToNode(part);

            Assert.AreEqual(node, partNode);
        }

        [Test]
        public void TestMapToPrimaryAndBackups()
        {
            ICacheAffinity aff = Affinity();

            const int key = 1;

            IList<IClusterNode> nodes = aff.MapKeyToPrimaryAndBackups(key);

            Assert.IsTrue(nodes.Count > 0);

            for (int i = 0; i < nodes.Count; i++)
            {
                if (i == 0)
                    Assert.IsTrue(aff.IsPrimary(nodes[i], key));
                else
                    Assert.IsTrue(aff.IsBackup(nodes[i], key));
            }

            int part = aff.GetPartition(key);

            IList<IClusterNode> partNodes = aff.MapPartitionToPrimaryAndBackups(part);

            Assert.AreEqual(nodes, partNodes);
        }

        [Test]
        public void TestMapKeysToNodes()
        {
            ICacheAffinity aff = Affinity();

            IList<int> keys = new List<int> {1, 2, 3};

            IDictionary<IClusterNode, IList<int>> map = aff.MapKeysToNodes(keys);

            Assert.IsTrue(map.Count > 0);

            foreach (int key in keys)
            {
                IClusterNode primary = aff.MapKeyToNode(key);

                Assert.IsTrue(map.ContainsKey(primary));

                IList<int> nodeKeys = map[primary];

                Assert.IsNotNull(nodeKeys);

                Assert.IsTrue(nodeKeys.Contains(key));
            }
        }

        [Test]
        public void TestMapPartitionsToNodes()
        {
            ICacheAffinity aff = Affinity();

            if (LocalCache())
            {
                IList<int> parts = new List<int> { 0 };

                IDictionary<int, IClusterNode> map = aff.MapPartitionsToNodes(parts);

                Assert.AreEqual(parts.Count, map.Count);

                Assert.AreEqual(GetIgnite(0).GetCluster().GetLocalNode(), map[0]);
            }
            else
            {
                IList<int> parts = new List<int> { 1, 2, 3 };

                IDictionary<int, IClusterNode> map = aff.MapPartitionsToNodes(parts);

                Assert.AreEqual(parts.Count, map.Count);

                foreach (int part in parts)
                {
                    Assert.IsTrue(map.ContainsKey(part));

                    IClusterNode primary = aff.MapPartitionToNode(part);

                    Assert.AreEqual(primary, map[part], "Wrong node for partition: " + part);
                }
            }
        }

        [Test]
        public void TestKeepBinaryFlag()
        {
            TestKeepBinaryFlag(false);
        }

        [Test]
        public void TestKeepBinaryFlagAsync()
        {
            TestKeepBinaryFlag(true);
        }

        [Test]
        public void TestNearKeys()
        {
            if (!NearEnabled())
                return;

            const int count = 20;

            var cache = Cache();
            var aff = cache.Ignite.GetAffinity(cache.Name);
            var node = cache.Ignite.GetCluster().GetLocalNode();

            for (int i = 0; i < count; i++)
                cache.Put(i, -i - 1);

            var nearKeys = Enumerable.Range(0, count).Where(x => !aff.IsPrimaryOrBackup(node, x)).ToArray();

            var nearKeysString = nearKeys.Select(x => x.ToString()).Aggregate((x, y) => x + ", " + y);

            Console.WriteLine("Near keys: " + nearKeysString);

            foreach (var nearKey in nearKeys.Take(3))
                Assert.AreNotEqual(0, cache.Get(nearKey));
        }

        [Test]
        public void TestSerializable()
        {
            var cache = Cache<int, TestSerializableObject>();

            var obj = new TestSerializableObject {Name = "Vasya", Id = 128};

            cache.Put(1, obj);

            var resultObj = cache.Get(1);

            Assert.AreEqual(obj, resultObj);
        }

        [Test]
        public void TestSerializableKeepBinary()
        {
            var cache = Cache<int, TestSerializableObject>();

            var obj = new TestSerializableObject {Name = "Vasya", Id = 128};

            cache.Put(1, obj);

            var binaryRes = cache.WithKeepBinary<int, IBinaryObject>().Get(1);

            var resultObj = binaryRes.Deserialize<TestSerializableObject>();

            Assert.AreEqual(obj, resultObj);
        }

        [Test]
        public void TestInvoke()
        {
            TestInvoke(false);
        }

        [Test]
        public void TestInvokeAsync()
        {
            TestInvoke(true);
        }

        private void TestInvoke(bool async)
        {
            TestInvoke<AddArgCacheEntryProcessor>(async);
            TestInvoke<BinarizableAddArgCacheEntryProcessor>(async);

            Assert.Throws<Exception>(() => TestInvoke<NonSerializableCacheEntryProcessor>(async));
        }

        private void TestInvoke<T>(bool async) where T: AddArgCacheEntryProcessor, new()
        {
            var cache = async ? Cache().WrapAsync() : Cache();

            cache.Clear();

            const int key = 1;
            const int value = 3;
            const int arg = 5;

            cache.Put(key, value);

            // Existing entry
            Assert.AreEqual(value + arg, cache.Invoke(key, new T(), arg));
            Assert.AreEqual(value + arg, cache.Get(key));

            // Non-existing entry
            Assert.AreEqual(arg, cache.Invoke(10, new T {Exists = false}, arg));
            Assert.AreEqual(arg, cache.Get(10));

            // Remove entry
            Assert.AreEqual(0, cache.Invoke(key, new T {Remove = true}, arg));
            Assert.AreEqual(false, cache.ContainsKey(key));

            // Test exceptions
            AssertThrowsCacheEntryProcessorException(() => cache.Invoke(key, new T {ThrowErr = true}, arg));
            AssertThrowsCacheEntryProcessorException(
                () => cache.Invoke(key, new T {ThrowErrBinarizable = true}, arg));
            AssertThrowsCacheEntryProcessorException(
                () => cache.Invoke(key, new T { ThrowErrNonSerializable = true }, arg), "ExpectedException");
        }

        /// <summary>
        /// Asserts that specified action throws a CacheEntryProcessorException.
        /// </summary>
        private static void AssertThrowsCacheEntryProcessorException(Action action, string containsText = null)
        {
            var ex = Assert.Throws<CacheEntryProcessorException>(() => action());

            Assert.IsInstanceOf<JavaException>(ex.InnerException);

            if (string.IsNullOrEmpty(containsText))
            {
                Assert.AreEqual(AddArgCacheEntryProcessor.ExceptionText, ex.GetBaseException().Message);
            }
            else
            {
                Assert.IsTrue(ex.ToString().Contains(containsText), "Expected: " + containsText + ", actual: " + ex);
            }
        }

        [Test]
        public void TestInvokeAll()
        {
            TestInvokeAll(false);
        }

        [Test]
        public void TestInvokeAllAsync()
        {
            TestInvokeAll(true);
        }

        private void TestInvokeAll(bool async)
        {
            for (var i = 1; i < 10; i++)
            {
                TestInvokeAll<AddArgCacheEntryProcessor>(async, i);
                TestInvokeAll<BinarizableAddArgCacheEntryProcessor>(async, i);
                Assert.Throws<Exception>(() => TestInvokeAll<NonSerializableCacheEntryProcessor>(async, i));
            }
        }

        private void TestInvokeAll<T>(bool async, int entryCount) where T : AddArgCacheEntryProcessor, new()
        {
            var cache = async ? Cache().WrapAsync() : Cache();

            var entries = Enumerable.Range(1, entryCount).ToDictionary(x => x, x => x + 1);

            cache.PutAll(entries);

            const int arg = 5;

            // Existing entries
            var res = cache.InvokeAll(entries.Keys, new T(), arg);

            var results = res.OrderBy(x => x.Key).Select(x => x.Result);
            var expectedResults = entries.OrderBy(x => x.Key).Select(x => x.Value + arg);

            Assert.IsTrue(results.SequenceEqual(expectedResults));

            var resultEntries = cache.GetAll(entries.Keys);

            Assert.IsTrue(resultEntries.All(x => x.Value == x.Key + 1 + arg));

            // Remove entries
            res = cache.InvokeAll(entries.Keys, new T {Remove = true}, arg);

            Assert.IsTrue(res.All(x => x.Result == 0));
            Assert.AreEqual(0, cache.GetAll(entries.Keys).Count);

            // Non-existing entries
            res = cache.InvokeAll(entries.Keys, new T {Exists = false}, arg);

            Assert.IsTrue(res.All(x => x.Result == arg));
            Assert.IsTrue(cache.GetAll(entries.Keys).All(x => x.Value == arg));

            // Test exceptions
            var errKey = entries.Keys.Reverse().Take(5).Last();

            TestInvokeAllException(cache, entries, new T { ThrowErr = true, ThrowOnKey = errKey }, arg, errKey);
            TestInvokeAllException(cache, entries, new T { ThrowErrBinarizable = true, ThrowOnKey = errKey },
                arg, errKey);
            TestInvokeAllException(cache, entries, new T { ThrowErrNonSerializable = true, ThrowOnKey = errKey },
                arg, errKey, "ExpectedException");

        }

        private static void TestInvokeAllException<T>(ICache<int, int> cache, Dictionary<int, int> entries,
            T processor, int arg, int errKey, string exceptionText = null) where T : AddArgCacheEntryProcessor
        {
            var res = cache.InvokeAll(entries.Keys, processor, arg);

            foreach (var procRes in res)
            {
                if (procRes.Key == errKey)
                    // ReSharper disable once AccessToForEachVariableInClosure
                    AssertThrowsCacheEntryProcessorException(() => { var x = procRes.Result; }, exceptionText);
                else
                    Assert.Greater(procRes.Result, 0);
            }
        }

        /// <summary>
        /// Test skip-store semantics.
        /// </summary>
        [Test]
        public void TestSkipStore()
        {
            var cache = (CacheImpl<int, int>) Cache();

            Assert.IsFalse(cache.IsSkipStore);

            // Ensure correct flag set.
            var cacheSkipStore1 = (CacheImpl<int, int>) cache.WithSkipStore();

            Assert.AreNotSame(cache, cacheSkipStore1);
            Assert.IsFalse(cache.IsSkipStore);
            Assert.IsTrue(cacheSkipStore1.IsSkipStore);

            // Ensure that the same instance is returned if flag is already set.
            var cacheSkipStore2 = (CacheImpl<int, int>) cacheSkipStore1.WithSkipStore();

            Assert.IsTrue(cacheSkipStore2.IsSkipStore);
            Assert.AreSame(cacheSkipStore1, cacheSkipStore2);

            // Ensure other flags are preserved.
            Assert.IsTrue(((CacheImpl<int, int>) cache.WithKeepBinary<int, int>().WithSkipStore()).IsKeepBinary);
        }

        [Test]
        public void TestRebalance()
        {
            var cache = Cache();

            var task = cache.Rebalance();

            task.Wait();
        }

        [Test]
        public void TestCacheNames()
        {
            var cacheNames = GetIgnite(0).GetCacheNames();
            var expectedNames = new[]
            {
                "local", "local_atomic", "partitioned", "partitioned_atomic",
                "partitioned_near", "partitioned_atomic_near", "replicated", "replicated_atomic"
            };

            Assert.AreEqual(0, expectedNames.Except(cacheNames).Count());
        }


        [Test]
        public void TestIndexer()
        {
            var cache = Cache();

            Assert.Throws<KeyNotFoundException>(() => Console.WriteLine(cache[0]));  // missing key throws

            cache[1] = 5;

            Assert.AreEqual(5, cache[1]);
        }

        /// <summary>
        /// Tests that operations in TransactionScope work correctly in any cache mode (tx or non-tx).
        /// </summary>
        [Test]
        public void TestTransactionScope()
        {
            var cache = Cache();

            cache[1] = 1;

            using (var ts = new TransactionScope())
            {
                cache[1] = 2;

                ts.Complete();
            }

            Assert.AreEqual(2, cache[1]);
        }

        private void TestKeepBinaryFlag(bool async)
        {
            var cache0 = async ? Cache().WrapAsync() : Cache();

            var cache = cache0.WithKeepBinary<int, BinarizablePerson>();

            var binCache = cache0.WithKeepBinary<int, IBinaryObject>();

            int cnt = 10;

            IList<int> keys = new List<int>();

            for (int i = 0; i < cnt; i++ ) {
                cache.Put(i, new BinarizablePerson("person-" + i, i));

                keys.Add(i);
            }

            IList<IBinaryObject> objs = new List<IBinaryObject>();

            for (int i = 0; i < cnt; i++)
            {
                var obj = binCache.Get(i);

                CheckPersonData(obj, "person-" + i, i);

                objs.Add(obj);
            }

            // Check objects weren't corrupted by subsequent cache operations.
            for (int i = 0; i < cnt; i++)
            {
                IBinaryObject obj = objs[i];

                CheckPersonData(obj, "person-" + i, i);
            }

            // Check keepBinary for GetAll operation.
            var allObjs1 = binCache.GetAll(keys).ToDictionary(x => x.Key, x => x.Value);

            var allObjs2 = binCache.GetAll(keys).ToDictionary(x => x.Key, x => x.Value);

            for (int i = 0; i < cnt; i++)
            {
                CheckPersonData(allObjs1[i], "person-" + i, i);

                CheckPersonData(allObjs2[i], "person-" + i, i);
            }

            // Check keepBinary for Remove operation.
            var success0 = cache.Remove(0);
            var success1 = cache.Remove(1);

            Assert.AreEqual(true, success0);
            Assert.AreEqual(true, success1);
        }

        private void CheckPersonData(IBinaryObject obj, string expName, int expAge)
        {
            Assert.AreEqual(expName, obj.GetField<string>("name"));
            Assert.AreEqual(expAge, obj.GetField<int>("age"));

            BinarizablePerson person = obj.Deserialize<BinarizablePerson>();

            Assert.AreEqual(expName, person.Name);
            Assert.AreEqual(expAge, person.Age);
        }

        private static int GetPrimaryKeyForCache(ICache<int, int> cache)
        {
            return GetPrimaryKeysForCacheFrom(cache, 0).First();
        }

        private static int GetPrimaryKeyForCache(ICache<int, int> cache, int startFrom)
        {
            return GetPrimaryKeysForCacheFrom(cache, startFrom).First();
        }

        private static List<int> GetPrimaryKeysForCache(ICache<int, int> cache, int cnt)
        {
            return GetPrimaryKeysForCacheFrom(cache, 0).Take(cnt).ToList();
        }

        private static IEnumerable<int> GetPrimaryKeysForCacheFrom(ICache<int, int> cache, int startFrom)
        {
            IClusterNode node = cache.Ignite.GetCluster().GetLocalNode();

            ICacheAffinity aff = cache.Ignite.GetAffinity(cache.Name);

            return Enumerable.Range(startFrom, int.MaxValue - startFrom).Where(x => aff.IsPrimary(node, x));
        }

        private static int NearKeyForCache(ICache<int, int> cache)
        {
            IClusterNode node = cache.Ignite.GetCluster().GetLocalNode();

            ICacheAffinity aff = cache.Ignite.GetAffinity(cache.Name);

            for (int i = 0; i < 100000; i++)
            {
                if (!aff.IsPrimaryOrBackup(node, i))
                    return i;
            }

            Assert.Fail("Failed to find near key.");

            return 0;
        }

        private static string GetKeyAffinity(ICache<int, int> cache, int key)
        {
            if (cache.Ignite.GetAffinity(cache.Name).IsPrimary(cache.Ignite.GetCluster().GetLocalNode(), key))
                return "primary";

            if (cache.Ignite.GetAffinity(cache.Name).IsBackup(cache.Ignite.GetCluster().GetLocalNode(), key))
                return "backup";

            return "near";
        }

        protected virtual int CachePartitions()
        {
            return 1024;
        }

        protected virtual int Backups()
        {
            return 0;
        }

        protected virtual int GridCount()
        {
            return 1;
        }

        protected virtual string CacheName()
        {
            return "partitioned";
        }

        protected virtual bool NearEnabled()
        {
            return false;
        }

        protected virtual bool LocalCache()
        {
            return false;
        }

        protected virtual bool ReplicatedCache()
        {
            return true;
        }

        private static int PeekInt(ICache<int, int> cache, int key)
        {
            int val;

            cache.TryLocalPeek(key, out val, CachePeekMode.Onheap);

            return val;
        }

        /// <summary>
        /// Test serializable object.
        /// </summary>
        [Serializable]
        private class TestSerializableObject
        {
            public string Name { get; set; }
            public int Id { get; set; }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;

                var other = (TestSerializableObject)obj;
                return obj.GetType() == GetType() && (string.Equals(Name, other.Name) && Id == other.Id);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ Id;
                }
            }
        }

        private class Container
        {
            public int Id;

            public Container Inner;
        }

        private class ExpiryPolicyFactory : IFactory<IExpiryPolicy>
        {
            /** */
            private readonly IExpiryPolicy _expiryPolicy;

            /// <summary>
            /// Initializes a new instance of the <see cref="ExpiryPolicyFactory"/> class.
            /// </summary>
            /// <param name="expiryPolicy">The expiry policy.</param>
            public ExpiryPolicyFactory(IExpiryPolicy expiryPolicy)
            {
                _expiryPolicy = expiryPolicy;
            }

            /** <inheritdoc /> */
            public IExpiryPolicy CreateInstance()
            {
                return _expiryPolicy;
            }
        }
    }
}

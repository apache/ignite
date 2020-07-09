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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Thin client cache test.
    /// </summary>
    public class CacheTest : ClientTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheTest"/> class.
        /// </summary>
        public CacheTest()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheTest"/> class.
        /// </summary>
        public CacheTest(int gridCount, bool enableSsl = false) : base(gridCount, enableSsl)
        {
            // No-op.
        }

        /// <summary>
        /// Tests the cache put / get with primitive data types.
        /// </summary>
        [Test]
        public void TestPutGetPrimitives()
        {
            GetCache<string>().Put(1, "foo");

            var clientCache = GetClientCache<int?, string>();

            clientCache.Put(2, "bar");
            clientCache[3] = "baz";

            // Existing key.
            Assert.AreEqual("foo", clientCache.Get(1));
            Assert.AreEqual("foo", clientCache[1]);
            Assert.AreEqual("bar", clientCache[2]);
            Assert.AreEqual("baz", clientCache[3]);

            // Missing key.
            Assert.Throws<KeyNotFoundException>(() => clientCache.Get(-1));

            // Null key.
            Assert.Throws<ArgumentNullException>(() => clientCache.Get(null));

            // Null vs 0.
            var intCache = GetClientCache<int?, int?>();
            intCache.Put(1, 0);
            Assert.AreEqual(0, intCache.Get(1));
        }

        /// <summary>
        /// Tests the cache put / get for Empty object type.
        /// </summary>
        [Test]
        public void TestPutGetEmptyObject()
        {
            var serverCache = GetCache<EmptyObject>();
            var clientCache = GetClientCache<EmptyObject>();

            serverCache.Put(1, new EmptyObject());
            Assert.IsNotNull(clientCache.Get(1));
        }

        /// <summary>
        /// Tests the cache put / get with user data types.
        /// </summary>
        [Test]
        public void TestPutGetUserObjects([Values(true, false)] bool compactFooter)
        {
            var cfg = GetClientConfiguration();

            cfg.BinaryConfiguration = new BinaryConfiguration
            {
                CompactFooter = compactFooter
            };

            using (var client = Ignition.StartClient(cfg))
            {
                var person = new Person {Id = 100, Name = "foo"};
                var person2 = new Person2 {Id = 200, Name = "bar"};

                var serverCache = GetCache<Person>();
                var clientCache = client.GetCache<int?, Person>(CacheName);

                Assert.AreEqual(CacheName, clientCache.Name);

                // Put through server cache.
                serverCache.Put(1, person);

                // Put through client cache.
                clientCache.Put(2, person2);
                clientCache[3] = person2;

                // Read from client cache.
                Assert.AreEqual("foo", clientCache.Get(1).Name);
                Assert.AreEqual(100, clientCache[1].Id);
                Assert.AreEqual(200, clientCache[2].Id);
                Assert.AreEqual(200, clientCache[3].Id);

                // Read from server cache.
                Assert.AreEqual("foo", serverCache.Get(1).Name);
                Assert.AreEqual(100, serverCache[1].Id);
                Assert.AreEqual(200, serverCache[2].Id);
                Assert.AreEqual(200, serverCache[3].Id);

                // Null key or value.
                Assert.Throws<ArgumentNullException>(() => clientCache.Put(10, null));
                Assert.Throws<ArgumentNullException>(() => clientCache.Put(null, person));
            }
        }

        /// <summary>
        /// Tests the cache put / get for Dictionary with Enum keys.
        /// </summary>
        [Test]
        public void TestPutGetDictionary([Values(true, false)] bool compactFooter)
        {
            var cfg = GetClientConfiguration();

            cfg.BinaryConfiguration = new BinaryConfiguration
            {
                CompactFooter = compactFooter
            };

            using (var client = Ignition.StartClient(cfg))
            {
                var dict = new Dictionary<ByteEnum, int> { { ByteEnum.One, 1 }, { ByteEnum.Two, 2 } };

                var serverCache = GetCache<Dictionary<ByteEnum, int>>();
                var clientCache = client.GetCache<int, Dictionary<ByteEnum, int>>(CacheName);

                serverCache.Put(1, dict);
                var res = clientCache.Get(1);

                Assert.AreEqual(dict, res);
            }
        }

        /// <summary>
        /// Tests the cache put / get for HashSet with Enum keys.
        /// </summary>
        [Test]
        public void TestPutGetHashSet([Values(true, false)] bool compactFooter)
        {
            var cfg = GetClientConfiguration();

            cfg.BinaryConfiguration = new BinaryConfiguration
            {
                CompactFooter = compactFooter
            };

            using (var client = Ignition.StartClient(cfg))
            {
                var hashSet = new HashSet<ByteEnum> { ByteEnum.One, ByteEnum.Two };

                var serverCache = GetCache<HashSet<ByteEnum>>();
                var clientCache = client.GetCache<int, HashSet<ByteEnum>>(CacheName);

                serverCache.Put(1, hashSet);
                var res = clientCache.Get(1);

                Assert.AreEqual(hashSet, res);
            }
        }

        /// <summary>
        /// Tests the GetAsync method.
        /// </summary>
        [Test]
        public void TestGetAsync()
        {
            var cache = GetClientCache<int>();
            cache[1] = 1;

            // Existing key.
            Assert.AreEqual(1, cache.GetAsync(1).Result);

            // Missing key.
            cache.Remove(1);
            var aex = Assert.Throws<AggregateException>(() => cache.GetAsync(1).Wait());
            Assert.IsInstanceOf<KeyNotFoundException>(aex.InnerException);

            // Incorrect data type.
            GetClientCache<Person>().PutAsync(1, new Person(1)).Wait();
            aex = Assert.Throws<AggregateException>(() => cache.GetAsync(1).Wait());
            Assert.IsInstanceOf<InvalidCastException>(aex.InnerException);
        }

        /// <summary>
        /// Tests the TryGet method.
        /// </summary>
        [Test]
        public void TestTryGet()
        {
            var cache = GetClientCache<int?, int>();

            cache[1] = 0;
            cache[2] = 2;

            // Non-existent key.
            int res;
            var success = cache.TryGet(0, out res);

            Assert.AreEqual(0, res);
            Assert.IsFalse(success);

            // Key with default value.
            success = cache.TryGet(1, out res);

            Assert.AreEqual(0, res);
            Assert.IsTrue(success);

            // Key with custom value.
            success = cache.TryGet(2, out res);

            Assert.AreEqual(2, res);
            Assert.IsTrue(success);

            // Null key.
            Assert.Throws<ArgumentNullException>(() => cache.TryGet(null, out res));
        }

        /// <summary>
        /// Tests the GetAll method.
        /// </summary>
        [Test]
        public void TestGetAll()
        {
            var cache = GetClientCache<int?, int>();

            cache[1] = 1;
            cache[2] = 2;
            cache[3] = 3;

            var res = cache.GetAll(new int?[] {1}).Single();
            Assert.AreEqual(1, res.Key);
            Assert.AreEqual(1, res.Value);

            res = cache.GetAll(new int?[] {1, -1}).Single();
            Assert.AreEqual(1, res.Key);
            Assert.AreEqual(1, res.Value);

            CollectionAssert.AreEquivalent(new[] {1, 2, 3},
                cache.GetAll(new int?[] {1, 2, 3}).Select(x => x.Value));

            Assert.Throws<ArgumentNullException>(() => cache.GetAll(null));

            Assert.Throws<IgniteClientException>(() => cache.GetAll(new int?[] {1, null}));
            Assert.Throws<IgniteClientException>(() => cache.GetAll(new int?[] {null}));
        }

        /// <summary>
        /// Tests the GetAndPut method.
        /// </summary>
        [Test]
        public void TestGetAndPut()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndPut(1, 1);
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);

            Assert.IsTrue(cache.ContainsKey(1));

            res = cache.GetAndPut(1, 2);
            Assert.IsTrue(res.Success);
            Assert.AreEqual(1, res.Value);

            Assert.AreEqual(2, cache[1]);

            Assert.Throws<ArgumentNullException>(() => cache.GetAndPut(1, null));
            Assert.Throws<ArgumentNullException>(() => cache.GetAndPut(null, 1));
        }

        /// <summary>
        /// Tests the GetAndReplace method.
        /// </summary>
        [Test]
        public void TestGetAndReplace()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndReplace(1, 1);
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);

            Assert.IsFalse(cache.ContainsKey(1));
            cache[1] = 1;

            res = cache.GetAndReplace(1, 2);
            Assert.IsTrue(res.Success);
            Assert.AreEqual(1, res.Value);

            Assert.AreEqual(2, cache[1]);

            Assert.Throws<ArgumentNullException>(() => cache.GetAndReplace(1, null));
            Assert.Throws<ArgumentNullException>(() => cache.GetAndReplace(null, 1));
        }

        /// <summary>
        /// Tests the GetAndRemove method.
        /// </summary>
        [Test]
        public void TestGetAndRemove()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndRemove(1);
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);

            Assert.IsFalse(cache.ContainsKey(1));
            cache[1] = 1;

            res = cache.GetAndRemove(1);
            Assert.IsTrue(res.Success);
            Assert.AreEqual(1, res.Value);

            Assert.IsFalse(cache.ContainsKey(1));

            Assert.Throws<ArgumentNullException>(() => cache.GetAndRemove(null));
        }

        /// <summary>
        /// Tests the ContainsKey method.
        /// </summary>
        [Test]
        public void TestContainsKey()
        {
            var cache = GetClientCache<int?, int>();

            cache[1] = 1;

            Assert.IsTrue(cache.ContainsKey(1));
            Assert.IsFalse(cache.ContainsKey(2));

            Assert.Throws<ArgumentNullException>(() => cache.ContainsKey(null));
        }

        /// <summary>
        /// Tests the ContainsKeys method.
        /// </summary>
        [Test]
        public void TestContainsKeys()
        {
            var cache = GetClientCache<int, int>();

            cache[1] = 1;
            cache[2] = 2;
            cache[3] = 3;

            Assert.IsTrue(cache.ContainsKeys(new[] {1}));
            Assert.IsTrue(cache.ContainsKeys(new[] {1, 2}));
            Assert.IsTrue(cache.ContainsKeys(new[] {2, 1}));
            Assert.IsTrue(cache.ContainsKeys(new[] {1, 2, 3}));
            Assert.IsTrue(cache.ContainsKeys(new[] {1, 3, 2}));

            Assert.IsFalse(cache.ContainsKeys(new[] {0}));
            Assert.IsFalse(cache.ContainsKeys(new[] {0, 1}));
            Assert.IsFalse(cache.ContainsKeys(new[] {1, 0}));
            Assert.IsFalse(cache.ContainsKeys(new[] {1, 2, 3, 0}));

            Assert.Throws<ArgumentNullException>(() => cache.ContainsKeys(null));
        }

        /// <summary>
        /// Tests the PutIfAbsent method.
        /// </summary>
        [Test]
        public void TestPutIfAbsent()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.PutIfAbsent(1, 1);
            Assert.IsTrue(res);
            Assert.AreEqual(1, cache[1]);

            res = cache.PutIfAbsent(1, 2);
            Assert.IsFalse(res);
            Assert.AreEqual(1, cache[1]);

            Assert.Throws<ArgumentNullException>(() => cache.PutIfAbsent(null, 1));
            Assert.Throws<ArgumentNullException>(() => cache.PutIfAbsent(1, null));
        }

        /// <summary>
        /// Tests the GetAndPutIfAbsent method.
        /// </summary>
        [Test]
        public void TestGetAndPutIfAbsent()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndPutIfAbsent(1, 1);
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);
            Assert.AreEqual(1, cache[1]);

            res = cache.GetAndPutIfAbsent(1, 2);
            Assert.IsTrue(res.Success);
            Assert.AreEqual(1, res.Value);
            Assert.AreEqual(1, cache[1]);

            Assert.Throws<ArgumentNullException>(() => cache.GetAndPutIfAbsent(null, 1));
            Assert.Throws<ArgumentNullException>(() => cache.GetAndPutIfAbsent(1, null));
        }

        /// <summary>
        /// Tests the Replace method.
        /// </summary>
        [Test]
        public void TestReplace()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.Replace(1, 1);
            Assert.IsFalse(res);
            Assert.IsFalse(cache.ContainsKey(1));

            cache[1] = 1;

            res = cache.Replace(1, 2);
            Assert.IsTrue(res);
            Assert.AreEqual(2, cache[1]);

            Assert.Throws<ArgumentNullException>(() => cache.Replace(null, 1));
            Assert.Throws<ArgumentNullException>(() => cache.Replace(1, null));
        }

        /// <summary>
        /// Tests the Replace overload with additional argument.
        /// </summary>
        [Test]
        public void TestReplaceIfEquals()
        {
            var cache = GetClientCache<int?, int?>();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.Replace(1, 1, 2);
            Assert.IsFalse(res);
            Assert.IsFalse(cache.ContainsKey(1));

            cache[1] = 1;

            res = cache.Replace(1, -1, 2);
            Assert.IsFalse(res);
            Assert.AreEqual(1, cache[1]);

            res = cache.Replace(1, 1, 2);
            Assert.IsTrue(res);
            Assert.AreEqual(2, cache[1]);

            Assert.Throws<ArgumentNullException>(() => cache.Replace(null, 1, 1));
            Assert.Throws<ArgumentNullException>(() => cache.Replace(1, null, 1));
            Assert.Throws<ArgumentNullException>(() => cache.Replace(1, 1, null));
        }

        /// <summary>
        /// Tests the PutAll method.
        /// </summary>
        [Test]
        public void TestPutAll()
        {
            using (var client = GetClient())
            {
                // Primitives.
                var cache = GetClientCache<int?, int?>();

                cache.PutAll(Enumerable.Range(1, 3).ToDictionary(x => (int?) x, x => (int?) x + 1));

                Assert.AreEqual(2, cache[1]);
                Assert.AreEqual(3, cache[2]);
                Assert.AreEqual(4, cache[3]);

                // Objects.
                var cache2 = client.GetCache<int, Container>(CacheName);

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

                // Huge data set.
                var cache3 = client.GetCache<int, Person>(CacheName);
                const int count = 30000;

                cache3.PutAll(Enumerable.Range(1, count).ToDictionary(x => x, x => new Person(x)));
                Assert.AreEqual(count, cache3.GetSize());

                // Nulls.
                Assert.Throws<ArgumentNullException>(() => cache.PutAll(null));

                Assert.Throws<IgniteClientException>(() => cache.PutAll(new[]
                {
                    new KeyValuePair<int?, int?>(null, 1)
                }));

                Assert.Throws<IgniteClientException>(() => cache.PutAll(new[]
                {
                    new KeyValuePair<int?, int?>(1, null)
                }));
            }
        }

        /// <summary>
        /// Tests the Clear method.
        /// </summary>
        [Test]
        public void TestClear()
        {
            var cache = GetClientCache<int?, int?>();

            cache[1] = 1;
            cache[2] = 2;

            cache.Clear();

            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsFalse(cache.ContainsKey(2));
        }

        /// <summary>
        /// Tests the Clear method with a key argument.
        /// </summary>
        [Test]
        public void TestClearKey()
        {
            var cache = GetClientCache<int?, int?>();

            cache[1] = 1;
            cache[2] = 2;

            cache.Clear(1);
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsTrue(cache.ContainsKey(2));

            cache.Clear(2);
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsFalse(cache.ContainsKey(2));

            Assert.Throws<ArgumentNullException>(() => cache.Clear(null));
        }

        /// <summary>
        /// Tests the ClearAll method.
        /// </summary>
        [Test]
        public void TestClearAll()
        {
            var cache = GetClientCache<int?, int?>();

            cache[1] = 1;
            cache[2] = 2;
            cache[3] = 3;

            cache.ClearAll(new int?[] {1, 3});
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsTrue(cache.ContainsKey(2));
            Assert.IsFalse(cache.ContainsKey(3));

            Assert.Throws<ArgumentNullException>(() => cache.ClearAll(null));
            Assert.Throws<IgniteClientException>(() => cache.ClearAll(new int?[] {null, 1}));
        }

        /// <summary>
        /// Tests the Remove method.
        /// </summary>
        [Test]
        public void TestRemove()
        {
            var cache = GetClientCache<int?, int?>();

            cache[1] = 1;
            cache[2] = 2;

            var res = cache.Remove(1);
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsTrue(cache.ContainsKey(2));

            res = cache.Remove(2);
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsFalse(cache.ContainsKey(2));

            res = cache.Remove(-1);
            Assert.IsFalse(res);

            Assert.Throws<ArgumentNullException>(() => cache.Remove(null));
        }

        /// <summary>
        /// Tests the Remove method with value argument.
        /// </summary>
        [Test]
        public void TestRemoveKeyVal()
        {
            var cache = GetClientCache<int?, int?>();

            cache[1] = 1;
            cache[2] = 2;

            var res = cache.Remove(1, 0);
            Assert.IsFalse(res);

            res = cache.Remove(0, 0);
            Assert.IsFalse(res);

            res = cache.Remove(1, 1);
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsTrue(cache.ContainsKey(2));

            res = cache.Remove(2, 2);
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(1));
            Assert.IsFalse(cache.ContainsKey(2));

            res = cache.Remove(2, 2);
            Assert.IsFalse(res);

            Assert.Throws<ArgumentNullException>(() => cache.Remove(1, null));
            Assert.Throws<ArgumentNullException>(() => cache.Remove(null, 1));
        }

        /// <summary>
        /// Tests the RemoveAll with a set of keys.
        /// </summary>
        [Test]
        public void TestRemoveKeys()
        {
            var cache = GetClientCache<int?, int?>();
            var keys = Enumerable.Range(1, 10).Cast<int?>().ToArray();

            cache.PutAll(keys.ToDictionary(x => x, x => x));

            cache.RemoveAll(keys.Skip(2));
            CollectionAssert.AreEquivalent(keys.Take(2), cache.GetAll(keys).Select(x => x.Key));

            cache.RemoveAll(new int?[] {1});
            Assert.AreEqual(2, cache.GetAll(keys).Single().Value);

            cache.RemoveAll(keys);
            cache.RemoveAll(keys);

            Assert.AreEqual(0, cache.GetSize());

            Assert.Throws<ArgumentNullException>(() => cache.RemoveAll(null));
            Assert.Throws<IgniteClientException>(() => cache.RemoveAll(new int?[] {1, null}));
        }

        /// <summary>
        /// Tests the RemoveAll method without argument.
        /// </summary>
        [Test]
        public void TestRemoveAll()
        {
            using (var client = GetClient())
            {
                var cache = client.GetCache<int, int>(CacheName);

                cache[1] = 1;
                cache[2] = 2;

                cache.RemoveAll();

                Assert.AreEqual(0, cache.GetSize());
            }
        }

        /// <summary>
        /// Tests the GetSize method.
        /// </summary>
        [Test]
        public void TestGetSize()
        {
            var cache = GetClientCache<int>();

            Assert.AreEqual(0, cache.GetSize());
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.All));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Backup));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Near));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Offheap));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Onheap));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Primary));

            cache[1] = 1;

            Assert.AreEqual(1, cache.GetSize());
            Assert.AreEqual(1, cache.GetSize(CachePeekMode.All));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Backup));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Near));
            Assert.AreEqual(1, cache.GetSize(CachePeekMode.Offheap));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Onheap));
            Assert.AreEqual(1, cache.GetSize(CachePeekMode.Primary));

            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));

            Assert.AreEqual(100, cache.GetSize());
            Assert.AreEqual(100, cache.GetSize(CachePeekMode.All));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Backup));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Near));
            Assert.AreEqual(100, cache.GetSize(CachePeekMode.Offheap));
            Assert.AreEqual(0, cache.GetSize(CachePeekMode.Onheap));
            Assert.AreEqual(100, cache.GetSize(CachePeekMode.Primary));
        }

        /// <summary>
        /// Tests client get in multiple threads with a single client.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestGetMultithreadedSingleClient()
        {
            GetCache<string>().Put(1, "foo");

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, string>(CacheName);

                TestUtils.RunMultiThreaded(() => Assert.AreEqual("foo", clientCache.Get(1)),
                    Environment.ProcessorCount, 5);
            }
        }

        /// <summary>
        /// Tests client get in multiple threads with multiple clients.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestGetMultithreadedMultiClient()
        {
            GetCache<string>().Put(1, "foo");

            // One client per thread.
            var clients = new ConcurrentDictionary<int, IIgniteClient>();

            TestUtils.RunMultiThreaded(() =>
                {
                    var client = clients.GetOrAdd(Thread.CurrentThread.ManagedThreadId, _ => GetClient());

                    var clientCache = client.GetCache<int, string>(CacheName);

                    Assert.AreEqual("foo", clientCache.Get(1));
                }, 4, 5);

            clients.ToList().ForEach(x => x.Value.Dispose());
        }

        /// <summary>
        /// Tests interleaved put/get operations.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPutGetAsyncMultithreaded()
        {
            const int count = 5000;

            var cache = GetClientCache<string>();
            var key = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                for (var i = 0; i < count; i++)
                {
                    // ReSharper disable once AccessToModifiedClosure
                    var k = Interlocked.Increment(ref key);
                    var v = k + "_" + Guid.NewGuid();

                    if (k % 3 == 0)
                    {
                        cache.Put(k, v);
                    }
                    else
                    {
                        cache.PutAsync(k, v);
                    }
                }
            }, Environment.ProcessorCount * 2);

            key = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                for (var i = 0; i < count; i++)
                {
                    var k = Interlocked.Increment(ref key);
                    var val = k % 3 == 0 ? cache.Get(k) : cache.GetAsync(k).Result;

                    Assert.IsTrue(val.Split('_').First() == k.ToString());
                }
            }, Environment.ProcessorCount * 2);
        }

        /// <summary>
        /// Tests that long operation completes later than short operation.
        /// </summary>
        [Test]
        public void TestAsyncCompletionOrder()
        {
            var config = GetClientConfiguration();
            config.SocketTimeout = TimeSpan.FromMinutes(2);
            using (var client = Ignition.StartClient(config))
            {
                var cache = client.GetCache<int, int>(CacheName);
                var cache2 = Client.GetOrCreateCache<int, int>("TestAsyncCompletionOrder");

                cache.PutAll(Enumerable.Range(1, 50000).Select(x => new KeyValuePair<int, int>(x, x)));
                var t1 = cache.RemoveAllAsync();
                var t2 = cache2.PutAsync(1, 1);

                t2.Wait();
                Assert.IsFalse(t1.IsCompleted);

                t1.Wait();
            }
        }

        /// <summary>
        /// Tests the cache exceptions.
        /// </summary>
        [Test]
        public void TestExceptions()
        {
            // Getting the cache instance does not throw.
            var cache = GetClientCache<int, int>("foobar");

            // Accessing non-existent cache throws.
            var ex = Assert.Throws<IgniteClientException>(() => cache.Put(1, 1));

            Assert.AreEqual("Cache doesn't exist: foobar", ex.Message);
            Assert.AreEqual(ClientStatusCode.CacheDoesNotExist, ex.StatusCode);
        }

        /// <summary>
        /// Tests various cache names.
        /// Cache id as calculated as a hash code and passed to the server side; this test verifies correct id
        /// calculation for different strings.
        /// </summary>
        [Test]
        public void TestCacheNames()
        {
            var cacheNames = new[]
            {
                "foo-bar",
                "Foo-Bar",
                "FOO-BAR",
                "testCache1",
                "TestCache2",
                "TESTCACHE3",
                new string('c', 100),
                new string('C', 100),
                Guid.NewGuid().ToString(),
                "тест",
                "Тест",
                "ТЕСТ",
                "тест1",
                "Тест2",
                "ТЕСТ3"
            };

            var ignite = Ignition.GetIgnite();

            for (var i = 0; i < cacheNames.Length; i++)
            {
                var cacheName = cacheNames[i];
                ignite.CreateCache<int, string>(cacheName).Put(i, cacheName);

                using (var client = GetClient())
                {
                    var cache = client.GetCache<int, string>(cacheName);
                    Assert.AreEqual(cacheName, cache[i]);
                }
            }
        }

        /// <summary>
        /// Test cache with expiry policy for Create action.
        /// </summary>
        [Test]
        public void TestCacheWithExpiryPolicyOnCreate()
        {
            const int val = 3;
            var expiryPolicy = new ExpiryPolicy(TimeSpan.FromMilliseconds(100), null, null);
            var cache = GetClientCache<int>();
            var cacheWithExpiryPolicy = cache.WithExpiryPolicy(expiryPolicy);

            cacheWithExpiryPolicy.Put(val, val);

            // Initially added value is the same.
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            // Wait for an expiration.
            Thread.Sleep(200);

            // Expiry policies should be applied, no cache item exists.
            Assert.IsFalse(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsFalse(cache.ContainsKey(val));
        }

        /// <summary>
        /// Test cache with expiry policy for Update enabled.
        /// </summary>
        [Test]
        public void TestCacheWithExpiryPolicyOnUpdate()
        {
            const int val = 4;
            var expiryPolicy = new ExpiryPolicy(null, TimeSpan.FromMilliseconds(100), null);
            var cache = GetClientCache<int>();
            var cacheWithExpiryPolicy = cache.WithExpiryPolicy(expiryPolicy);

            cacheWithExpiryPolicy.Put(val, val);
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            Thread.Sleep(50);
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            cacheWithExpiryPolicy.Put(val, val + 1);

            Thread.Sleep(70);
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            Thread.Sleep(50);
            
            // Expiry policies should be applied, no cache item exists.
            Assert.IsFalse(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsFalse(cache.ContainsKey(val));
        }

        /// <summary>
        /// Test cache with expiry policy for Access enabled.
        /// </summary>
        [Test]
        public void TestCacheWithExpiryPolicyOnAccess()
        {
            const int val = 6;
            var expiryPolicy = new ExpiryPolicy(null, null, TimeSpan.FromMilliseconds(200));
            var cache = GetClientCache<int>();
            var cacheWithExpiryPolicy = cache.WithExpiryPolicy(expiryPolicy);

            cacheWithExpiryPolicy.Put(val, val);
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            Thread.Sleep(100);
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            cacheWithExpiryPolicy.Get(val);

            Thread.Sleep(150);
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsTrue(cache.ContainsKey(val));

            Thread.Sleep(150);

            // Expiry policies should be applied, no cache item exists.
            Assert.IsFalse(cacheWithExpiryPolicy.ContainsKey(val));
            Assert.IsFalse(cache.ContainsKey(val));
        }

        /// <summary>
        /// Test cache with expiration does not affect original cache.
        /// </summary>
        [Test]
        public void TestCacheWithExpirationHasIsolatedScope()
        {
            const int val = 7;
            var expiryPolicy = new ExpiryPolicy(TimeSpan.FromMilliseconds(100), null, null);
            var cache = GetClientCache<int>();
            var cacheWithExpiryPolicy = cache.WithExpiryPolicy(expiryPolicy);

            cache.Put(val, val);
            cacheWithExpiryPolicy.Put(val + 1, val);

            Thread.Sleep(200);
            
            // Both caches contains the original value.
            Assert.IsTrue(cache.ContainsKey(val));
            Assert.IsTrue(cacheWithExpiryPolicy.ContainsKey(val));

            // New value is being absent for both caches.
            Assert.IsFalse(cache.ContainsKey(val + 1));
            Assert.IsFalse(cacheWithExpiryPolicy.ContainsKey(val + 1));
        }
        
        /// <summary>
        /// Test cache with expiration does not modify keepBinary flag.
        /// </summary>
        [Test]
        public void TestCacheWithExpirationDoesNotAffectKeepBinarySettings()
        {
            const int key = 10;
            var person = new Person(1);

            var cache = GetClientCache<Person>();
            cache.Put(key, person);

            var cacheWithKeepBinary = cache.WithKeepBinary<int, IBinaryObject>();
            AssertExtensions.ReflectionEqual(person, cacheWithKeepBinary.Get(key).Deserialize<Person>());

            var expiryPolicy = new ExpiryPolicy(null, null, TimeSpan.FromMilliseconds(100));
            
            var cacheWithExpiryPolicy = cacheWithKeepBinary.WithExpiryPolicy(expiryPolicy);
            AssertExtensions.ReflectionEqual(person, cacheWithExpiryPolicy.Get(key).Deserialize<Person>());

            Thread.Sleep(200);

            Assert.IsFalse(cacheWithExpiryPolicy.ContainsKey(key));
            Assert.IsFalse(cache.ContainsKey(key));
        }

        /// <summary>
        /// Test cache with keepBinary does not modify expiry policy settings.
        /// </summary>
        [Test]
        public void TestCacheWithKeepBinaryDoesNotAffectExpirationPolicy()
        {
            const int key = 11;
            var person = new Person(1);

            var expiryPolicy = new ExpiryPolicy(null, null, TimeSpan.FromMilliseconds(100));
            var cacheWithExpiryPolicy = GetClientCache<Person>().WithExpiryPolicy(expiryPolicy);

            cacheWithExpiryPolicy.Put(key, person);

            var cacheWithKeepBinary = cacheWithExpiryPolicy.WithKeepBinary<int, IBinaryObject>();
            AssertExtensions.ReflectionEqual(person, cacheWithKeepBinary.Get(key).Deserialize<Person>());

            Thread.Sleep(200);

            Assert.IsFalse(cacheWithKeepBinary.ContainsKey(key));
            Assert.IsFalse(cacheWithExpiryPolicy.ContainsKey(key));
        }

        private class Container
        {
            public Container Inner;
        }

        public enum ByteEnum : byte
        {
            One = 1,
            Two = 2,
        }
    }
}

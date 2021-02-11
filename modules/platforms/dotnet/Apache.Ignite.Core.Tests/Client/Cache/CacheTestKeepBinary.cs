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
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Thin client cache test in binary mode.
    /// </summary>
    public sealed class CacheTestKeepBinary : ClientTestBase
    {
        /// <summary>
        /// Tests the cache put / get with primitive data types.
        /// </summary>
        [Test]
        public void TestPutGetPrimitives()
        {
            using (var client = GetClient())
            {
                GetCache<string>().Put(1, "foo");

                var clientCache = client.GetCache<int?, string>(CacheName)
                    .WithKeepBinary<int?, string>();

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
                var intCache = client.GetCache<int?, int?>(CacheName);
                intCache.Put(1, 0);
                Assert.AreEqual(0, intCache.Get(1));
            }
        }

        /// <summary>
        /// Tests the cache put / get for Empty object type.
        /// </summary>
        [Test]
        public void TestPutGetEmptyObject()
        {
            using (var client = GetClient())
            {
                var serverCache = GetCache<EmptyObject>();
                var clientCache = client.GetCache<int, EmptyObject>(CacheName)
                    .WithKeepBinary<int, IBinaryObject>();

                serverCache.Put(1, new EmptyObject());
                
                var res = clientCache.Get(1);
                Assert.IsNotNull(res);
                Assert.IsInstanceOf<EmptyObject>(res.Deserialize<object>());
            }
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
                var clientCache = client.GetCache<int?, Person>(CacheName)
                    .WithKeepBinary<int?, IBinaryObject>();

                Assert.AreEqual(CacheName, clientCache.Name);

                // Put through server cache.
                serverCache.Put(1, person);

                // Put through client cache.
                var binPerson = client.GetBinary().ToBinary<IBinaryObject>(person2);
                clientCache.Put(2, binPerson);

                // Read from client cache.
                Assert.AreEqual("foo", clientCache.Get(1).GetField<string>("Name"));
                Assert.AreEqual(100, clientCache[1].GetField<int>("Id"));
                Assert.AreEqual(200, clientCache[2].GetField<int>("Id"));

                // Read from server cache.
                Assert.AreEqual("foo", serverCache.Get(1).Name);
                Assert.AreEqual(100, serverCache[1].Id);
                Assert.AreEqual(200, serverCache[2].Id);

                // Null key or value.
                Assert.Throws<ArgumentNullException>(() => clientCache.Put(10, null));
                Assert.Throws<ArgumentNullException>(() => clientCache.Put(null, binPerson));
            }
        }

        /// <summary>
        /// Tests the GetAll method.
        /// </summary>
        [Test]
        public void TestGetAll()
        {
            var cache = GetBinaryCache();
            cache[1] = GetBinaryPerson(1);
            cache[2] = GetBinaryPerson(2);

            var res = cache.GetAll(new [] {1}).Single();
            Assert.AreEqual(1, res.Key);
            Assert.AreEqual(1, res.Value.GetField<int>("Id"));

            res = cache.GetAll(new [] {1, -1}).Single();
            Assert.AreEqual(1, res.Key);
            Assert.AreEqual(1, res.Value.GetField<int>("Id"));

            CollectionAssert.AreEquivalent(new[] {1, 2}, 
                cache.GetAll(new [] {1, 2, 3}).Select(x => x.Value.GetField<int>("Id")));
        }

        /// <summary>
        /// Tests the GetAndPut method.
        /// </summary>
        [Test]
        public void TestGetAndPut()
        {
            var cache = GetBinaryCache();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndPut(1, GetBinaryPerson(1));
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);

            Assert.IsTrue(cache.ContainsKey(1));

            res = cache.GetAndPut(1, GetBinaryPerson(2));
            Assert.IsTrue(res.Success);
            Assert.AreEqual(GetBinaryPerson(1), res.Value);

            Assert.AreEqual(GetBinaryPerson(2), cache[1]);
        }

        /// <summary>
        /// Tests the GetAndReplace method.
        /// </summary>
        [Test]
        public void TestGetAndReplace()
        {
            var cache = GetBinaryCache();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndReplace(1, GetBinaryPerson(1));
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);

            Assert.IsFalse(cache.ContainsKey(1));
            cache[1] = GetBinaryPerson(1);

            res = cache.GetAndReplace(1, GetBinaryPerson(2));
            Assert.IsTrue(res.Success);
            Assert.AreEqual(GetBinaryPerson(1), res.Value);

            Assert.AreEqual(GetBinaryPerson(2), cache[1]);
        }

        /// <summary>
        /// Tests the GetAndRemove method.
        /// </summary>
        [Test]
        public void TestGetAndRemove()
        {
            var cache = GetBinaryCache();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.GetAndRemove(1);
            Assert.IsFalse(res.Success);
            Assert.IsNull(res.Value);

            Assert.IsFalse(cache.ContainsKey(1));
            cache[1] = GetBinaryPerson(1);

            res = cache.GetAndRemove(1);
            Assert.IsTrue(res.Success);
            Assert.AreEqual(GetBinaryPerson(1), res.Value);

            Assert.IsFalse(cache.ContainsKey(1));
        }

        /// <summary>
        /// Tests the ContainsKey method.
        /// </summary>
        [Test]
        public void TestContainsKey()
        {
            var cache = GetBinaryKeyCache();

            cache[GetBinaryPerson(25)] = 1;

            Assert.IsTrue(cache.ContainsKey(GetBinaryPerson(25)));
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(26)));

            Assert.Throws<ArgumentNullException>(() => cache.ContainsKey(null));
        }

        /// <summary>
        /// Tests the ContainsKeys method.
        /// </summary>
        [Test]
        public void TestContainsKeys()
        {
            var cache = GetBinaryKeyCache();

            cache[GetBinaryPerson(1)] = 1;
            cache[GetBinaryPerson(2)] = 2;
            cache[GetBinaryPerson(3)] = 3;

            Assert.IsTrue(cache.ContainsKeys(new[] {GetBinaryPerson(1)}));
            Assert.IsTrue(cache.ContainsKeys(new[] {GetBinaryPerson(1), GetBinaryPerson(2)}));
            Assert.IsTrue(cache.ContainsKeys(new[] {GetBinaryPerson(2), GetBinaryPerson(1)}));
            Assert.IsTrue(cache.ContainsKeys(new[] {GetBinaryPerson(1), GetBinaryPerson(2), GetBinaryPerson(3)}));
            Assert.IsTrue(cache.ContainsKeys(new[] {GetBinaryPerson(1), GetBinaryPerson(3), GetBinaryPerson(2)}));

            Assert.IsFalse(cache.ContainsKeys(new[] { GetBinaryPerson(0) }));
            Assert.IsFalse(cache.ContainsKeys(new[] { GetBinaryPerson(0), GetBinaryPerson(1) }));
            Assert.IsFalse(cache.ContainsKeys(new[] { GetBinaryPerson(1), GetBinaryPerson(0) }));
            Assert.IsFalse(cache.ContainsKeys(new[]
                {GetBinaryPerson(1), GetBinaryPerson(3), GetBinaryPerson(2), GetBinaryPerson(0)}));

            Assert.Throws<ArgumentNullException>(() => cache.ContainsKeys(null));
        }

        /// <summary>
        /// Tests the PutIfAbsent method.
        /// </summary>
        [Test]
        public void TestPutIfAbsent()
        {
            var cache = GetBinaryKeyCache();

            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));

            var res = cache.PutIfAbsent(GetBinaryPerson(1), 1);
            Assert.IsTrue(res);
            Assert.AreEqual(1, cache[GetBinaryPerson(1)]);

            res = cache.PutIfAbsent(GetBinaryPerson(1), 2);
            Assert.IsFalse(res);
            Assert.AreEqual(1, cache[GetBinaryPerson(1)]);

            Assert.Throws<ArgumentNullException>(() => cache.PutIfAbsent(null, 1));
        }

        /// <summary>
        /// Tests the GetAndPutIfAbsent method.
        /// </summary>
        [Test]
        public void TestGetAndPutIfAbsent()
        {
            var cache = GetBinaryKeyCache();

            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));

            var res = cache.GetAndPutIfAbsent(GetBinaryPerson(1), 1);
            Assert.IsFalse(res.Success);
            Assert.AreEqual(0, res.Value);
            Assert.AreEqual(1, cache[GetBinaryPerson(1)]);

            res = cache.GetAndPutIfAbsent(GetBinaryPerson(1), 2);
            Assert.IsTrue(res.Success);
            Assert.AreEqual(1, res.Value);
            Assert.AreEqual(1, cache[GetBinaryPerson(1)]);

            Assert.Throws<ArgumentNullException>(() => cache.GetAndPutIfAbsent(null, 1));
        }

        /// <summary>
        /// Tests the Replace method.
        /// </summary>
        [Test]
        public void TestReplace()
        {
            var cache = GetBinaryKeyCache();
            
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));

            var res = cache.Replace(GetBinaryPerson(1), 1);
            Assert.IsFalse(res);
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));

            cache[GetBinaryPerson(1)] = 1;

            res = cache.Replace(GetBinaryPerson(1), 2);
            Assert.IsTrue(res);
            Assert.AreEqual(2, cache[GetBinaryPerson(1)]);

            Assert.Throws<ArgumentNullException>(() => cache.Replace(null, 1));
        }

        /// <summary>
        /// Tests the Replace overload with additional argument.
        /// </summary>
        [Test]
        [Ignore("IGNITE-7072")]
        public void TestReplaceIfEquals()
        {
            var cache = GetBinaryCache();

            Assert.IsFalse(cache.ContainsKey(1));

            var res = cache.Replace(1, GetBinaryPerson(1), GetBinaryPerson(2));
            Assert.IsFalse(res);
            Assert.IsFalse(cache.ContainsKey(1));

            cache[1] = GetBinaryPerson(1);

            res = cache.Replace(1, GetBinaryPerson(-1), GetBinaryPerson(2));
            Assert.IsFalse(res);
            Assert.AreEqual(1, cache[1]);

            res = cache.Replace(1, GetBinaryPerson(1), GetBinaryPerson(2));
            Assert.IsTrue(res);
            Assert.AreEqual(GetBinaryPerson(2), cache[1]);
        }

        /// <summary>
        /// Tests the PutAll method.
        /// </summary>
        [Test]
        public void TestPutAll()
        {
            var cache = GetBinaryCache();

            var keys = Enumerable.Range(1, 10).ToArray();
            cache.PutAll(keys.ToDictionary(x => x, GetBinaryPerson));

            Assert.AreEqual(keys, cache.GetAll(keys).Select(x => x.Value.GetField<int>("Id")));
        }

        /// <summary>
        /// Tests the Clear method with a key argument.
        /// </summary>
        [Test]
        public void TestClearKey()
        {
            var cache = GetBinaryKeyCache();

            cache[GetBinaryPerson(1)] = 1;
            cache[GetBinaryPerson(2)] = 2;

            cache.Clear(GetBinaryPerson(1));
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));
            Assert.IsTrue(cache.ContainsKey(GetBinaryPerson(2)));

            cache.Clear(GetBinaryPerson(2));
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(2)));
        }

        /// <summary>
        /// Tests the Remove method.
        /// </summary>
        [Test]
        public void TestRemove()
        {
            var cache = GetBinaryKeyCache();

            cache[GetBinaryPerson(1)] = 1;
            cache[GetBinaryPerson(2)] = 2;

            var res = cache.Remove(GetBinaryPerson(1));
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));
            Assert.IsTrue(cache.ContainsKey(GetBinaryPerson(2)));

            res = cache.Remove(GetBinaryPerson(2));
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(1)));
            Assert.IsFalse(cache.ContainsKey(GetBinaryPerson(2)));

            res = cache.Remove(GetBinaryPerson(-1));
            Assert.IsFalse(res);

            Assert.Throws<ArgumentNullException>(() => cache.Remove(null));
        }

        /// <summary>
        /// Tests the Remove method with value argument.
        /// </summary>
        [Test]
        [Ignore("IGNITE-7072")]
        public void TestRemoveKeyVal()
        {
            var cache = GetBinaryKeyValCache();

            var x = GetBinaryPerson(1);
            var y = GetBinaryPerson(2);
            var z = GetBinaryPerson(0);

            cache[x] = x;
            cache[y] = y;

            var res = cache.Remove(x, z);
            Assert.IsFalse(res);

            res = cache.Remove(z, z);
            Assert.IsFalse(res);

            res = cache.Remove(x, x);
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(x));
            Assert.IsTrue(cache.ContainsKey(y));

            res = cache.Remove(y, y);
            Assert.IsTrue(res);
            Assert.IsFalse(cache.ContainsKey(x));
            Assert.IsFalse(cache.ContainsKey(y));

            res = cache.Remove(y, y);
            Assert.IsFalse(res);
        }

        /// <summary>
        /// Tests the RemoveAll with a set of keys.
        /// </summary>
        [Test]
        public void TestRemoveKeys()
        {
            var cache = GetBinaryKeyCache();

            var ids = Enumerable.Range(1, 10).ToArray();
            var keys = ids.Select(GetBinaryPerson).ToArray();
            cache.PutAll(ids.ToDictionary(GetBinaryPerson, x => x));

            cache.RemoveAll(keys.Skip(2));
            CollectionAssert.AreEquivalent(keys.Take(2), cache.GetAll(keys).Select(x => x.Key));

            cache.RemoveAll(new[] {GetBinaryPerson(1)});
            Assert.AreEqual(2, cache.GetAll(keys).Single().Value);

            cache.RemoveAll(keys);
            cache.RemoveAll(keys);

            Assert.AreEqual(0, cache.GetSize());

            Assert.Throws<ArgumentNullException>(() => cache.RemoveAll(null));
        }

        /// <summary>
        /// Tests the WithKeepBinary logic.
        /// </summary>
        [Test]
        public void TestWithKeepBinary()
        {
            var cache = GetBinaryCache();
            var cache2 = cache.WithKeepBinary<int, IBinaryObject>();

            Assert.AreSame(cache, cache2);

            Assert.Throws<InvalidOperationException>(() => cache.WithKeepBinary<int, object>());
        }
    }
}
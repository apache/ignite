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
    using System.Net;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Thin client cache test.
    /// </summary>
    public sealed class CacheTest : ClientTestBase
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

                var clientCache = client.GetCache<int?, string>(CacheName);

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
                var clientCache = client.GetCache<int, EmptyObject>(CacheName);

                serverCache.Put(1, new EmptyObject());
                Assert.IsNotNull(clientCache.Get(1));
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
                },
                Environment.ProcessorCount, 5);

            clients.ToList().ForEach(x => x.Value.Dispose());
        }

        /// <summary>
        /// Tests the cache exceptions.
        /// </summary>
        [Test]
        public void TestExceptions()
        {
            using (var client = GetClient())
            {
                // Getting the cache instance does not throw.
                var cache = client.GetCache<int, int>("foobar");

                // Accessing non-existent cache throws.
                var ex = Assert.Throws<IgniteClientException>(() => cache.Put(1, 1));

                Assert.AreEqual("Cache doesn't exist: foobar", ex.Message);
                Assert.AreEqual((int) ClientStatus.CacheDoesNotExist, ex.ErrorCode);
            }
        }
    }
}

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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests scan queries.
    /// </summary>
    public class ScanQueryTest : ClientTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQueryTest"/> class.
        /// </summary>
        public ScanQueryTest() : base(2)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var cfg = base.GetIgniteConfiguration();

            cfg.ClientConnectorConfiguration = new ClientConnectorConfiguration
            {
                MaxOpenCursorsPerConnection = 3
            };

            return cfg;
        }

        /// <summary>
        /// Tests scan query without filter.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed")]
        public void TestNoFilter()
        {
            var cache = GetPersonCache();

            Action<IEnumerable<ICacheEntry<int, Person>>> checkResults = e =>
            {
                Assert.AreEqual(cache.Select(x => x.Value.Name).OrderBy(x => x).ToArray(),
                    e.Select(x => x.Value.Name).OrderBy(x => x).ToArray());
            };

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var query = new ScanQuery<int, Person>();

                // GetAll.
                var cursor = clientCache.Query(query);
                checkResults(cursor.GetAll());

                // Can't iterate or call GetAll again.
                Assert.Throws<InvalidOperationException>(() => cursor.ToArray());
                Assert.Throws<InvalidOperationException>(() => cursor.GetAll());

                // Iterator.
                using (cursor = clientCache.Query(query))
                {
                    checkResults(cursor.ToArray());

                    // Can't iterate or call GetAll again.
                    Assert.Throws<InvalidOperationException>(() => cursor.ToArray());
                    Assert.Throws<InvalidOperationException>(() => cursor.GetAll());
                }

                // Partial iterator.
                using (cursor = clientCache.Query(query))
                {
                    var item = cursor.First();
                    Assert.AreEqual(item.Key.ToString(), item.Value.Name);
                }

                // Local.
                query.Local = true;
                var localRes = clientCache.Query(query).ToList();
                Assert.Less(localRes.Count, cache.GetSize());
            }
        }

        /// <summary>
        /// Tests scan query with .NET filter.
        /// </summary>
        [Test]
        public void TestWithFilter()
        {
            GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                // One result.
                var single = clientCache.Query(new ScanQuery<int, Person>(new PersonKeyFilter(3))).Single();
                Assert.AreEqual(3, single.Key);

#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0   // Serializing delegates is not supported on this platform.
                // Multiple results.
                var res = clientCache.Query(new ScanQuery<int, Person>(new PersonFilter(x => x.Name.Length == 1)))
                    .ToList();
                Assert.AreEqual(9, res.Count);

                // No results.
                res = clientCache.Query(new ScanQuery<int, Person>(new PersonFilter(x => x == null))).ToList();
                Assert.AreEqual(0, res.Count);
#endif
            }
        }

        /// <summary>
        /// Tests scan query with .NET filter in binary mode.
        /// </summary>
        [Test]
        public void TestWithFilterBinary()
        {
            GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);
                var binCache = clientCache.WithKeepBinary<int, IBinaryObject>();

                // One result.
                var single = binCache.Query(new ScanQuery<int, IBinaryObject>(new PersonIdFilterBinary(8))).Single();
                Assert.AreEqual(8, single.Key);
            }
        }


#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0  // Serializing delegates and exceptions is not supported on this platform.
        /// <summary>
        /// Tests the exception in filter.
        /// </summary>
        [Test]
        public void TestExceptionInFilter()
        {
            GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var qry = new ScanQuery<int, Person>(new PersonFilter(x =>
                {
                    throw new ArithmeticException("foo");
                }));

                var ex = Assert.Throws<IgniteClientException>(() => clientCache.Query(qry).GetAll());
                Assert.AreEqual("foo", ex.Message);
            }
        }
#endif

        /// <summary>
        /// Tests multiple cursors with the same client.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "GenericEnumeratorNotDisposed")]
        public void TestMultipleCursors()
        {
            var cache = GetPersonCache();

            using (var client = GetClient())
            {
                var clientCache = client.GetCache<int, Person>(CacheName);

                var qry = new ScanQuery<int, Person>();

                var cur1 = clientCache.Query(qry).GetEnumerator();
                var cur2 = clientCache.Query(qry).GetEnumerator();
                var cur3 = clientCache.Query(qry).GetEnumerator();

                // MaxCursors = 3
                var ex = Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));
                Assert.AreEqual("Too many open cursors", ex.Message.Substring(0, 21));
                Assert.AreEqual(ClientStatusCode.TooManyCursors, ex.StatusCode);

                var count = 0;

                while (cur1.MoveNext())
                {
                    count++;

                    Assert.IsTrue(cur2.MoveNext());
                    Assert.IsTrue(cur3.MoveNext());

                    Assert.IsNotNull(cur1.Current);
                    Assert.IsNotNull(cur2.Current);
                    Assert.IsNotNull(cur3.Current);

                    Assert.AreEqual(cur1.Current.Key, cur2.Current.Key);
                    Assert.AreEqual(cur1.Current.Key, cur3.Current.Key);
                }

                Assert.AreEqual(cache.GetSize(), count);

                // Old cursors were auto-closed on last page, we can open new cursors now.
                var c1 = clientCache.Query(qry);
                var c2 = clientCache.Query(qry);
                var c3 = clientCache.Query(qry);

                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

                // Close one of the cursors.
                c1.Dispose();
                c1 = clientCache.Query(qry);
                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

                // Close cursor via GetAll.
                c1.GetAll();
                c1 = clientCache.Query(qry);
                Assert.Throws<IgniteClientException>(() => clientCache.Query(qry));

                c1.Dispose();
                c2.Dispose();
                c3.Dispose();
            }
        }

        /// <summary>
        /// Gets the string cache.
        /// </summary>
        private static ICache<int, Person> GetPersonCache()
        {
            var cache = GetCache<Person>();

            cache.RemoveAll();
            cache.PutAll(Enumerable.Range(1, 10000).ToDictionary(x => x, x => new Person
            {
                Id = x,
                Name = x.ToString()
            }));

            return cache;
        }

        /// <summary>
        /// Person filter.
        /// </summary>
        private class PersonFilter : ICacheEntryFilter<int, Person>
        {
            /** Filter predicate. */
            private readonly Func<Person, bool> _filter;

            /// <summary>
            /// Initializes a new instance of the <see cref="PersonFilter"/> class.
            /// </summary>
            /// <param name="filter">The filter.</param>
            public PersonFilter(Func<Person, bool> filter)
            {
                Debug.Assert(filter != null);

                _filter = filter;
            }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return _filter(entry.Value);
            }
        }

        /// <summary>
        /// Person filter.
        /// </summary>
        private class PersonKeyFilter : ICacheEntryFilter<int, Person>
        {
            /** Key. */
            private readonly int _key;

            /// <summary>
            /// Initializes a new instance of the <see cref="PersonFilter"/> class.
            /// </summary>
            public PersonKeyFilter(int key)
            {
                _key = key;
            }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return entry.Key == _key;
            }
        }

        /// <summary>
        /// Person filter.
        /// </summary>
        private class PersonIdFilterBinary : ICacheEntryFilter<int, IBinaryObject>
        {
            /** Key. */
            private readonly int _id;

            /// <summary>
            /// Initializes a new instance of the <see cref="PersonFilter"/> class.
            /// </summary>
            public PersonIdFilterBinary(int id)
            {
                _id = id;
            }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, IBinaryObject> entry)
            {
                return entry.Value.GetField<int>("Id") == _id;
            }
        }
    }
}

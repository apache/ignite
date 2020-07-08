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

// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable MemberCanBePrivate.Global
namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Queries tests.
    /// </summary>
    public class CacheQueriesTest
    {
        /** Grid count. */
        private const int GridCnt = 2;

        /** Cache name. */
        private const string CacheName = "cache";

        /** Path to XML configuration. */
        private const string CfgPath = "Config\\cache-query.xml";

        /** Maximum amount of items in cache. */
        private const int MaxItemCnt = 100;

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void StartGrids()
        {
            for (int i = 0; i < GridCnt; i++)
            {
                Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    BinaryConfiguration = new BinaryConfiguration
                    {
                        NameMapper = GetNameMapper()
                    },
                    SpringConfigUrl = CfgPath,
                    IgniteInstanceName = "grid-" + i
                });
            }
        }
        
        /// <summary>
        /// Gets the name mapper.
        /// </summary>
        protected virtual IBinaryNameMapper GetNameMapper()
        {
            return new BinaryBasicNameMapper {IsSimpleName = false};
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
        public void AfterTest()
        {
            var cache = Cache();

            for (int i = 0; i < GridCnt; i++)
            {
                cache.Clear();

                Assert.IsTrue(cache.IsEmpty());
            }

            TestUtils.AssertHandleRegistryIsEmpty(300,
                Enumerable.Range(0, GridCnt).Select(x => Ignition.GetIgnite("grid-" + x)).ToArray());

            Console.WriteLine("Test finished: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        /// Gets the ignite.
        /// </summary>
        private static IIgnite GetIgnite()
        {
            return Ignition.GetIgnite("grid-0");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private static ICache<int, QueryPerson> Cache()
        {
            return GetIgnite().GetCache<int, QueryPerson>(CacheName);
        }

        /// <summary>
        /// Test arguments validation for SQL queries.
        /// </summary>
        [Test]
        public void TestValidationSql()
        {
#pragma warning disable 618
            // 1. No sql.
            Assert.Throws<ArgumentException>(() =>
                { Cache().Query(new SqlQuery(typeof(QueryPerson), null)); });

            // 2. No type.
            Assert.Throws<ArgumentException>(() =>
                { Cache().Query(new SqlQuery((string)null, "age >= 50")); });
#pragma warning restore 618
        }

        /// <summary>
        /// Test arguments validation for SQL fields queries.
        /// </summary>
        [Test]
        public void TestValidationSqlFields()
        {
            // 1. No sql.
            Assert.Throws<ArgumentException>(() => { Cache().Query(new SqlFieldsQuery(null)); });
        }

        /// <summary>
        /// Test arguments validation for TEXT queries.
        /// </summary>
        [Test]
        public void TestValidationText()
        {
            // 1. No text.
            Assert.Throws<ArgumentException>(() =>
                { Cache().Query(new TextQuery(typeof(QueryPerson), null)); });

            // 2. No type.
            Assert.Throws<ArgumentException>(() =>
                { Cache().Query(new TextQuery((string)null, "Ivanov")); });
        }

        /// <summary>
        /// Cursor tests.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed")]
        public void TestCursor()
        {
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(1, new QueryPerson("Petrov", 40));
            Cache().Put(1, new QueryPerson("Sidorov", 50));

#pragma warning disable 618
            SqlQuery qry = new SqlQuery(typeof(QueryPerson), "age >= 20");
#pragma warning restore 618

            // 1. Test GetAll().
            using (IQueryCursor<ICacheEntry<int, QueryPerson>> cursor = Cache().Query(qry))
            {
                cursor.GetAll();

                Assert.Throws<InvalidOperationException>(() => { cursor.GetAll(); });
                Assert.Throws<InvalidOperationException>(() => { cursor.GetEnumerator(); });
            }

            // 2. Test GetEnumerator.
            using (IQueryCursor<ICacheEntry<int, QueryPerson>> cursor = Cache().Query(qry))
            {
                cursor.GetEnumerator();

                Assert.Throws<InvalidOperationException>(() => { cursor.GetAll(); });
                Assert.Throws<InvalidOperationException>(() => { cursor.GetEnumerator(); });
            }
        }

        /// <summary>
        /// Test enumerator.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "UnusedVariable")]
        public void TestEnumerator()
        {
#pragma warning disable 618
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(2, new QueryPerson("Petrov", 40));
            Cache().Put(3, new QueryPerson("Sidorov", 50));
            Cache().Put(4, new QueryPerson("Unknown", 60));

            // 1. Empty result set.
            using (IQueryCursor<ICacheEntry<int, QueryPerson>> cursor =
                Cache().Query(new SqlQuery(typeof(QueryPerson), "age = 100")))
            {
                IEnumerator<ICacheEntry<int, QueryPerson>> e = cursor.GetEnumerator();

                Assert.Throws<InvalidOperationException>(() =>
                    { ICacheEntry<int, QueryPerson> entry = e.Current; });

                Assert.IsFalse(e.MoveNext());

                Assert.Throws<InvalidOperationException>(() =>
                    { ICacheEntry<int, QueryPerson> entry = e.Current; });

                Assert.Throws<NotSupportedException>(() => e.Reset());

                e.Dispose();
            }

            SqlQuery qry = new SqlQuery(typeof (QueryPerson), "age < 60");

            Assert.AreEqual(QueryBase.DefaultPageSize, qry.PageSize);

            // 2. Page size is bigger than result set.
            qry.PageSize = 4;
            CheckEnumeratorQuery(qry);

            // 3. Page size equal to result set.
            qry.PageSize = 3;
            CheckEnumeratorQuery(qry);

            // 4. Page size if less than result set.
            qry.PageSize = 2;
            CheckEnumeratorQuery(qry);
#pragma warning restore 618
        }

        /// <summary>
        /// Test SQL query arguments passing.
        /// </summary>
        [Test]
        public void TestSqlQueryArguments()
        {
#pragma warning disable 618
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(2, new QueryPerson("Petrov", 40));
            Cache().Put(3, new QueryPerson("Sidorov", 50));

            // 1. Empty result set.
            using (var cursor = Cache().Query(new SqlQuery(typeof(QueryPerson), "age < ?", 50)))
            {
                foreach (ICacheEntry<int, QueryPerson> entry in cursor.GetAll())
                    Assert.IsTrue(entry.Key == 1 || entry.Key == 2);
            }
#pragma warning restore 618
        }

        /// <summary>
        /// Test SQL fields query arguments passing.
        /// </summary>
        [Test]
        public void TestSqlFieldsQueryArguments()
        {
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(2, new QueryPerson("Petrov", 40));
            Cache().Put(3, new QueryPerson("Sidorov", 50));

            // 1. Empty result set.
            using (var cursor = Cache().Query(new SqlFieldsQuery("SELECT age FROM QueryPerson WHERE age < ?", 50)))
            {
                foreach (var entry in cursor.GetAll())
                    Assert.IsTrue((int) entry[0] < 50);
            }
        }

        /// <summary>
        /// Check query result for enumerator test.
        /// </summary>
        /// <param name="qry">QUery.</param>
#pragma warning disable 618
        private void CheckEnumeratorQuery(SqlQuery qry)
        {
            using (IQueryCursor<ICacheEntry<int, QueryPerson>> cursor = Cache().Query(qry))
            {
                bool first = false;
                bool second = false;
                bool third = false;

                foreach (var entry in cursor)
                {
                    if (entry.Key == 1)
                    {
                        first = true;

                        Assert.AreEqual("Ivanov", entry.Value.Name);
                        Assert.AreEqual(30, entry.Value.Age);
                    }
                    else if (entry.Key == 2)
                    {
                        second = true;

                        Assert.AreEqual("Petrov", entry.Value.Name);
                        Assert.AreEqual(40, entry.Value.Age);
                    }
                    else if (entry.Key == 3)
                    {
                        third = true;

                        Assert.AreEqual("Sidorov", entry.Value.Name);
                        Assert.AreEqual(50, entry.Value.Age);
                    }
                    else
                        Assert.Fail("Unexpected value: " + entry);
                }

                Assert.IsTrue(first && second && third);
            }
        }
#pragma warning restore 618

        /// <summary>
        /// Check SQL query.
        /// </summary>
        [Test]
        public void TestSqlQuery([Values(true, false)] bool loc, [Values(true, false)] bool keepBinary, 
            [Values(true, false)] bool distrJoin)
        {
            var cache = Cache();

            // 1. Populate cache with data, calculating expected count in parallel.
            var exp = PopulateCache(cache, loc, MaxItemCnt, x => x < 50);

            // 2. Validate results.
#pragma warning disable 618
            var qry = new SqlQuery(typeof(QueryPerson), "age < 50", loc)
            {
                EnableDistributedJoins = distrJoin,
                ReplicatedOnly = false,
                Timeout = TimeSpan.FromSeconds(3)
            };
#pragma warning restore 618

            Assert.AreEqual(string.Format("SqlQuery [Sql=age < 50, Arguments=[], Local={0}, " +
                                          "PageSize=1024, EnableDistributedJoins={1}, Timeout={2}, " +
                                          "ReplicatedOnly=False]", loc, distrJoin, qry.Timeout), qry.ToString());

            ValidateQueryResults(cache, qry, exp, keepBinary);
        }

        /// <summary>
        /// Check SQL fields query.
        /// </summary>
        [Test]
        public void TestSqlFieldsQuery([Values(true, false)] bool loc, [Values(true, false)] bool distrJoin, 
            [Values(true, false)] bool enforceJoinOrder, [Values(true, false)] bool lazy)
        {
            int cnt = MaxItemCnt;

            var cache = Cache();

            // 1. Populate cache with data, calculating expected count in parallel.
            var exp = PopulateCache(cache, loc, cnt, x => x < 50);

            // 2. Validate results.
            var qry = new SqlFieldsQuery("SELECT name, age FROM QueryPerson WHERE age < 50")
            {
                EnableDistributedJoins = distrJoin,
                EnforceJoinOrder = enforceJoinOrder,
                Colocated = !distrJoin,
#pragma warning disable 618
                ReplicatedOnly = false,
#pragma warning restore 618
                Local = loc,
                Timeout = TimeSpan.FromSeconds(2),
                Lazy = lazy
            };

            using (var cursor = cache.Query(qry))
            {
                HashSet<int> exp0 = new HashSet<int>(exp);

                foreach (var entry in cursor.GetAll())
                {
                    Assert.AreEqual(2, entry.Count);
                    Assert.AreEqual(entry[0].ToString(), entry[1].ToString());

                    exp0.Remove((int)entry[1]);
                }

                Assert.AreEqual(0, exp0.Count);
                Assert.AreEqual(new[] {"NAME", "AGE"}, cursor.FieldNames);
            }

            // Test old API as well.
#pragma warning disable 618
            using (var cursor = cache.QueryFields(qry))
            {
                HashSet<int> exp0 = new HashSet<int>(exp);

                foreach (var entry in cursor)
                {
                    Assert.AreEqual(entry[0].ToString(), entry[1].ToString());

                    exp0.Remove((int)entry[1]);
                }

                Assert.AreEqual(0, exp0.Count);
            }
#pragma warning restore 618
        }

        /// <summary>
        /// Tests that query configuration propagates from Spring XML correctly.
        /// </summary>
        [Test]
        public void TestQueryConfiguration()
        {
            var qe = Cache().GetConfiguration().QueryEntities.Single();

            Assert.AreEqual(typeof(QueryPerson).FullName, qe.ValueTypeName);

            var age = qe.Fields.First();
            Assert.AreEqual("age", age.Name);
            Assert.AreEqual(typeof(int), age.FieldType);
            Assert.IsFalse(age.IsKeyField);

            var name = qe.Fields.Last();
            Assert.AreEqual("name", name.Name);
            Assert.AreEqual(typeof(string), name.FieldType);
            Assert.IsFalse(name.IsKeyField);

            var textIdx = qe.Indexes.First();
            Assert.AreEqual(QueryIndexType.FullText, textIdx.IndexType);
            Assert.AreEqual("name", textIdx.Fields.Single().Name);
            Assert.AreEqual(QueryIndex.DefaultInlineSize, textIdx.InlineSize);

            var sqlIdx = qe.Indexes.Last();
            Assert.AreEqual(QueryIndexType.Sorted, sqlIdx.IndexType);
            Assert.AreEqual("age", sqlIdx.Fields.Single().Name);
            Assert.AreEqual(2345, sqlIdx.InlineSize);
        }

        /// <summary>
        /// Check text query.
        /// </summary>
        [Test]
        public void TestTextQuery([Values(true, false)] bool loc, [Values(true, false)] bool keepBinary)
        {
            var cache = Cache();

            // 1. Populate cache with data, calculating expected count in parallel.
            var exp = PopulateCache(cache, loc, MaxItemCnt, x => x.ToString().StartsWith("1"));

            // 2. Validate results.
            var qry = new TextQuery(typeof(QueryPerson), "1*", loc);

            ValidateQueryResults(cache, qry, exp, keepBinary);
        }

        /// <summary>
        /// Check scan query.
        /// </summary>
        [Test]
        public void TestScanQuery([Values(true, false)]  bool loc)
        {
            CheckScanQuery<QueryPerson>(loc, false);
        }

        /// <summary>
        /// Check scan query in binary mode.
        /// </summary>
        [Test]
        public void TestScanQueryBinary([Values(true, false)]  bool loc)
        {
            CheckScanQuery<IBinaryObject>(loc, true);
        }

        /// <summary>
        /// Check scan query with partitions.
        /// </summary>
        [Test]
        public void TestScanQueryPartitions([Values(true, false)]  bool loc)
        {
            CheckScanQueryPartitions<QueryPerson>(loc, false);
        }

        /// <summary>
        /// Check scan query with partitions in binary mode.
        /// </summary>
        [Test]
        public void TestScanQueryPartitionsBinary([Values(true, false)]  bool loc)
        {
            CheckScanQueryPartitions<IBinaryObject>(loc, true);
        }

        /// <summary>
        /// Checks that scan query is thread-safe and throws correct exception when disposed from another thread.
        /// </summary>
        [Test]
        public void TestScanQueryDisposedFromAnotherThreadThrowsObjectDisposedException()
        {
            var cache = GetIgnite().GetOrCreateCache<int, int>(TestUtils.TestName);

            const int totalCount = 10000;
            cache.PutAll(Enumerable.Range(1, totalCount).ToDictionary(x => x, x => x));

            var scanQuery = new ScanQuery<int, int>
            {
                Filter = new ScanQueryFilter<int> {AcceptAll = true}
            };

            var cursor = cache.Query(scanQuery);

            long count = 0;
            Task.Factory.StartNew(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                while (Interlocked.Read(ref count) < totalCount / 10) { }
                cursor.Dispose();
            });

            Assert.Throws<ObjectDisposedException>(() =>
            {
                foreach (var unused in cursor)
                {
                    Interlocked.Increment(ref count);
                }
            });
        }

        /// <summary>
        /// Tests that query attempt on non-indexed cache causes an exception.
        /// </summary>
        [Test]
        public void TestIndexingDisabledError()
        {
            var cache = GetIgnite().GetOrCreateCache<int, QueryPerson>("nonindexed_cache");

            // Text query.
            var err = Assert.Throws<IgniteException>(() => cache.Query(new TextQuery(typeof(QueryPerson), "1*")));

            Assert.AreEqual("Indexing is disabled for cache: nonindexed_cache. " +
                "Use setIndexedTypes or setTypeMetadata methods on CacheConfiguration to enable.", err.Message);

            // SQL query.
#pragma warning disable 618
            err = Assert.Throws<IgniteException>(() => cache.Query(new SqlQuery(typeof(QueryPerson), "age < 50")));
#pragma warning restore 618

            Assert.AreEqual("Failed to find SQL table for type: QueryPerson", err.Message);
        }

        /// <summary>
        /// Check scan query.
        /// </summary>
        /// <param name="loc">Local query flag.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        private static void CheckScanQuery<TV>(bool loc, bool keepBinary)
        {
            var cache = Cache();
            int cnt = MaxItemCnt;

            // No predicate
            var exp = PopulateCache(cache, loc, cnt, x => true);
            var qry = new ScanQuery<int, TV>();
            ValidateQueryResults(cache, qry, exp, keepBinary);

            // Serializable
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, TV>(new ScanQueryFilter<TV>());
            ValidateQueryResults(cache, qry, exp, keepBinary);

            // Binarizable
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, TV>(new BinarizableScanQueryFilter<TV>());
            ValidateQueryResults(cache, qry, exp, keepBinary);

            // Invalid
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, TV>(new InvalidScanQueryFilter<TV>());
            Assert.Throws<BinaryObjectException>(() => ValidateQueryResults(cache, qry, exp, keepBinary));

            // Exception
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, TV>(new ScanQueryFilter<TV> {ThrowErr = true});
            
            var ex = Assert.Throws<IgniteException>(() => ValidateQueryResults(cache, qry, exp, keepBinary));
            Assert.AreEqual(ScanQueryFilter<TV>.ErrMessage, ex.Message);
        }

        /// <summary>
        /// Checks scan query with partitions.
        /// </summary>
        /// <param name="loc">Local query flag.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        private void CheckScanQueryPartitions<TV>(bool loc, bool keepBinary)
        {
            StopGrids();
            StartGrids();

            var cache = Cache();
            int cnt = MaxItemCnt;

            var aff = cache.Ignite.GetAffinity(CacheName);
            var exp = PopulateCache(cache, loc, cnt, x => true);  // populate outside the loop (slow)

            for (var part = 0; part < aff.Partitions; part++)
            {
                //var exp0 = new HashSet<int>(exp.Where(x => aff.Partition(x) == part)); // filter expected keys
                var exp0 = new HashSet<int>();
                foreach (var x in exp)
                    if (aff.GetPartition(x) == part)
                        exp0.Add(x);

                var qry = new ScanQuery<int, TV> { Partition = part };

                ValidateQueryResults(cache, qry, exp0, keepBinary);
            }

            // Partitions with predicate
            exp = PopulateCache(cache, loc, cnt, x => x < 50);  // populate outside the loop (slow)

            for (var part = 0; part < aff.Partitions; part++)
            {
                //var exp0 = new HashSet<int>(exp.Where(x => aff.Partition(x) == part)); // filter expected keys
                var exp0 = new HashSet<int>();
                foreach (var x in exp)
                    if (aff.GetPartition(x) == part)
                        exp0.Add(x);

                var qry = new ScanQuery<int, TV>(new ScanQueryFilter<TV>()) { Partition = part };

                ValidateQueryResults(cache, qry, exp0, keepBinary);
            }
            
        }

        /// <summary>
        /// Tests custom schema name.
        /// </summary>
        [Test]
        public void TestCustomSchema()
        {
            var doubles = GetIgnite().GetOrCreateCache<int, double>(new CacheConfiguration("doubles",
                    new QueryEntity(typeof(int), typeof(double))));
            var strings = GetIgnite().GetOrCreateCache<int, string>(new CacheConfiguration("strings",
                    new QueryEntity(typeof(int), typeof(string))));

            doubles[1] = 36.6;
            strings[1] = "foo";

            // Default schema.
            var res = doubles.Query(new SqlFieldsQuery(
                    "select S._val from double as D join \"strings\".string as S on S._key = D._key"))
                .Select(x => (string) x[0])
                .Single();

            Assert.AreEqual("foo", res);

            // Custom schema.
            res = doubles.Query(new SqlFieldsQuery(
                    "select S._val from \"doubles\".double as D join string as S on S._key = D._key")
                {
                    Schema = strings.Name
                })
                .Select(x => (string)x[0])
                .Single();

            Assert.AreEqual("foo", res);
        }

        /// <summary>
        /// Tests the distributed joins flag.
        /// </summary>
        [Test]
        public void TestDistributedJoins()
        {
            var cache = GetIgnite().GetOrCreateCache<int, QueryPerson>(
                new CacheConfiguration("replicatedCache")
                {
                    QueryEntities = new[]
                    {
                        new QueryEntity(typeof(int), typeof(QueryPerson))
                        {
                            Fields = new[] {new QueryField("age", "int")}
                        }
                    }
                });

            const int count = 100;

            cache.PutAll(Enumerable.Range(0, count).ToDictionary(x => x, x => new QueryPerson("Name" + x, x)));

            // Test non-distributed join: returns partial results
            var sql = "select T0.Age from QueryPerson as T0 " +
                      "inner join QueryPerson as T1 on ((? - T1.Age - 1) = T0._key)";

            var res = cache.Query(new SqlFieldsQuery(sql, count)).GetAll().Distinct().Count();

            Assert.Greater(res, 0);
            Assert.Less(res, count);

            // Test distributed join: returns complete results
            res = cache.Query(new SqlFieldsQuery(sql, count) {EnableDistributedJoins = true})
                .GetAll().Distinct().Count();

            Assert.AreEqual(count, res);
        }

        /// <summary>
        /// Tests the get configuration.
        /// </summary>
        [Test]
        public void TestGetConfiguration()
        {
            var entity = Cache().GetConfiguration().QueryEntities.Single();

            var ageField = entity.Fields.Single(x => x.Name == "age");
            Assert.AreEqual(typeof(int), ageField.FieldType);
            Assert.IsFalse(ageField.NotNull);
            Assert.IsFalse(ageField.IsKeyField);

            var nameField = entity.Fields.Single(x => x.Name == "name");
            Assert.AreEqual(typeof(string), nameField.FieldType);
            Assert.IsTrue(nameField.NotNull);
            Assert.IsFalse(nameField.IsKeyField);
        }

        /// <summary>
        /// Tests custom key and value field names.
        /// </summary>
        [Test]
        public void TestCustomKeyValueFieldNames()
        {
            // Check select * with default config - does not include _key, _val.
            var cache = Cache();

            cache[1] = new QueryPerson("Joe", 48);

            var row = cache.Query(new SqlFieldsQuery("select * from QueryPerson")).GetAll()[0];
            Assert.AreEqual(2, row.Count);
            Assert.AreEqual(48, row[0]);
            Assert.AreEqual("Joe", row[1]);

            // Check select * with custom names - fields are included.
            cache = GetIgnite().GetOrCreateCache<int, QueryPerson>(
                new CacheConfiguration("customKeyVal")
                {
                    QueryEntities = new[]
                    {
                        new QueryEntity(typeof(int), typeof(QueryPerson))
                        {
                            Fields = new[]
                            {
                                new QueryField("age", "int"),
                                new QueryField("FullKey", "int"),
                                new QueryField("FullVal", "QueryPerson")
                            },
                            KeyFieldName = "FullKey",
                            ValueFieldName = "FullVal"
                        }
                    }
                });

            cache[1] = new QueryPerson("John", 33);

            row = cache.Query(new SqlFieldsQuery("select * from QueryPerson")).GetAll()[0];
            
            Assert.AreEqual(3, row.Count);
            Assert.AreEqual(33, row[0]);
            Assert.AreEqual(1, row[1]);

            var person = (QueryPerson) row[2];
            Assert.AreEqual("John", person.Name);

            // Check explicit select.
            row = cache.Query(new SqlFieldsQuery("select FullKey from QueryPerson")).GetAll()[0];
            Assert.AreEqual(1, row[0]);
        }

        /// <summary>
        /// Tests query timeouts.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestSqlQueryTimeout()
        {
            var cache = Cache();
            PopulateCache(cache, false, 30000, x => true);

#pragma warning disable 618
            var sqlQry = new SqlQuery(typeof(QueryPerson), "WHERE age < 2000")
            {
                Timeout = TimeSpan.FromMilliseconds(1)
            };
#pragma warning restore 618

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<CacheException>(() => cache.Query(sqlQry).ToArray());
            Assert.IsTrue(ex.ToString().Contains("QueryCancelledException: The query was cancelled while executing."));
        }

        /// <summary>
        /// Tests fields query timeouts.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestSqlFieldsQueryTimeout()
        {
            var cache = Cache();
            PopulateCache(cache, false, 20000, x => true);

            var fieldsQry = new SqlFieldsQuery("SELECT * FROM QueryPerson WHERE age < 5000 AND name like '%0%'")
            {
                Timeout = TimeSpan.FromMilliseconds(3)
            };

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<CacheException>(() => cache.Query(fieldsQry).ToArray());
            Assert.IsTrue(ex.ToString().Contains("QueryCancelledException: The query was cancelled while executing."));
        }

        /// <summary>
        /// Tests the FieldNames property.
        /// </summary>
        [Test]
        public void TestFieldNames()
        {
            var cache = Cache();
            PopulateCache(cache, false, 5, x => true);

            // Get before iteration.
            var qry = new SqlFieldsQuery("SELECT * FROM QueryPerson");
            var cur = cache.Query(qry);
            var names = cur.FieldNames;

            Assert.AreEqual(new[] {"AGE", "NAME" }, names);
            
            cur.Dispose();
            Assert.AreSame(names, cur.FieldNames);

            Assert.Throws<NotSupportedException>(() => cur.FieldNames.Add("x"));

            // Custom order, key-val, get after iteration.
            qry.Sql = "SELECT NAME, _key, AGE, _val FROM QueryPerson";
            cur = cache.Query(qry);
            cur.GetAll();

            Assert.AreEqual(new[] { "NAME", "_KEY", "AGE", "_VAL" }, cur.FieldNames);

            // Get after disposal.
            qry.Sql = "SELECT 1, AGE FROM QueryPerson";
            cur = cache.Query(qry);
            cur.Dispose();
            
            Assert.AreEqual(new[] { "1", "AGE" }, cur.FieldNames);
        }

        /// <summary>
        /// Tests the FieldsMetadata property.
        /// </summary>
        [Test]
        public void TestFieldsMetadata()
        {
            var cache = Cache();
            PopulateCache(cache, false, 5, x => true);

            // Get before iteration.
            var qry = new SqlFieldsQuery("SELECT * FROM QueryPerson");
            var cur = cache.Query(qry);
            var metas = cur.Fields;

            ValidateFieldsMetadata(
                metas,
                new[] {"AGE", "NAME"},
                new[] {typeof(int), typeof(string)},
                new[] {"java.lang.Integer", "java.lang.String"}
            );

            cur.Dispose();
            Assert.AreSame(metas, cur.Fields);

            Assert.Throws<NotSupportedException>(() => cur.Fields.Add(default(IQueryCursorField)));

            // Custom order, key-val, get after iteration.
            qry.Sql = "SELECT NAME, _key, AGE, _val FROM QueryPerson";
            cur = cache.Query(qry);
            cur.GetAll();

            ValidateFieldsMetadata(
                cur.Fields,
                new[] {"NAME", "_KEY", "AGE", "_VAL"},
                new[] {typeof(string), typeof(object), typeof(int), typeof(object)},
                new[] {"java.lang.String", "java.lang.Object", "java.lang.Integer", "java.lang.Object"}
            );

            // Get after disposal.
            qry.Sql = "SELECT 1, AGE FROM QueryPerson";
            cur = cache.Query(qry);
            cur.Dispose();

            ValidateFieldsMetadata(
                cur.Fields,
                new[] {"1", "AGE"},
                new[] {typeof(int), typeof(int)},
                new[] {"java.lang.Integer", "java.lang.Integer"}
            );
        }

        /// <summary>
        /// Validates fields metadata collection
        /// </summary>
        /// <param name="metadata">Metadata</param>
        /// <param name="expectedNames">Expected field names</param>
        /// <param name="expectedTypes">Expected field types</param>
        /// <param name="expectedJavaTypeNames">Expected java type names</param>
        private static void ValidateFieldsMetadata(
            IList<IQueryCursorField> metadata,
            string[] expectedNames,
            Type[] expectedTypes,
            string[] expectedJavaTypeNames
        )
        {
            Assert.AreEqual(expectedNames, metadata.Select(m => m.Name));
            Assert.AreEqual(expectedTypes, metadata.Select(m => m.Type));
            Assert.AreEqual(expectedJavaTypeNames, metadata.Select(m => m.JavaTypeName));
        }

        /// <summary>
        /// Validates the query results.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="qry">Query.</param>
        /// <param name="exp">Expected keys.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        private static void ValidateQueryResults(ICache<int, QueryPerson> cache, QueryBase qry, HashSet<int> exp,
            bool keepBinary)
        {
            if (keepBinary)
            {
                var cache0 = cache.WithKeepBinary<int, IBinaryObject>();

                using (var cursor = cache0.Query(qry))
                {
                    HashSet<int> exp0 = new HashSet<int>(exp);
                    var all = new List<ICacheEntry<int, object>>();

                    foreach (var entry in cursor.GetAll())
                    {
                        all.Add(entry);

                        Assert.AreEqual(entry.Key.ToString(), entry.Value.GetField<string>("name"));
                        Assert.AreEqual(entry.Key, entry.Value.GetField<int>("age"));

                        exp0.Remove(entry.Key);
                    }

                    AssertMissingExpectedKeys(exp0, cache, all);
                }

                using (var cursor = cache0.Query(qry))
                {
                    HashSet<int> exp0 = new HashSet<int>(exp);
                    var all = new List<ICacheEntry<int, object>>();

                    foreach (var entry in cursor)
                    {
                        all.Add(entry);

                        Assert.AreEqual(entry.Key.ToString(), entry.Value.GetField<string>("name"));
                        Assert.AreEqual(entry.Key, entry.Value.GetField<int>("age"));

                        exp0.Remove(entry.Key);
                    }

                    AssertMissingExpectedKeys(exp0, cache, all);
                }
            }
            else
            {
                using (var cursor = cache.Query(qry))
                {
                    HashSet<int> exp0 = new HashSet<int>(exp);
                    var all = new List<ICacheEntry<int, object>>();

                    foreach (var entry in cursor.GetAll())
                    {
                        all.Add(entry);

                        Assert.AreEqual(entry.Key.ToString(), entry.Value.Name);
                        Assert.AreEqual(entry.Key, entry.Value.Age);

                        exp0.Remove(entry.Key);
                    }

                    AssertMissingExpectedKeys(exp0, cache, all);
                }

                using (var cursor = cache.Query(qry))
                {
                    HashSet<int> exp0 = new HashSet<int>(exp);
                    var all = new List<ICacheEntry<int, object>>();

                    foreach (var entry in cursor)
                    {
                        all.Add(entry);

                        Assert.AreEqual(entry.Key.ToString(), entry.Value.Name);
                        Assert.AreEqual(entry.Key, entry.Value.Age);

                        exp0.Remove(entry.Key);
                    }

                    AssertMissingExpectedKeys(exp0, cache, all);
                }
            }
        }

        /// <summary>
        /// Asserts that all expected entries have been received.
        /// </summary>
        private static void AssertMissingExpectedKeys(ICollection<int> exp, ICache<int, QueryPerson> cache, 
            IList<ICacheEntry<int, object>> all)
        {
            if (exp.Count == 0)
                return;

            var sb = new StringBuilder();
            var aff = cache.Ignite.GetAffinity(cache.Name);

            foreach (var key in exp)
            {
                var part = aff.GetPartition(key);
                sb.AppendFormat(
                    "Query did not return expected key '{0}' (exists: {1}), partition '{2}', partition nodes: ", 
                    key, cache.Get(key) != null, part);

                var partNodes = aff.MapPartitionToPrimaryAndBackups(part);

                foreach (var node in partNodes)
                    sb.Append(node).Append("  ");

                sb.AppendLine(";");
            }

            sb.Append("Returned keys: ");

            foreach (var e in all)
                sb.Append(e.Key).Append(" ");

            sb.AppendLine(";");

            Assert.Fail(sb.ToString());
        }

        /// <summary>
        /// Populates the cache with random entries and returns expected results set according to filter.
        /// </summary>
        /// <param name="cache">The cache.</param>
        /// <param name="cnt">Amount of cache entries to create.</param>
        /// <param name="loc">Local query flag.</param>
        /// <param name="expectedEntryFilter">The expected entry filter.</param>
        /// <returns>Expected results set.</returns>
        private static HashSet<int> PopulateCache(ICache<int, QueryPerson> cache,  bool loc, int cnt,
            Func<int, bool> expectedEntryFilter)
        {
            var rand = new Random();

            for (var i = 0; i < cnt; i++)
            {
                var val = rand.Next(cnt);

                cache.Put(val, new QueryPerson(val.ToString(), val));
            }

            var entries = loc
                ? cache.GetLocalEntries(CachePeekMode.Primary)
                : cache;

            return new HashSet<int>(entries.Select(x => x.Key).Where(expectedEntryFilter));
        }
    }

    /// <summary>
    /// Person.
    /// </summary>
    public class QueryPerson
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="age">Age.</param>
        public QueryPerson(string name, int age)
        {
            Name = name;
            Age = age % 2000;
            Birthday = DateTime.UtcNow.AddYears(-Age);
        }

        /// <summary>
        /// Name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Age.
        /// </summary>
        public int Age { get; set; }

        /// <summary>
        /// Gets or sets the birthday.
        /// </summary>
        [QuerySqlField]  // Enforce Timestamp serialization
        public DateTime Birthday { get; set; }
    }

    /// <summary>
    /// Query filter.
    /// </summary>
    [Serializable]
    public class ScanQueryFilter<TV> : ICacheEntryFilter<int, TV>
    {
        // Error message
        public const string ErrMessage = "Error in ScanQueryFilter.Invoke";

        // Error flag
        public bool ThrowErr { get; set; }

        // Error flag
        public bool AcceptAll { get; set; }

        // Injection test
        [InstanceResource]
        public IIgnite Ignite { get; set; }

        /** <inheritdoc /> */
        public bool Invoke(ICacheEntry<int, TV> entry)
        {
            Assert.IsNotNull(Ignite);

            if (ThrowErr)
                throw new Exception(ErrMessage);

            return entry.Key < 50 || AcceptAll;
        }
    }

    /// <summary>
    /// binary query filter.
    /// </summary>
    public class BinarizableScanQueryFilter<TV> : ScanQueryFilter<TV>, IBinarizable
    {
        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var w = writer.GetRawWriter();

            w.WriteBoolean(ThrowErr);
        }

        /** <inheritdoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            var r = reader.GetRawReader();

            ThrowErr = r.ReadBoolean();
        }
    }

    /// <summary>
    /// Filter that can't be serialized.
    /// </summary>
    public class InvalidScanQueryFilter<TV> : ScanQueryFilter<TV>, IBinarizable
    {
        public void WriteBinary(IBinaryWriter writer)
        {
            throw new BinaryObjectException("Expected");
        }

        public void ReadBinary(IBinaryReader reader)
        {
            throw new BinaryObjectException("Expected");
        }
    }
}

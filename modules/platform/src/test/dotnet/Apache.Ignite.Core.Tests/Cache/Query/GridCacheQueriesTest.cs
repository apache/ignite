﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Queries tests.
    /// </summary>
    public class GridCacheQueriesTest
    {
        /** Grid count. */
        private const int GRID_CNT = 2;

        /** Cache name. */
        private const string CACHE_NAME = "cache";

        /** Path to XML configuration. */
        private const string CFG_PATH = "config\\cache-query.xml";

        /** Maximum amount of items in cache. */
        private const int MAX_ITEM_CNT = 100;

        /// <summary>
        /// 
        /// </summary>
        [TestFixtureSetUp]
        public virtual void StartGrids()
        {
            GridTestUtils.JVM_DEBUG = true;
            GridTestUtils.KillProcesses();

            GridConfigurationEx cfg = new GridConfigurationEx
            {
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new PortableTypeConfiguration(typeof (QueryPerson)),
                        new PortableTypeConfiguration(typeof (PortableScanQueryFilter<QueryPerson>)),
                        new PortableTypeConfiguration(typeof (PortableScanQueryFilter<PortableUserObject>))
                    }
                },
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                SpringConfigUrl = CFG_PATH
            };

            for (int i = 0; i < GRID_CNT; i++)
            {
                cfg.GridName = "grid-" + i;

                GridFactory.Start(cfg);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [TestFixtureTearDown]
        public virtual void StopGrids()
        {
            for (int i = 0; i < GRID_CNT; i++)
                GridFactory.Stop("grid-" + i, true);
        }

        /// <summary>
        /// 
        /// </summary>
        [SetUp]
        public virtual void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        /// 
        /// </summary>
        [TearDown]
        public virtual void AfterTest()
        {
            var cache = Cache();

            for (int i = 0; i < GRID_CNT; i++)
            {
                for (int j = 0; j < MAX_ITEM_CNT; j++)
                    cache.Remove(j);

                Assert.IsTrue(cache.IsEmpty);
            }

            Console.WriteLine("Test finished: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public IIgnite Grid(int idx)
        {
            return GridFactory.Grid("grid-" + idx);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public ICache<int, QueryPerson> Cache(int idx)
        {
            return Grid(idx).Cache<int, QueryPerson>(CACHE_NAME);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ICache<int, QueryPerson> Cache()
        {
            return Cache(0);
        }

        /// <summary>
        /// Test arguments validation for SQL queries.
        /// </summary>
        [Test]
        public void TestValidationSql()
        {
            // 1. No sql.
            Assert.Throws<ArgumentException>(() =>
                { Cache().Query(new SqlQuery(typeof(QueryPerson), null)); });

            // 2. No type.
            Assert.Throws<ArgumentException>(() =>
                { Cache().Query(new SqlQuery((string)null, "age >= 50")); });
        }

        /// <summary>
        /// Test arguments validation for SQL fields queries.
        /// </summary>
        [Test]
        public void TestValidationSqlFields()
        {
            // 1. No sql.
            Assert.Throws<ArgumentException>(() => { Cache().QueryFields(new SqlFieldsQuery(null)); });
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
            var cache0 = Cache().WithAsync();

            cache0.WithAsync().Put(1, new QueryPerson("Ivanov", 30));

            IFuture<object> res = cache0.GetFuture<object>();

            res.Get();

            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(1, new QueryPerson("Petrov", 40));
            Cache().Put(1, new QueryPerson("Sidorov", 50));

            SqlQuery qry = new SqlQuery(typeof(QueryPerson), "age >= 20");

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
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(2, new QueryPerson("Petrov", 40));
            Cache().Put(3, new QueryPerson("Sidorov", 50));
            Cache().Put(4, new QueryPerson("Unknown", 60));

            // 1. Empty result set.
            using (
                IQueryCursor<ICacheEntry<int, QueryPerson>> cursor =
                    Cache().Query(new SqlQuery(typeof(QueryPerson), "age = 100")))
            {
                IEnumerator<ICacheEntry<int, QueryPerson>> e = cursor.GetEnumerator();

                Assert.Throws<InvalidOperationException>(() =>
                    { ICacheEntry<int, QueryPerson> entry = e.Current; });

                Assert.IsFalse(e.MoveNext());

                Assert.Throws<InvalidOperationException>(() =>
                    { ICacheEntry<int, QueryPerson> entry = e.Current; });

                Assert.Throws<NotSupportedException>(() => e.Reset());
            }

            SqlQuery qry = new SqlQuery(typeof (QueryPerson), "age < 60");

            // 2. Page size is bigger than result set.
            qry.PageSize = 4;
            CheckEnumeratorQuery(qry);

            // 3. Page size equal to result set.
            qry.PageSize = 3;
            CheckEnumeratorQuery(qry);

            // 4. Page size if less than result set.
            qry.PageSize = 2;
            CheckEnumeratorQuery(qry);
        }

        /// <summary>
        /// Test SQL query arguments passing.
        /// </summary>
        public void TestSqlQueryArguments()
        {
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(2, new QueryPerson("Petrov", 40));
            Cache().Put(3, new QueryPerson("Sidorov", 50));

            // 1. Empty result set.
            using (
                IQueryCursor<ICacheEntry<int, QueryPerson>> cursor =
                    Cache().Query(new SqlQuery(typeof(QueryPerson), "age < ?", 50)))
            {
                foreach (ICacheEntry<int, QueryPerson> entry in cursor.GetAll())
                    Assert.IsTrue(entry.Key == 1 || entry.Key == 2);
            }
        }

        /// <summary>
        /// Test SQL fields query arguments passing.
        /// </summary>
        public void TestSqlFieldsQueryArguments()
        {
            Cache().Put(1, new QueryPerson("Ivanov", 30));
            Cache().Put(2, new QueryPerson("Petrov", 40));
            Cache().Put(3, new QueryPerson("Sidorov", 50));

            // 1. Empty result set.
            using (
                IQueryCursor<IList> cursor = Cache().QueryFields(
                    new SqlFieldsQuery("SELECT age FROM QueryPerson WHERE age < ?", 50)))
            {
                foreach (IList entry in cursor.GetAll())
                    Assert.IsTrue((int) entry[0] < 50);
            }
        }

        /// <summary>
        /// Check query result for enumerator test.
        /// </summary>
        /// <param name="qry">QUery.</param>
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

        /// <summary>
        /// Check SQL query.
        /// </summary>
        [Test]
        public void TestSqlQuery()
        {
            CheckSqlQuery(MAX_ITEM_CNT, false, false);
        }

        /// <summary>
        /// Check SQL query in portable mode.
        /// </summary>
        [Test]
        public void TestSqlQueryPortable()
        {
            CheckSqlQuery(MAX_ITEM_CNT, false, true);
        }

        /// <summary>
        /// Check local SQL query.
        /// </summary>
        [Test]
        public void TestSqlQueryLocal()
        {
            CheckSqlQuery(MAX_ITEM_CNT, true, false);
        }

        /// <summary>
        /// Check local SQL query in portable mode.
        /// </summary>
        [Test]
        public void TestSqlQueryLocalPortable()
        {
            CheckSqlQuery(MAX_ITEM_CNT, true, true);
        }

        /// <summary>
        /// Check SQL query.
        /// </summary>
        /// <param name="cnt">Amount of cache entries to create.</param>
        /// <param name="loc">Local query flag.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        private void CheckSqlQuery(int cnt, bool loc, bool keepPortable)
        {
            var cache = Cache();

            // 1. Populate cache with data, calculating expected count in parallel.
            var exp = PopulateCache(cache, loc, cnt, x => x < 50);

            // 2. Validate results.
            SqlQuery qry = loc ?  new SqlQuery(typeof(QueryPerson), "age < 50", true) :
                new SqlQuery(typeof(QueryPerson), "age < 50");

            ValidateQueryResults(cache, qry, exp, keepPortable);
        }

        /// <summary>
        /// Check SQL fields query.
        /// </summary>
        [Test]
        public void TestSqlFieldsQuery()
        {
            CheckSqlFieldsQuery(MAX_ITEM_CNT, false);
        }

        /// <summary>
        /// Check local SQL fields query.
        /// </summary>
        [Test]
        public void TestSqlFieldsQueryLocal()
        {
            CheckSqlFieldsQuery(MAX_ITEM_CNT, true);
        }

        /// <summary>
        /// Check SQL fields query.
        /// </summary>
        /// <param name="cnt">Amount of cache entries to create.</param>
        /// <param name="loc">Local query flag.</param>
        private void CheckSqlFieldsQuery(int cnt, bool loc)
        {
            var cache = Cache();

            // 1. Populate cache with data, calculating expected count in parallel.
            var exp = PopulateCache(cache, loc, cnt, x => x < 50);

            // 2. Vlaidate results.
            SqlFieldsQuery qry = loc ? new SqlFieldsQuery("SELECT name, age FROM QueryPerson WHERE age < 50", true) :
                new SqlFieldsQuery("SELECT name, age FROM QueryPerson WHERE age < 50");

            using (IQueryCursor<IList> cursor = cache.QueryFields(qry))
            {
                HashSet<int> exp0 = new HashSet<int>(exp);

                foreach (var entry in cursor.GetAll())
                {
                    Assert.AreEqual(2, entry.Count);
                    Assert.AreEqual(entry[0].ToString(), entry[1].ToString());

                    exp0.Remove((int)entry[1]);
                }

                Assert.AreEqual(0, exp0.Count);
            }

            using (IQueryCursor<IList> cursor = cache.QueryFields(qry))
            {
                HashSet<int> exp0 = new HashSet<int>(exp);

                foreach (var entry in cursor)
                {
                    Assert.AreEqual(entry[0].ToString(), entry[1].ToString());

                    exp0.Remove((int)entry[1]);
                }

                Assert.AreEqual(0, exp0.Count);
            }
        }

        /// <summary>
        /// Check text query.
        /// </summary>
        [Test]
        public void TestTextQuery()
        {
            CheckTextQuery(MAX_ITEM_CNT, false, false);
        }

        /// <summary>
        /// Check SQL query in portable mode.
        /// </summary>
        [Test]
        public void TestTextQueryPortable()
        {
            CheckTextQuery(MAX_ITEM_CNT, false, true);
        }

        /// <summary>
        /// Check local SQL query.
        /// </summary>
        [Test]
        public void TestTextQueryLocal()
        {
            CheckTextQuery(MAX_ITEM_CNT, true, false);
        }

        /// <summary>
        /// Check local SQL query in portable mode.
        /// </summary>
        [Test]
        public void TestTextQueryLocalPortable()
        {
            CheckTextQuery(MAX_ITEM_CNT, true, true);
        }

        /// <summary>
        /// Check text query.
        /// </summary>
        /// <param name="cnt">Amount of cache entries to create.</param>
        /// <param name="loc">Local query flag.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        private void CheckTextQuery(int cnt, bool loc, bool keepPortable)
        {
            var cache = Cache();

            // 1. Populate cache with data, calculating expected count in parallel.
            var exp = PopulateCache(cache, loc, cnt, x => x.ToString().StartsWith("1"));

            // 2. Validate results.
            TextQuery qry = loc ? new TextQuery(typeof(QueryPerson), "1*", true) :
                new TextQuery(typeof(QueryPerson), "1*");

            ValidateQueryResults(cache, qry, exp, keepPortable);
        }

        /// <summary>
        /// Check scan query.
        /// </summary>
        [Test]
        public void TestScanQuery()
        {
            CheckScanQuery<QueryPerson>(MAX_ITEM_CNT, false, false);
        }

        /// <summary>
        /// Check scan query in portable mode.
        /// </summary>
        [Test]
        public void TestScanQueryPortable()
        {
            CheckScanQuery<PortableUserObject>(MAX_ITEM_CNT, false, true);
        }

        /// <summary>
        /// Check local scan query.
        /// </summary>
        [Test]
        public void TestScanQueryLocal()
        {
            CheckScanQuery<QueryPerson>(MAX_ITEM_CNT, true, false);
        }

        /// <summary>
        /// Check local scan query in portable mode.
        /// </summary>
        [Test]
        public void TestScanQueryLocalPortable()
        {
            CheckScanQuery<PortableUserObject>(MAX_ITEM_CNT, true, true);
        }

        /// <summary>
        /// Check scan query with partitions.
        /// </summary>
        [Test]
        [Ignore("IGNITE-1012")]
        public void TestScanQueryPartitions([Values(true, false)]  bool loc)
        {
            CheckScanQueryPartitions<QueryPerson>(MAX_ITEM_CNT, loc, false);
        }

        /// <summary>
        /// Check scan query with partitions in portable mode.
        /// </summary>
        [Test]
        [Ignore("IGNITE-1012")]
        public void TestScanQueryPartitionsPortable([Values(true, false)]  bool loc)
        {
            CheckScanQueryPartitions<PortableUserObject>(MAX_ITEM_CNT, loc, true);
        }

        /// <summary>
        /// Tests that query attempt on non-indexed cache causes an exception.
        /// </summary>
        [Test]
        public void TestIndexingDisabledError()
        {
            var cache = Grid(0).GetOrCreateCache<int, QueryPerson>("nonindexed_cache");

            var queries = new QueryBase[]
            {
                new TextQuery(typeof (QueryPerson), "1*"),
                new SqlQuery(typeof (QueryPerson), "age < 50")
            };

            foreach (var qry in queries)
            {
                var err = Assert.Throws<IgniteException>(() => cache.Query(qry));

                Assert.AreEqual("Indexing is disabled for cache: nonindexed_cache. " +
                    "Use setIndexedTypes or setTypeMetadata methods on CacheConfiguration to enable.", err.Message);
            }
        }

        /// <summary>
        /// Check scan query.
        /// </summary>
        /// <param name="cnt">Amount of cache entries to create.</param>
        /// <param name="loc">Local query flag.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        private void CheckScanQuery<V>(int cnt, bool loc, bool keepPortable)
        {
            var cache = Cache();

            // No predicate
            var exp = PopulateCache(cache, loc, cnt, x => true);
            var qry = new ScanQuery<int, V>();
            ValidateQueryResults(cache, qry, exp, keepPortable);

            // Serializable
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, V>(new ScanQueryFilter<V>());
            ValidateQueryResults(cache, qry, exp, keepPortable);

            // Portable
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, V>(new PortableScanQueryFilter<V>());
            ValidateQueryResults(cache, qry, exp, keepPortable);

            // Exception
            exp = PopulateCache(cache, loc, cnt, x => x < 50);
            qry = new ScanQuery<int, V>(new ScanQueryFilter<V> {ThrowErr = true});
            
            var ex = Assert.Throws<IgniteException>(() => ValidateQueryResults(cache, qry, exp, keepPortable));
            Assert.AreEqual(ScanQueryFilter<V>.ERR_MESSAGE, ex.Message);
        }

        /// <summary>
        /// Checks scan query with partitions.
        /// </summary>
        /// <param name="cnt">Amount of cache entries to create.</param>
        /// <param name="loc">Local query flag.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        private void CheckScanQueryPartitions<V>(int cnt, bool loc, bool keepPortable)
        {
            StopGrids();
            StartGrids();

            var cache = Cache();

            var aff = cache.Grid.Affinity(CACHE_NAME);
            var exp = PopulateCache(cache, loc, cnt, x => true);  // populate outside the loop (slow)

            for (var part = 0; part < aff.Partitions; part++)
            {
                //var exp0 = new HashSet<int>(exp.Where(x => aff.Partition(x) == part)); // filter expected keys
                var exp0 = new HashSet<int>();
                foreach (var x in exp)
                    if (aff.Partition(x) == part)
                        exp0.Add(x);

                var qry = new ScanQuery<int, V> { Partition = part };

                Console.WriteLine("Checking query on partition " + part);
                ValidateQueryResults(cache, qry, exp0, keepPortable);
            }

            // Partitions with predicate
            exp = PopulateCache(cache, loc, cnt, x => x < 50);  // populate outside the loop (slow)

            for (var part = 0; part < aff.Partitions; part++)
            {
                //var exp0 = new HashSet<int>(exp.Where(x => aff.Partition(x) == part)); // filter expected keys
                var exp0 = new HashSet<int>();
                foreach (var x in exp)
                    if (aff.Partition(x) == part)
                        exp0.Add(x);

                var qry = new ScanQuery<int, V>(new ScanQueryFilter<V>()) { Partition = part };

                Console.WriteLine("Checking predicate query on partition " + part);
                ValidateQueryResults(cache, qry, exp0, keepPortable);
            }
            
        }

        /// <summary>
        /// Validates the query results.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="qry">Query.</param>
        /// <param name="exp">Expected keys.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        private static void ValidateQueryResults(ICache<int, QueryPerson> cache, QueryBase qry, HashSet<int> exp,
            bool keepPortable)
        {
            if (keepPortable)
            {
                var cache0 = cache.WithKeepPortable<int, IPortableObject>();

                using (var cursor = cache0.Query(qry))
                {
                    HashSet<int> exp0 = new HashSet<int>(exp);
                    var all = new List<ICacheEntry<int, object>>();

                    foreach (var entry in cursor.GetAll())
                    {
                        all.Add(entry);

                        Assert.AreEqual(entry.Key.ToString(), entry.Value.Field<string>("name"));
                        Assert.AreEqual(entry.Key, entry.Value.Field<int>("age"));

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

                        Assert.AreEqual(entry.Key.ToString(), entry.Value.Field<string>("name"));
                        Assert.AreEqual(entry.Key, entry.Value.Field<int>("age"));

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
            var aff = cache.Grid.Affinity(cache.Name);

            foreach (var key in exp)
            {
                var part = aff.Partition(key);
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

            var exp = new HashSet<int>();

            for (var i = 0; i < cnt; i++)
            {
                var val = rand.Next(100);

                cache.Put(val, new QueryPerson(val.ToString(), val));

                if (expectedEntryFilter(val) && (!loc || cache.Grid.Affinity(cache.Name)
                    .IsPrimary(cache.Grid.Cluster.LocalNode, val)))
                    exp.Add(val);
            }

            return exp;
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
        public QueryPerson()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="age">Age.</param>
        public QueryPerson(string name, int age)
        {
            Name = name;
            Age = age;
        }

        /// <summary>
        /// Name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Age.
        /// </summary>
        public int Age { get; set; }
    }

    /// <summary>
    /// Query filter.
    /// </summary>
    [Serializable]
    public class ScanQueryFilter<V> : ICacheEntryFilter<int, V>
    {
        // Error message
        public const string ERR_MESSAGE = "Error in ScanQueryFilter.Invoke";

        // Error flag
        public bool ThrowErr { get; set; }

        /** <inheritdoc /> */
        public bool Invoke(ICacheEntry<int, V> entry)
        {
            if (ThrowErr)
                throw new Exception(ERR_MESSAGE);

            return entry.Key < 50;
        }
    }

    /// <summary>
    /// Portable query filter.
    /// </summary>
    public class PortableScanQueryFilter<V> : ScanQueryFilter<V>, IPortableMarshalAware
    {
        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var w = writer.RawWriter();

            w.WriteBoolean(ThrowErr);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            var r = reader.RawReader();

            ThrowErr = r.ReadBoolean();
        }
    }
}

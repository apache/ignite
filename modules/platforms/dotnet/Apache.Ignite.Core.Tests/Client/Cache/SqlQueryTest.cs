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
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests SQL queries via thin client.
    /// </summary>
    public class SqlQueryTest : ClientTestBase
    {
        /// <summary>
        /// Cache item count.
        /// </summary>
        private const int Count = 10;

        /// <summary>
        /// Second cache name.
        /// </summary>
        private const string CacheName2 = CacheName + "2";

        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQueryTest"/> class.
        /// </summary>
        public SqlQueryTest() : base(2)
        {
            // No-op.
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        public override void TestSetUp()
        {
            InitCache(CacheName);
            InitCache(CacheName2);
        }

        /// <summary>
        /// Tests the SQL query.
        /// </summary>
        [Test]
        public void TestSqlQuery()
        {
            var cache = GetClientCache<Person>();

            // All items.
            var qry = new SqlQuery(typeof(Person), "where 1 = 1");
            Assert.AreEqual(Count, cache.Query(qry).Count());

            // All items local.
            qry.Local = true;
            Assert.Greater(Count, cache.Query(qry).Count());

            // Filter.
            qry = new SqlQuery(typeof(Person), "where Name like '%7'");
            Assert.AreEqual(7, cache.Query(qry).Single().Key);

            // Args.
            qry = new SqlQuery(typeof(Person), "where Id = ?", 3);
            Assert.AreEqual(3, cache.Query(qry).Single().Value.Id);

            // DateTime.
            qry = new SqlQuery(typeof(Person), "where DateTime > ?", DateTime.UtcNow.AddDays(Count - 1));
            Assert.AreEqual(Count, cache.Query(qry).Single().Key);

            // Invalid args.
            qry.Sql = null;
            Assert.Throws<ArgumentNullException>(() => cache.Query(qry));

            qry.Sql = "abc";
            qry.QueryType = null;
            Assert.Throws<ArgumentNullException>(() => cache.Query(qry));
        }

        /// <summary>
        /// Tests the SQL query with distributed joins.
        /// </summary>
        [Test]
        public void TestSqlQueryDistributedJoins()
        {
            var cache = GetClientCache<Person>();

            // Non-distributed join returns incomplete results.
            var qry = new SqlQuery(typeof(Person),
                string.Format("from \"{0}\".Person, \"{1}\".Person as p2 where Person.Id = 11 - p2.Id",
                    CacheName, CacheName2));
            
            Assert.Greater(Count, cache.Query(qry).Count());

            // Distributed join fixes the problem.
            qry.EnableDistributedJoins = true;
            Assert.AreEqual(Count, cache.Query(qry).Count());
        }

        /// <summary>
        /// Tests the fields query.
        /// </summary>
        [Test]
        public void TestFieldsQuery()
        {
            var cache = GetClientCache<Person>();

            // All items.
            var qry = new SqlFieldsQuery("select Id from Person");
            var cursor = cache.Query(qry);
            CollectionAssert.AreEquivalent(Enumerable.Range(1, Count), cursor.Select(x => (int) x[0]));
            Assert.AreEqual("ID", cursor.FieldNames.Single());

            // All items local.
            qry.Local = true;
            Assert.Greater(Count, cache.Query(qry).Count());

            // Filter.
            qry = new SqlFieldsQuery("select Name from Person where Id = ?", 1)
            {
                Lazy = true,
                PageSize = 5,
            };
            Assert.AreEqual("Person 1", cache.Query(qry).Single().Single());

            // DateTime.
            qry = new SqlFieldsQuery("select Id, DateTime from Person where DateTime > ?", DateTime.UtcNow.AddDays(9));
            Assert.AreEqual(cache[Count].DateTime, cache.Query(qry).Single().Last());

            // Invalid args.
            qry.Sql = null;
            Assert.Throws<ArgumentNullException>(() => cache.Query(qry));
        }

        /// <summary>
        /// Tests the SQL fields query with distributed joins.
        /// </summary>
        [Test]
        public void TestFieldsQueryDistributedJoins()
        {
            var cache = GetClientCache<Person>();

            // Non-distributed join returns incomplete results.
            var qry = new SqlFieldsQuery(string.Format(
                "select p2.Name from \"{0}\".Person, \"{1}\".Person as p2 where Person.Id = 11 - p2.Id", 
                CacheName, CacheName2));

            Assert.Greater(Count, cache.Query(qry).Count());

            // Distributed join fixes the problem.
            qry.EnableDistributedJoins = true;
            Assert.AreEqual(Count, cache.Query(qry).Count());
        }

        /// <summary>
        /// Tests the fields query timeout.
        /// </summary>
        [Test]
        public void TestFieldsQueryTimeout()
        {
            var cache = GetClientCache<Person>();

            cache.PutAll(Enumerable.Range(1, 30000).ToDictionary(x => x, x => new Person(x)));

            var qry = new SqlFieldsQuery("select * from Person where Name like '%ers%'")
            {
                Timeout = TimeSpan.FromMilliseconds(1)
            };

            Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
        }

        /// <summary>
        /// Tests the fields query on a missing cache.
        /// </summary>
        [Test]
        public void TestFieldsQueryMissingCache()
        {
            var cache = Client.GetCache<int, Person>("I do not exist");
            var qry = new SqlFieldsQuery("select name from person")
            {
                Schema = CacheName
            };

            // Schema is set => we still check for cache existence.
            var ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            Assert.AreEqual("Cache doesn't exist: I do not exist", ex.Message);

            // Schema not set => also exception.
            qry.Schema = null;
            ex = Assert.Throws<IgniteClientException>(() => cache.Query(qry).GetAll());
            Assert.AreEqual("Cache doesn't exist: I do not exist", ex.Message);
        }

        /// <summary>
        /// Tests fields query with custom schema.
        /// </summary>
        [Test]
        public void TestFieldsQueryCustomSchema()
        {
            var cache1 = Client.GetCache<int, Person>(CacheName);
            var cache2 = Client.GetCache<int, Person>(CacheName2);

            cache1.RemoveAll();

            var qry = new SqlFieldsQuery("select name from person");

            // Schema not set: cache name is used.
            Assert.AreEqual(0, cache1.Query(qry).Count());
            Assert.AreEqual(Count, cache2.Query(qry).Count());

            // Schema set to first cache: no results both cases.
            qry.Schema = cache1.Name;
            Assert.AreEqual(0, cache1.Query(qry).Count());
            Assert.AreEqual(0, cache2.Query(qry).Count());

            // Schema set to second cache: full results both cases.
            qry.Schema = cache2.Name;
            Assert.AreEqual(Count, cache1.Query(qry).Count());
            Assert.AreEqual(Count, cache2.Query(qry).Count());
        }

        /// <summary>
        /// Tests the DML.
        /// </summary>
        [Test]
        public void TestDml()
        {
            var cache = GetClientCache<Person>();

            var qry = new SqlFieldsQuery("insert into Person (_key, id, name) values (?, ?, ?)", -10, 1, "baz");
            var res = cache.Query(qry).GetAll();

            Assert.AreEqual(1, res[0][0]);
            Assert.AreEqual("baz", cache[-10].Name);
        }

        /// <summary>
        /// Initializes the cache.
        /// </summary>
        private static void InitCache(string cacheName)
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, Person>(
                new CacheConfiguration(cacheName, new QueryEntity(typeof(int), typeof(Person))));

            cache.RemoveAll();

            cache.PutAll(Enumerable.Range(1, Count).ToDictionary(x => x, x => new Person(x)));
        }
    }
}

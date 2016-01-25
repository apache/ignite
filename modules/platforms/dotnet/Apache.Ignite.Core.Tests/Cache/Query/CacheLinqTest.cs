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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ queries.
    /// </summary>
    public class CacheLinqTest : IgniteTestBase
    {
        /** Cache name. */
        private const string CacheName = "cache";

        /** */
        private const int DataSize = 100;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheLinqTest"/> class.
        /// </summary>
        public CacheLinqTest() : base("config\\cache-query.xml")
        {
            // No-op.

            // TODO: Start cache with in-code config, test attributes
        }

        public override void TestSetUp()
        {
            base.TestSetUp();

            var cache = GetCache();

            for (var i = 0; i < DataSize; i++)
                cache.Put(i, new QueryPerson("Person_" + i, i));
        }

        // TODO: DELME
        public object TestSql0(string sql, params object[] args)
        {
            return GetCache().Query(new SqlQuery(typeof (QueryPerson), sql, args)).ToArray();
        }

        [Test]
        public void TestSql()
        {
            var res1 = TestSql0("_key > ?", 10);
            var res2 = TestSql0("_val.Age > ?", 10);
            Assert.IsNotNull(res1);
            Assert.IsNotNull(res2);
        }

        [Test]
        public void TestEmptyQuery()
        {
            var cache = GetCache();

            var results = cache.ToQueryable().ToArray();

            Assert.AreEqual(DataSize, results.Length);
        }

        [Test]
        public void TestWhere()
        {
            var cache = GetCache();

            Assert.AreEqual(10, cache.ToQueryable()
                .Where(x => x.Value.Age < 10).ToArray().Length);

            Assert.AreEqual(19, cache.ToQueryable()
                .Where(x => x.Value.Age > 10 && x.Value.Age < 30).ToArray().Length);

            Assert.AreEqual(20, cache.ToQueryable()
                .Where(x => x.Value.Age > 10).Where(x => x.Value.Age < 30 || x.Value.Age == 50).ToArray().Length);
        }

        [Test]
        [Ignore("Contains does not work for some reason")]
        public void TestStrings()
        {
            var cache = GetCache();

            var result = cache.ToQueryable().Where(x => x.Value.Name.Contains("Person")).ToArray();

            Assert.AreEqual(DataSize, result.Length);
        }

        private ICache<int, QueryPerson> GetCache()
        {
            return Grid.GetCache<int, QueryPerson>(CacheName);
        }
    }
}

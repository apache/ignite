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
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ queries.
    /// </summary>
    public class CacheLinqTest : IgniteTestBase
    {
        /** Cache name. */
        private const string CacheName = "cache";

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheLinqTest"/> class.
        /// </summary>
        public CacheLinqTest() : base("config\\cache-query.xml")
        {
            // No-op.
        }

        public override void TestSetUp()
        {
            base.TestSetUp();

            var cache = GetCache();

            for (var i = 0; i < 100; i++)
                cache.Put(i, new QueryPerson("Person_" + i, i));
        }

        [Test]
        public void Test()
        {
            var cache = GetCache();

            var query = cache.ToQueryable()
                .Where(x => x.Value.Age < 30 && x.Value.Age > 20)
                .Where(x => x.Value.Name.Contains("Person"));

            var result = query.ToArray();

            Assert.AreEqual(10, result.Length);
        }

        private ICache<int, QueryPerson> GetCache()
        {
            return Grid.GetCache<int, QueryPerson>(CacheName);
        }
    }
}

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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.Cache
{
    extern alias ExamplesDll;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests query metrics propagation.
    /// </summary>
    public class CacheQueryMetricsTest
    {
        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests query metrics.
        /// </summary>
        [Test]
        public void TestQueryMetrics()
        {
            var cache = GetCache();

            cache.ResetQueryMetrics();

            ExecuteTwoQueries(cache);

            CheckMetrics(cache.GetQueryMetrics());
        }

        /// <summary>
        /// Tests failed queries count.
        /// </summary>
        [Test]
        public void TestQueryFails()
        {
            var cache = GetCache();

            Assert.Throws<IgniteException>(() => cache.Query(new SqlFieldsQuery("select * from NOT_A_TABLE")));

            Assert.AreEqual(1, cache.GetQueryMetrics().Fails, "Check Fails count.");
        }

        /// <summary>
        /// Tests query metrics reset.
        /// </summary>
        [Test]
        public void TestQueryMetricsReset()
        {
            var cache = GetCache();

            ExecuteTwoQueries(cache);

            cache.ResetQueryMetrics();

            IQueryMetrics metrics = cache.GetQueryMetrics();

            Assert.AreEqual(0, metrics.Executions, "Check Executions count.");
            Assert.AreEqual(0, metrics.Fails, "Check Fails count.");
            Assert.AreEqual(0, metrics.MinimumTime, "Check MinimumTime.");
            Assert.AreEqual(0, metrics.MaximumTime, "Check MaximumTime.");
            Assert.AreEqual(0, metrics.AverageTime, 0.1, "Check AverageTime.");

            ExecuteTwoQueries(cache);

            CheckMetrics(cache.GetQueryMetrics());
        }

        /// <summary>
        /// Get Cache instance.
        /// </summary>
        /// <returns>Cache instance.</returns>
        private ICache<int, Person> GetCache()
        {
            var ignite = Ignition.GetIgnite();

            var cache = ignite.GetOrCreateCache<int, Person>(GetCacheConfiguration());

            if (cache.GetSize() == 0)
            {
                Person person = new Person()
                {
                    Name = "Adam",
                    Age = 35000
                };

                cache.Put(1, person);
            }

            return cache;
        }

        /// <summary>
        /// Check cache configuration.
        /// </summary>
        /// <returns>Cache configuration.</returns>
        private CacheConfiguration GetCacheConfiguration()
        {
            return new CacheConfiguration("cacheName")
            {
                EnableStatistics = true,

                QueryEntities = new List<QueryEntity>()
                {
                    new QueryEntity(typeof(int), typeof(Person))
                }
            };
        }

        /// <summary>
        /// Person.
        /// </summary>
        private class Person
        {
            [QuerySqlField]
            public string Name { get; set; }

            [QuerySqlField]
            public int Age { get; set; }
        }

        /// <summary>
        /// Execute two queries.
        /// </summary>
        /// <param name="cache">Cache instance.</param>
        [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed")]
        private static void ExecuteTwoQueries(ICache<int, Person> cache)
        {
            IQueryable<ICacheEntry<int, Person>> queryable = cache.AsCacheQueryable();

            queryable.Count(p => p.Value.Age > 0);

            queryable.Count(p => p.Value.Age > 1000);
        }

        /// <summary>
        /// Check metrics after two queries are executed.
        /// </summary>
        /// <param name="metrics">Query metrics.</param>
        private static void CheckMetrics(IQueryMetrics metrics)
        {
            Assert.AreEqual(2, metrics.Executions, "Check Executions count.");
            Assert.AreEqual(0, metrics.Fails, "Check Fails count.");

            Assert.GreaterOrEqual(metrics.MinimumTime, 0, "Check MinimumTime.");
            Assert.GreaterOrEqual(metrics.MaximumTime, metrics.MinimumTime, "Check MaximumTime.");

            Assert.AreEqual((double)(metrics.MinimumTime + metrics.MaximumTime) / 2, metrics.AverageTime, 1, "Check AverageTime.");
        }
    }
}

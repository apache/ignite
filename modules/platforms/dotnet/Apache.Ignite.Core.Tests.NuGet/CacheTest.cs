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
namespace Apache.Ignite.Core.Tests.NuGet
{
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Cache test.
    /// </summary>
    public class CacheTest
    {
        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                BinaryConfiguration = new BinaryConfiguration(typeof(Person))
            };

            Ignition.Start(cfg);
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
        /// Tests cache put/get.
        /// </summary>
        [Test]
        public void TestPutGet()
        {
            var ignite = Ignition.GetIgnite();

            var cache = ignite.CreateCache<int, int>("cache");

            cache[1] = 5;

            Assert.AreEqual(5, cache[1]);
        }

        /// <summary>
        /// Tests the SQL.
        /// </summary>
        [Test]
        public void TestSql()
        {
            var cache = GetPersonCache();

            var sqlRes = cache.Query(new SqlQuery(typeof (Person), "age < ?", 30)).GetAll();

            Assert.AreEqual(29, sqlRes.Count);
            Assert.IsTrue(sqlRes.All(x => x.Value.Age < 30));
        }

        /// <summary>
        /// Tests the LINQ.
        /// </summary>
        [Test]
        public void TestLinq()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            var res = cache.Where(x => x.Value.Age < 30).ToList();

            Assert.AreEqual(29, res.Count);
            Assert.IsTrue(res.All(x => x.Value.Age < 30));
        }

        /// <summary>
        /// Gets the person cache.
        /// </summary>
        /// <returns></returns>
        private static ICache<int, Person> GetPersonCache()
        {
            var ignite = Ignition.GetIgnite();

            var cache = ignite.GetOrCreateCache<int, Person>(new CacheConfiguration("sqlCache", typeof(Person)));

            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => new Person { Name = "Name" + x, Age = x }));

            return cache;
        }

        /// <summary>
        /// Query class.
        /// </summary>
        private class Person
        {
            /// <summary>
            /// Gets or sets the name.
            /// </summary>
            [QuerySqlField]
            public string Name { get; set; }

            /// <summary>
            /// Gets or sets the age.
            /// </summary>
            [QuerySqlField]
            public int Age { get; set; }
        }
    }
}

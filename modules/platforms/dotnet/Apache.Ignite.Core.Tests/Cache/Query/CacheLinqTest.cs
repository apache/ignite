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
    using System;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ queries.
    /// </summary>
    public class CacheLinqTest
    {
        /** Cache name. */
        private const string CacheName = "cache";

        /** */
        private const int DataSize = 100;

        /** */
        private bool _runDbConsole;

        /** */
        private int _testField;

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _runDbConsole = false;  // set to true to open H2 console

            if (_runDbConsole)
                Environment.SetEnvironmentVariable("IGNITE_H2_DEBUG_CONSOLE", "true");

            Ignition.Start(new IgniteConfiguration
            {
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                BinaryConfiguration =
                    new BinaryConfiguration(typeof (LinqPerson), typeof (LinqOrganization), typeof (LinqAddress))
            });

            var cache = GetCache();

            for (var i = 0; i < DataSize; i++)
                cache.Put(i, new LinqPerson(i, "Person_" + i)
                {
                    Address = new LinqAddress {Zip = i, Street = "Street " + i},
                    OrganizationId = i % 2 + 1000
                });

            var orgCache = GetOrgCache();

            orgCache[1000] = new LinqOrganization {Id = 1000, Name = "Org_0"};
            orgCache[1001] = new LinqOrganization {Id = 1001, Name = "Org_1"};

            Assert.AreEqual(1, cache[1].Age);
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            if (_runDbConsole)
                Thread.Sleep(Timeout.Infinite);
            Ignition.StopAll(true);
        }

        [Test]
        public void TestEmptyQuery()
        {
            var cache = GetCache();

            var results = cache.ToQueryable().ToArray();

            // There are both persons and organizations in the same cache, but query should only return specific type
            Assert.AreEqual(DataSize, results.Length);
        }

        [Test]
        public void TestWhere()
        {
            var cache = GetCache();

            var queryable = cache.ToQueryable().Where(x => x.Value.Age < 10);
            Assert.AreEqual(10, queryable.ToArray().Length);

            Assert.AreEqual(10, cache.ToQueryable()
                .Where(x => x.Value.Address.Zip < 10).ToArray().Length);

            Assert.AreEqual(19, cache.ToQueryable()
                .Where(x => x.Value.Age > 10 && x.Value.Age < 30).ToArray().Length);

            Assert.AreEqual(20, cache
                .ToQueryable().Where(x => x.Value.Age > 10).Count(x => x.Value.Age < 30 || x.Value.Age == 50));
        }

        [Test]
        public void TestKeyQuery()
        {
            var cache = GetCache();

            Assert.AreEqual(15, cache.ToQueryable().Where(x => x.Key < 15).ToArray().Length);
            Assert.AreEqual(15, cache.ToQueryable().Where(x => -x.Key > -15).ToArray().Length);
        }

        [Test]
        public void TestSingleFieldQuery()
        {
            var cache = GetCache();

            // Multiple values
            Assert.AreEqual(new[] {0, 1, 2},
                cache.ToQueryable().Where(x => x.Key < 3).Select(x => x.Value.Address.Zip).ToArray());

            // Single value
            Assert.AreEqual(0, cache.ToQueryable().Where(x => x.Key < 0).Select(x => x.Value.Age).FirstOrDefault());
            Assert.AreEqual(3, cache.ToQueryable().Where(x => x.Key == 3).Select(x => x.Value.Age).FirstOrDefault());
            Assert.AreEqual(3, cache.ToQueryable().Where(x => x.Key == 3).Select(x => x.Value).Single().Age);
            Assert.AreEqual(3, cache.ToQueryable().Where(x => x.Key == 3).Select(x => x.Key).Single());
        }

        [Test]
        public void TestMultiFieldQuery()
        {
            var cache = GetCache();

            // Test anonymous type (ctor invoke)
            var data = cache.ToQueryable()
                .Where(x => x.Key < 5)
                .Select(x => new {x.Key, x.Value.Age, x.Value.Address})
                .ToArray();

            Assert.AreEqual(5, data.Length);

            foreach (var t in data)
            {
                Assert.AreEqual(t.Age, t.Key);
                Assert.AreEqual(t.Age, t.Address.Zip);
            }

            // Test static method call
            var person = cache.ToQueryable().Where(x => x.Key == 13)
                .Select(x => CreatePersonStatic(x.Value.Age, x.Value.Name)).ToArray().Single();

            Assert.AreEqual(13, person.Age);
            
            // Test instance method call
            _testField = DateTime.Now.Second;

            var person2 = cache.ToQueryable().Where(x => x.Key == 14)
                .Select(x => CreatePersonInstance(x.Value.Name)).ToArray().Single();

            Assert.AreEqual(_testField, person2.Age);

            // Test lambda/delegate
            Func<int, LinqPerson> func = x => new LinqPerson(x, _testField.ToString());

            var person3 = cache.ToQueryable().Where(x => x.Key == 15)
                .Select(x => func(x.Key)).ToArray().Single();

            Assert.AreEqual(15, person3.Age);
            Assert.AreEqual(_testField.ToString(), person3.Name);
        }

        private static LinqPerson CreatePersonStatic(int age, string name)
        {
            return new LinqPerson(age, name);
        }

        private LinqPerson CreatePersonInstance(string name)
        {
            return new LinqPerson(_testField, name);
        }

        [Test]
        public void TestScalarQuery()
        {
            var cache = GetCache();

            Assert.AreEqual(DataSize - 1, cache.ToQueryable().Max(x => x.Value.Age));
            Assert.AreEqual(0, cache.ToQueryable().Min(x => x.Value.Age));

            Assert.AreEqual(21,
                cache.ToQueryable().Where(x => x.Key > 5 && x.Value.Age < 9).Select(x => x.Value.Age).Sum());

            Assert.AreEqual(DataSize, cache.ToQueryable().Count());
            Assert.AreEqual(DataSize, cache.ToQueryable().Count(x => x.Key < DataSize));
        }

        [Test]
        public void TestStrings()
        {
            var cache = GetCache();

            Assert.AreEqual(DataSize, cache.ToQueryable().Count(x => x.Value.Name.Contains("erson")));
            Assert.AreEqual(11, cache.ToQueryable().Count(x => x.Value.Name.StartsWith("Person_9")));
            Assert.AreEqual(1, cache.ToQueryable().Count(x => x.Value.Name.EndsWith("_99")));

            Assert.AreEqual(DataSize, cache.ToQueryable().Count(x => x.Value.Name.ToLower().StartsWith("person")));
            Assert.AreEqual(DataSize, cache.ToQueryable().Count(x => x.Value.Name.ToUpper().StartsWith("PERSON")));
        }

        [Test]
        public void TestSameCacheJoin()
        {
            //var res = cache.Query(new SqlQuery("LinqPerson",
            //    "from LinqPerson, LinqOrganization where LinqPerson.OrganizationId = LinqOrganization.Id and LinqOrganization.Name = ?", "Org_1"))
            //    .GetAll();

            // Select persons in specific organization
            var organizations = GetOrgCache().ToQueryable().Where(org => org.Value.Name == "Org_1");
            var persons = GetCache().ToQueryable();

            var res = persons.Join(organizations, person => person.Value.OrganizationId, org => org.Value.Id,
                (person, org) => new {Person = person.Value, Org = org.Value}).ToList();

            Assert.AreEqual(DataSize / 2, res.Count);

            Assert.IsTrue(res.All(r => r.Person.OrganizationId == r.Org.Id));

            // Test full projection (selects pair of ICacheEntry)
            var res2 = persons.Join(organizations, person => person.Value.OrganizationId, org => org.Value.Id,
                (person, org) => new { Person = person, Org = org }).ToList();

            Assert.AreEqual(DataSize / 2, res2.Count);
        }

        [Test]
        public void TestIntrospection()
        {
            var cache = GetCache();

            // Check regular query
            var query = (ICacheQueryable) cache.ToQueryable().Where(x => x.Key > 10);

            Assert.AreEqual(cache.Name, query.CacheName);
            Assert.AreEqual(cache.Ignite, query.Ignite);
            Assert.AreEqual("", query.ToTraceString());

            // Check fields query
            var fieldsQuery = (ICacheQueryable) cache.ToQueryable().Select(x => x.Value.Name);

            Assert.AreEqual(cache.Name, fieldsQuery.CacheName);
            Assert.AreEqual(cache.Ignite, fieldsQuery.Ignite);
            Assert.AreEqual("", query.ToTraceString());
        }

        private static ICache<int, LinqPerson> GetCache()
        {
            return GetCacheOf<LinqPerson>();
        }

        private static ICache<int, LinqOrganization> GetOrgCache()
        {
            return GetCacheOf<LinqOrganization>();
        }

        private static ICache<int, T> GetCacheOf<T>()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, T>(new CacheConfiguration(CacheName,
                    new QueryEntity(typeof (int), typeof (LinqPerson)),
                        new QueryEntity(typeof (int), typeof (LinqOrganization))));
        }

        public class LinqPerson : IBinarizable
        {
            public LinqPerson(int age, string name)
            {
                Age = age;
                Name = name;
            }

            [QuerySqlField(Name = "age1")]
            public int Age { get; set; }

            [QuerySqlField]
            public string Name { get; set; }

            [QuerySqlField]
            public LinqAddress Address { get; set; }

            [QuerySqlField]
            public int OrganizationId { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("age1", Age);
                writer.WriteString("name", Name);
                writer.WriteInt("OrganizationId", OrganizationId);
                writer.WriteObject("Address", Address);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Age = reader.ReadInt("age1");
                Name = reader.ReadString("name");
                OrganizationId = reader.ReadInt("OrganizationId");
                Address = reader.ReadObject<LinqAddress>("Address");
            }
        }

        public class LinqAddress
        {
            [QuerySqlField]
            public int Zip { get; set; }

            [QuerySqlField]
            public string Street { get; set; }
        }

        public class LinqOrganization
        {
            [QuerySqlField]
            public int Id { get; set; }

            [QuerySqlField]
            public string Name { get; set; }
        }
    }
}

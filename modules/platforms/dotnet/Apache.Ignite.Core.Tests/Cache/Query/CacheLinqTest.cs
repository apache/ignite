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

// ReSharper disable SuspiciousTypeConversion.Global
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
        private const string PersonOrgCacheName = null;

        /** Cache name. */
        private const string PersonSecondCacheName = "person_cache";

        /** Role cache name. */
        private const string RoleCacheName = "role_cache";

        /** */
        private const int RoleCount = 3;

        /** */
        private const int PersonCount = 100;

        /** */
        private bool _runDbConsole;

        /** */
        private static readonly DateTime StartDateTime = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

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
                BinaryConfiguration = new BinaryConfiguration(typeof (Person),
                    typeof (Organization), typeof (Address), typeof (Role), typeof (RoleKey))
            });

            var cache = GetPersonOrgCache();
            var personCache = GetSecondPersonCacheCache();

            for (var i = 0; i < PersonCount; i++)
            {
                cache.Put(i, new Person(i, "Person_" + i)
                {
                    Address = new Address {Zip = i, Street = "Street " + i},
                    OrganizationId = i%2 + 1000,
                    Birthday = StartDateTime.AddYears(i)
                });

                var i2 = i + PersonCount;
                personCache.Put(i2, new Person(i2, "Person_" + i2)
                {
                    Address = new Address {Zip = i2, Street = "Street " + i2},
                    OrganizationId = i%2 + 1000,
                    Birthday = StartDateTime.AddYears(i)
                });
            }

            var orgCache = GetOrgCache();

            orgCache[1000] = new Organization {Id = 1000, Name = "Org_0"};
            orgCache[1001] = new Organization {Id = 1001, Name = "Org_1"};

            var roleCache = GetRoleCache();

            roleCache[new RoleKey(1, 101)] = new Role {Name = "Role_1", Date = StartDateTime };
            roleCache[new RoleKey(2, 102)] = new Role {Name = "Role_2", Date = StartDateTime.AddYears(1)};
            roleCache[new RoleKey(3, 103)] = new Role {Name = null, Date = StartDateTime.AddYears(2)};
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
            // There are both persons and organizations in the same cache, but query should only return specific type
            Assert.AreEqual(PersonCount, GetPersonOrgCache().ToQueryable().ToArray().Length);
            Assert.AreEqual(RoleCount, GetRoleCache().ToQueryable().ToArray().Length);
        }

        [Test]
        public void TestWhere()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            Assert.AreEqual(10, cache.Where(x => x.Value.Age < 10).ToArray().Length);
            Assert.AreEqual(10, cache.Where(x => x.Value.Address.Zip < 10).ToArray().Length);
            Assert.AreEqual(19, cache.Where(x => x.Value.Age > 10 && x.Value.Age < 30).ToArray().Length);
            Assert.AreEqual(20, cache.Where(x => x.Value.Age > 10).Count(x => x.Value.Age < 30 || x.Value.Age == 50));
            Assert.AreEqual(15, cache.Where(x => x.Key < 15).ToArray().Length);
            Assert.AreEqual(15, cache.Where(x => -x.Key > -15).ToArray().Length);

            Assert.AreEqual(1, GetRoleCache().ToQueryable().Where(x => x.Key.Foo < 2).ToArray().Length);
            Assert.AreEqual(2, GetRoleCache().ToQueryable().Where(x => x.Key.Bar > 2 && x.Value.Name != "11")
                .ToArray().Length);
        }

        [Test]
        public void TestSingleFieldQuery()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            // Multiple values
            Assert.AreEqual(new[] {0, 1, 2},
                cache.Where(x => x.Key < 3).Select(x => x.Value.Address.Zip).ToArray());

            // Single value
            Assert.AreEqual(0, cache.Where(x => x.Key < 0).Select(x => x.Value.Age).FirstOrDefault());
            Assert.AreEqual(3, cache.Where(x => x.Key == 3).Select(x => x.Value.Age).FirstOrDefault());
            Assert.AreEqual(3, cache.Where(x => x.Key == 3).Select(x => x.Value).Single().Age);
            Assert.AreEqual(3, cache.Select(x => x.Key).Single(x => x == 3));
            Assert.AreEqual(7,
                cache.Select(x => x.Value)
                    .Where(x => x.Age == 7)
                    .Select(x => x.Address)
                    .Where(x => x.Zip > 0)
                    .Select(x => x.Zip)
                    .Single());
        }

        [Test]
        public void TestFieldProjection()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            // Project whole cache entry to anonymous class
            Assert.AreEqual(5, cache.Where(x => x.Key == 5).Select(x => new { Foo = x }).Single().Foo.Key);
        }

        [Test]
        public void TestMultiFieldQuery()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            // Test anonymous type (ctor invoke)
            var data = cache.Where(x => x.Key < 5)
                .Select(x => new {Key = x.Key + 20, Age = x.Value.Age + 10, x.Value.Address})
                .ToArray();

            Assert.AreEqual(5, data.Length);

            foreach (var t in data)
            {
                Assert.AreEqual(t.Age - 10, t.Key - 20);
                Assert.AreEqual(t.Age - 10, t.Address.Zip);
            }
        }

        [Test]
        public void TestScalarQuery()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            Assert.AreEqual(PersonCount - 1, cache.Max(x => x.Value.Age));
            Assert.AreEqual(0, cache.Min(x => x.Value.Age));

            Assert.AreEqual(21, cache.Where(x => x.Key > 5 && x.Value.Age < 9).Select(x => x.Value.Age).Sum());

            Assert.AreEqual(PersonCount, cache.Count());
            Assert.AreEqual(PersonCount, cache.Count(x => x.Key < PersonCount));
        }

        [Test]
        public void TestStrings()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            Assert.AreEqual(PersonCount, cache.Count(x => x.Value.Name.Contains("erson")));
            Assert.AreEqual(11, cache.Count(x => x.Value.Name.StartsWith("Person_9")));
            Assert.AreEqual(1, cache.Count(x => x.Value.Name.EndsWith("_99")));

            Assert.AreEqual(PersonCount, cache.Count(x => x.Value.Name.ToLower().StartsWith("person")));
            Assert.AreEqual(PersonCount, cache.Count(x => x.Value.Name.ToUpper().StartsWith("PERSON")));
        }

        [Test]
        public void TestAggregates()
        {
            var cache = GetPersonOrgCache().ToQueryable();

            Assert.AreEqual(PersonCount, cache.Count());
            Assert.AreEqual(PersonCount, cache.Select(x => x.Key).Count());
            
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Assert.Throws<NotSupportedException>(() => cache.Select(x => new {x.Key, x.Value}).Count());

            Assert.AreEqual(2, cache.Select(x => x.Value.OrganizationId).Distinct().Count());
        }

        [Test]
        public void TestSameCacheJoin()
        {
            // Select persons in specific organization
            var organizations = GetOrgCache().ToQueryable();
            var persons = GetPersonOrgCache().ToQueryable();

            var res = persons.Join(organizations, person => person.Value.OrganizationId + 3, org => org.Value.Id + 3,
                (person, org) => new {Person = person.Value, Org = org.Value})
                .Where(x => x.Org.Name == "Org_1")
                .ToList();

            Assert.AreEqual(PersonCount / 2, res.Count);

            Assert.IsTrue(res.All(r => r.Person.OrganizationId == r.Org.Id));

            // Test full projection (selects pair of ICacheEntry)
            var res2 = persons.Join(organizations, person => person.Value.OrganizationId - 1, org => org.Value.Id - 1,
                (person, org) => new {Person = person, Org = org})
                .Where(x => x.Org.Value.Name.ToLower() == "org_0")
                .ToList();

            Assert.AreEqual(PersonCount / 2, res2.Count);
        }

        [Test]
        public void TestMultiKeyJoin()
        {
            var organizations = GetOrgCache().ToQueryable();
            var persons = GetPersonOrgCache().ToQueryable();

            var multiKey =
                from person in persons
                join org in organizations on
                    new { OrgId = person.Value.OrganizationId, person.Key } equals
                    new { OrgId = org.Value.Id, Key = org.Key - 1000 }
                where person.Key == 1
                select new { PersonName = person.Value.Name, OrgName = org.Value.Name };

            Assert.AreEqual("Person_1", multiKey.Single().PersonName);
        }

        [Test]
        public void TestCrossCacheJoin()
        {
            var persons = GetPersonOrgCache().ToQueryable();
            var roles = GetRoleCache().ToQueryable();

            var res = persons.Join(roles, person => person.Key, role => role.Key.Foo, (person, role) => role)
                .ToArray();

            Assert.AreEqual(RoleCount, res.Length);
            Assert.AreEqual(101, res[0].Key.Bar);
        }

        [Test]
        public void TestMultiCacheJoin()
        {
            var organizations = GetOrgCache().ToQueryable();
            var persons = GetPersonOrgCache().ToQueryable();
            var roles = GetRoleCache().ToQueryable();

            var res = roles.Join(persons, role => role.Key.Foo, person => person.Key,
                (role, person) => new {person, role})
                .Join(organizations, pr => pr.person.Value.OrganizationId, org => org.Value.Id,
                    (pr, org) => new {org, pr.person, pr.role}).ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        [Test]
        public void TestMultiCacheJoinSubquery()
        {
            var organizations = GetOrgCache().ToQueryable().Where(x => x.Key == 1001);
            var persons = GetPersonOrgCache().ToQueryable().Where(x => x.Key < 20);
            var roles = GetRoleCache().ToQueryable().Where(x => x.Key.Foo >= 0);

            var res = roles.Join(persons, role => role.Key.Foo, person => person.Key,
                (role, person) => new {person, role})
                .Join(organizations, pr => pr.person.Value.OrganizationId, org => org.Value.Id,
                    (pr, org) => new {org, pr.person, pr.role}).ToArray();

            Assert.AreEqual(2, res.Length);
        }

        [Test]
        public void TestOuterJoin()
        {
            var persons = GetPersonOrgCache().ToQueryable();
            var roles = GetRoleCache().ToQueryable();

            var res = persons.Join(roles.Where(r => r.Key.Bar > 0).DefaultIfEmpty(),
                person => person.Key, role => role.Key.Foo,
                (person, role) => new
                {
                    PersonName = person.Value.Name,
                    RoleName = role.Value.Name
                })
                .Where(x => x.PersonName != " ")
                .ToArray();

            Assert.AreEqual(PersonCount, res.Length);
        }

        [Test]
        public void TestSubqueryJoin()
        {
            var persons = GetPersonOrgCache().ToQueryable().Where(x => x.Key >= 0);

            var orgs = GetOrgCache().ToQueryable().Where(x => x.Key > 10);

            var res = persons.Join(orgs,
                p => p.Value.OrganizationId,
                o => o.Value.Id, (p, o) => p)
                .Where(x => x.Key >= 0)
                .ToList();

            Assert.AreEqual(PersonCount, res.Count);
        }

        [Test]
        public void TestInvalidJoin()
        {
            // Join on non-IQueryable
            Assert.Throws<NotSupportedException>(() =>
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                GetPersonOrgCache().ToQueryable().Join(GetOrgCache(), p => p.Key, o => o.Key, (p, o) => p).ToList());
        }

        [Test]
        public void TestMultipleFrom()
        {
            var persons = GetPersonOrgCache().ToQueryable().Where(x => x.Key < PersonCount);
            var roles = GetRoleCache().ToQueryable().Where(x => x.Value.Name != "1");

            var resQuery = 
                from person in persons
                from role in roles
                where person.Key == role.Key.Foo
                select new {Person = person.Value.Name, Role = role.Value.Name};

            var res = resQuery.ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        [Test]
        public void TestUnion()
        {
            // Direct union
            var persons = GetPersonOrgCache().ToQueryable();
            var persons2 = GetSecondPersonCacheCache().ToQueryable();

            var res = persons.Union(persons2).ToArray();

            Assert.AreEqual(PersonCount * 2, res.Length);

            // Subquery
            var roles = GetRoleCache().ToQueryable().Select(x => -x.Key.Foo);
            var ids = GetPersonOrgCache().ToQueryable().Select(x => x.Key).Union(roles).ToArray();

            Assert.AreEqual(RoleCount + PersonCount, ids.Length);
        }

        [Test]
        public void TestIntersect()
        {
            // Direct intersect
            var persons = GetPersonOrgCache().ToQueryable();
            var persons2 = GetSecondPersonCacheCache().ToQueryable();

            var res = persons.Intersect(persons2).ToArray();

            Assert.AreEqual(0, res.Length);

            // Subquery
            var roles = GetRoleCache().ToQueryable().Select(x => x.Key.Foo);
            var ids = GetPersonOrgCache().ToQueryable().Select(x => x.Key).Intersect(roles).ToArray();

            Assert.AreEqual(RoleCount, ids.Length);
        }

        [Test]
        public void TestExcept()
        {
            // Direct except
            var persons = GetPersonOrgCache().ToQueryable();
            var persons2 = GetSecondPersonCacheCache().ToQueryable();

            var res = persons.Except(persons2).ToArray();

            Assert.AreEqual(PersonCount, res.Length);

            // Subquery
            var roles = GetRoleCache().ToQueryable().Select(x => x.Key.Foo);
            var ids = GetPersonOrgCache().ToQueryable().Select(x => x.Key).Except(roles).ToArray();

            Assert.AreEqual(PersonCount - RoleCount, ids.Length);
        }

        [Test]
        public void TestOrdering()
        {
            var persons = GetPersonOrgCache().ToQueryable()
                .OrderByDescending(x => x.Key)
                .ThenBy(x => x.Value.Age)
                .ToArray();

            Assert.AreEqual(Enumerable.Range(0, PersonCount).Reverse().ToArray(), persons.Select(x => x.Key).ToArray());

            var personsByOrg = GetPersonOrgCache().ToQueryable()
                .Join(GetOrgCache().ToQueryable(), p => p.Value.OrganizationId, o => o.Value.Id,
                    (p, o) => new
                    {
                        PersonId = p.Key,
                        PersonName = p.Value.Name.ToUpper(),
                        OrgName = o.Value.Name
                    })
                .OrderBy(x => x.OrgName.ToLower())
                .ThenBy(x => x.PersonName)
                .ToArray();

            var expectedIds = Enumerable.Range(0, PersonCount)
                .OrderBy(x => (x%2).ToString())
                .ThenBy(x => x.ToString())
                .ToArray();

            var actualIds = personsByOrg.Select(x => x.PersonId).ToArray();

            Assert.AreEqual(expectedIds, actualIds);
        }

        [Test]
        public void TestNulls()
        {
            var roles = GetRoleCache().ToQueryable();

            var nullNameRole = roles.Single(x => x.Value.Name == null);
            Assert.AreEqual(null, nullNameRole.Value.Name);

            var nonNullNameRoles = roles.Where(x => x.Value.Name != null);
            Assert.AreEqual(RoleCount - 1, nonNullNameRoles.Count());
        }

        [Test]
        public void TestDateTime()
        {
            // TODO: DateTimes in binary format (serializable) won't work in comparisons, what do we do?
            // TODO: QueryField.TypeName override allows storing timestamps, but query parameters are passed incorrectly
            // TODO: QueryDateTimeField?

            var roles = GetRoleCache().ToQueryable();
            var persons = GetPersonOrgCache().ToQueryable();

            // Test retrieval
            var dates = roles.OrderBy(x => x.Key).Select(x => x.Value.Date).ToArray();
            var expDates = new[] {StartDateTime, StartDateTime.AddYears(1), StartDateTime.AddYears(2)};
            Assert.AreEqual(expDates, dates);

            // Filtering
            Assert.AreEqual(1, persons.Count(x => x.Value.Birthday == StartDateTime));
            Assert.AreEqual(PersonCount, persons.Count(x => x.Value.Birthday >= StartDateTime));

            // Joins
        }

        [Test]
        public void TestIntrospection()
        {
            var cache = GetPersonOrgCache();

            // Check regular query
            var query = (ICacheQueryable) cache.ToQueryable().Where(x => x.Key > 10);

            Assert.AreEqual(cache.Name, query.CacheName);
            Assert.AreEqual(cache.Ignite, query.Ignite);
            Assert.AreEqual("Fields Query [SQL=select T0._key, T0._val from \"\".Person as T0 " +
                            "where (T0._key > ?), Parameters=10]", query.ToTraceString());

            // Check fields query
            var fieldsQuery = (ICacheQueryable) cache.ToQueryable().Select(x => x.Value.Name);

            Assert.AreEqual(cache.Name, fieldsQuery.CacheName);
            Assert.AreEqual(cache.Ignite, fieldsQuery.Ignite);
            Assert.AreEqual("Fields Query [SQL=select T0.Name from \"\".Person as T0, Parameters=]",
                fieldsQuery.ToTraceString());
        }

        private static ICache<int, Person> GetPersonOrgCache()
        {
            return GetCacheOf<Person>();
        }

        private static ICache<int, Organization> GetOrgCache()
        {
            return GetCacheOf<Organization>();
        }

        private static ICache<int, T> GetCacheOf<T>()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, T>(new CacheConfiguration(PersonOrgCacheName,
                    new QueryEntity(typeof (int), typeof (Person)),
                        new QueryEntity(typeof (int), typeof (Organization))));
        }

        private static ICache<RoleKey, Role> GetRoleCache()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<RoleKey, Role>(new CacheConfiguration(RoleCacheName,
                    new QueryEntity(typeof(RoleKey), typeof(Role))));
        }

        private static ICache<int, Person> GetSecondPersonCacheCache()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, Person>(new CacheConfiguration(PersonSecondCacheName,
                    new QueryEntity(typeof(int), typeof(Person))));
        }

        public class Person : IBinarizable
        {
            public Person(int age, string name)
            {
                Age = age;
                Name = name;
            }

            [QuerySqlField(Name = "age1")] public int Age { get; set; }

            [QuerySqlField] public string Name { get; set; }

            [QuerySqlField] public Address Address { get; set; }

            [QuerySqlField] public int OrganizationId { get; set; }

            [QuerySqlField(TypeName = "java.sql.Timestamp")] public DateTime? Birthday { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("age1", Age);
                writer.WriteString("name", Name);
                writer.WriteInt("OrganizationId", OrganizationId);
                writer.WriteObject("Address", Address);
                writer.WriteTimestamp("Birthday", Birthday);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Age = reader.ReadInt("age1");
                Name = reader.ReadString("name");
                OrganizationId = reader.ReadInt("OrganizationId");
                Address = reader.ReadObject<Address>("Address");
                Birthday = reader.ReadTimestamp("Birthday");
            }
        }

        public class Address
        {
            [QuerySqlField] public int Zip { get; set; }
            [QuerySqlField] public string Street { get; set; }
        }

        public class Organization
        {
            [QuerySqlField] public int Id { get; set; }
            [QuerySqlField] public string Name { get; set; }
        }

        public class Role
        {
            [QuerySqlField] public string Name { get; set; }
            [QuerySqlField] public DateTime Date { get; set; }
        }

        public struct RoleKey : IEquatable<RoleKey>
        {
            private readonly int _foo;
            private readonly long _bar;

            public RoleKey(int foo, long bar)
            {
                _foo = foo;
                _bar = bar;
            }

            [QuerySqlField(Name = "_foo")]
            public int Foo
            {
                get { return _foo; }
            }

            [QuerySqlField(Name = "_bar")]
            public long Bar
            {
                get { return _bar; }
            }

            public bool Equals(RoleKey other)
            {
                return _foo == other._foo && _bar == other._bar;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is RoleKey && Equals((RoleKey) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (_foo*397) ^ _bar.GetHashCode();
                }
            }

            public static bool operator ==(RoleKey left, RoleKey right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(RoleKey left, RoleKey right)
            {
                return !left.Equals(right);
            }
        }
    }
}

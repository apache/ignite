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
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable StringIndexOfIsCultureSpecific.1
// ReSharper disable StringIndexOfIsCultureSpecific.2
// ReSharper disable StringCompareToIsCultureSpecific
// ReSharper disable StringCompareIsCultureSpecific.1
// ReSharper disable UnusedMemberInSuper.Global
namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.RegularExpressions;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
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
        private const int PersonCount = 900;

        /** */
        private bool _runDbConsole;

        /** */
        private static readonly DateTime StartDateTime = new DateTime(2000, 5, 17, 15, 4, 5, DateTimeKind.Utc);

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _runDbConsole = false;  // set to true to open H2 console

            if (_runDbConsole)
                Environment.SetEnvironmentVariable("IGNITE_H2_DEBUG_CONSOLE", "true");

            Ignition.Start(GetConfig());
            Ignition.Start(GetConfig("grid2"));

            // Populate caches
            var cache = GetPersonCache();
            var personCache = GetSecondPersonCache();

            for (var i = 0; i < PersonCount; i++)
            {
                cache.Put(i, new Person(i, string.Format(" Person_{0}  ", i))
                {
                    Address = new Address {Zip = i, Street = "Street " + i, AliasTest = i},
                    OrganizationId = i%2 + 1000,
                    Birthday = StartDateTime.AddYears(i),
                    AliasTest = -i
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
            orgCache[1002] = new Organization {Id = 1002, Name = null};

            var roleCache = GetRoleCache();

            roleCache[new RoleKey(1, 101)] = new Role {Name = "Role_1", Date = StartDateTime};
            roleCache[new RoleKey(2, 102)] = new Role {Name = "Role_2", Date = StartDateTime.AddYears(1)};
            roleCache[new RoleKey(3, 103)] = new Role {Name = null, Date = StartDateTime.AddHours(5432)};
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private static IgniteConfiguration GetConfig(string gridName = null)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
                    typeof(Organization), typeof(Address), typeof(Role), typeof(RoleKey), typeof(Numerics)),
                GridName = gridName
            };
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            if (_runDbConsole)
                Thread.Sleep(Timeout.Infinite);
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the empty query.
        /// </summary>
        [Test]
        public void TestEmptyQuery()
        {
            // There are both persons and organizations in the same cache, but query should only return specific type
            Assert.AreEqual(PersonCount, GetPersonCache().AsCacheQueryable().ToArray().Length);
            Assert.AreEqual(RoleCount, GetRoleCache().AsCacheQueryable().ToArray().Length);
        }

        /// <summary>
        /// Tests where clause.
        /// </summary>
        [Test]
        public void TestWhere()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // Test const and var parameters
            const int age = 10;
            var key = 15;

            Assert.AreEqual(age, cache.Where(x => x.Value.Age < age).ToArray().Length);
            Assert.AreEqual(age, cache.Where(x => x.Value.Address.Zip < age).ToArray().Length);
            Assert.AreEqual(19, cache.Where(x => x.Value.Age > age && x.Value.Age < 30).ToArray().Length);
            Assert.AreEqual(20, cache.Where(x => x.Value.Age > age).Count(x => x.Value.Age < 30 || x.Value.Age == 50));
            Assert.AreEqual(key, cache.Where(x => x.Key < key).ToArray().Length);
            Assert.AreEqual(key, cache.Where(x => -x.Key > -key).ToArray().Length);

            Assert.AreEqual(1, GetRoleCache().AsCacheQueryable().Where(x => x.Key.Foo < 2).ToArray().Length);
            Assert.AreEqual(2, GetRoleCache().AsCacheQueryable().Where(x => x.Key.Bar > 2 && x.Value.Name != "11")
                .ToArray().Length);
        }

        /// <summary>
        /// Tests the single field query.
        /// </summary>
        [Test]
        public void TestSingleFieldQuery()
        {
            var cache = GetPersonCache().AsCacheQueryable();

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

        /// <summary>
        /// Tests the field projection.
        /// </summary>
        [Test]
        public void TestFieldProjection()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // Project whole cache entry to anonymous class
            Assert.AreEqual(5, cache.Where(x => x.Key == 5).Select(x => new { Foo = x }).Single().Foo.Key);
        }

        /// <summary>
        /// Tests the multi field query.
        /// </summary>
        [Test]
        public void TestMultiFieldQuery()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // Test anonymous type (ctor invoke)
            var data = cache
                .Select(x => new {Id = x.Key + 20, Age_ = x.Value.Age + 10, Addr = x.Value.Address})
                .Where(x => x.Id < 25)
                .ToArray();

            Assert.AreEqual(5, data.Length);

            foreach (var t in data)
            {
                Assert.AreEqual(t.Age_ - 10, t.Id - 20);
                Assert.AreEqual(t.Age_ - 10, t.Addr.Zip);
            }
        }

        /// <summary>
        /// Tests the scalar query.
        /// </summary>
        [Test]
        public void TestScalarQuery()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            Assert.AreEqual(PersonCount - 1, cache.Max(x => x.Value.Age));
            Assert.AreEqual(0, cache.Min(x => x.Value.Age));

            Assert.AreEqual(21, cache.Where(x => x.Key > 5 && x.Value.Age < 9).Select(x => x.Value.Age).Sum());

            Assert.AreEqual(PersonCount, cache.Count());
            Assert.AreEqual(PersonCount, cache.Count(x => x.Key < PersonCount));
        }

        /// <summary>
        /// Tests strings.
        /// </summary>
        [Test]
        public void TestStrings()
        {
            var strings = GetSecondPersonCache().AsCacheQueryable().Select(x => x.Value.Name);

            CheckFunc(x => x.ToLower(), strings);
            CheckFunc(x => x.ToUpper(), strings);
            CheckFunc(x => x.StartsWith("Person_9"), strings);
            CheckFunc(x => x.EndsWith("7"), strings);
            CheckFunc(x => x.Contains("son_3"), strings);
            CheckFunc(x => x.Length, strings);

            CheckFunc(x => x.IndexOf("9"), strings);
            CheckFunc(x => x.IndexOf("7", 4), strings);

            CheckFunc(x => x.Substring(4), strings);
            CheckFunc(x => x.Substring(4, 5), strings);

            CheckFunc(x => x.Trim(), strings);
            CheckFunc(x => x.Trim('P'), strings);
            CheckFunc(x => x.Trim('3'), strings);
            CheckFunc(x => x.TrimStart('P'), strings);
            CheckFunc(x => x.TrimStart('3'), strings);
            Assert.Throws<NotSupportedException>(() => CheckFunc(x => x.TrimStart('P', 'e'), strings));
            CheckFunc(x => x.TrimEnd('P'), strings);
            CheckFunc(x => x.TrimEnd('3'), strings);

            CheckFunc(x => Regex.Replace(x, @"son.\d", "kele!"), strings);
            CheckFunc(x => x.Replace("son", ""), strings);
            CheckFunc(x => x.Replace("son", "kele"), strings);

            // Concat
            CheckFunc(x => x + x, strings);

            // String + int
            CheckFunc(x => x + 10, strings);
        }

        /// <summary>
        /// Tests aggregates.
        /// </summary>
        [Test]
        public void TestAggregates()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            Assert.AreEqual(PersonCount, cache.Count());
            Assert.AreEqual(PersonCount, cache.Select(x => x.Key).Count());
            Assert.AreEqual(2, cache.Select(x => x.Value.OrganizationId).Distinct().Count());

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Assert.Throws<NotSupportedException>(() => cache.Select(x => new {x.Key, x.Value}).Count());

            // Min/max/sum/avg
            var ints = cache.Select(x => x.Key);
            Assert.AreEqual(0, ints.Min());
            Assert.AreEqual(PersonCount - 1, ints.Max());
            Assert.AreEqual(ints.ToArray().Sum(), ints.Sum());
            Assert.AreEqual((int)ints.ToArray().Average(), (int)ints.Average());

            var dupInts = ints.Select(x => x/10);  // duplicate values
            CollectionAssert.AreEquivalent(dupInts.ToArray().Distinct().ToArray(), dupInts.Distinct().ToArray());
            Assert.AreEqual(dupInts.ToArray().Distinct().Sum(), dupInts.Distinct().Sum());

            // All/any
            Assert.IsFalse(ints.Where(x => x > -5).Any(x => x > PersonCount && x > 0));
            Assert.IsTrue(ints.Any(x => x < PersonCount / 2));

            // Skip/take
            var keys = cache.Select(x => x.Key).OrderBy(x => x);
            Assert.AreEqual(new[] {0, 1}, keys.Take(2).ToArray());
            Assert.AreEqual(new[] {1, 2}, keys.Skip(1).Take(2).ToArray());
            Assert.AreEqual(new[] {PersonCount - 2, PersonCount - 1}, keys.Skip(PersonCount - 2).ToArray());
        }

        /// <summary>
        /// Tests aggregates with all clause.
        /// </summary>
        [Test]
        public void TestAggregatesAll()
        {
            var ints = GetPersonCache().AsCacheQueryable().Select(x => x.Key);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<NotSupportedException>(() => ints.Where(x => x > -10)
                .All(x => x < PersonCount && x >= 0));

            Assert.IsTrue(ex.Message.StartsWith("Operator is not supported: All"));
        }

        /// <summary>
        /// Tests conditions.
        /// </summary>
        [Test]
        public void TestConditions()
        {
            var persons = GetPersonCache().AsCacheQueryable();

            var res = persons.Select(x => new {Foo = x.Key%2 == 0 ? "even" : "odd", x.Value}).ToArray();
            Assert.AreEqual("even", res[0].Foo);
            Assert.AreEqual("odd", res[1].Foo);

            var roles = GetRoleCache().AsCacheQueryable();
            CheckFunc(x => x.Value.Name ?? "def_name", roles);
        }

        /// <summary>
        /// Tests the same cache join.
        /// </summary>
        [Test]
        public void TestSameCacheJoin()
        {
            // Select persons in specific organization
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

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

        /// <summary>
        /// Tests the multi key join.
        /// </summary>
        [Test]
        public void TestMultiKeyJoin()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var multiKey =
                from person in persons
                join org in organizations on
                    new { OrgId = person.Value.OrganizationId, person.Key } equals
                    new { OrgId = org.Value.Id, Key = org.Key - 1000 }
                where person.Key == 1
                select new { PersonName = person.Value.Name, OrgName = org.Value.Name };

            Assert.AreEqual(" Person_1  ", multiKey.Single().PersonName);
        }

        /// <summary>
        /// Tests the cross cache join.
        /// </summary>
        [Test]
        public void TestCrossCacheJoin()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();

            var res = persons.Join(roles, person => person.Key, role => role.Key.Foo, (person, role) => role)
                .ToArray();

            Assert.AreEqual(RoleCount, res.Length);
            Assert.AreEqual(101, res[0].Key.Bar);
        }

        /// <summary>
        /// Tests the cross cache join.
        /// </summary>
        [Test]
        public void TestCrossCacheJoinInline()
        {
            var res = GetPersonCache().AsCacheQueryable().Join(GetRoleCache().AsCacheQueryable(), 
                person => person.Key, role => role.Key.Foo, (person, role) => role).ToArray();

            Assert.AreEqual(RoleCount, res.Length);
            Assert.AreEqual(101, res[0].Key.Bar);
        }

        /// <summary>
        /// Tests the multi cache join.
        /// </summary>
        [Test]
        public void TestMultiCacheJoin()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();

            var res = roles.Join(persons, role => role.Key.Foo, person => person.Key,
                (role, person) => new {person, role})
                .Join(organizations, pr => pr.person.Value.OrganizationId, org => org.Value.Id,
                    (pr, org) => new {org, pr.person, pr.role}).ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        /// <summary>
        /// Tests the multi cache join subquery.
        /// </summary>
        [Test]
        public void TestMultiCacheJoinSubquery()
        {
            var organizations = GetOrgCache().AsCacheQueryable().Where(x => x.Key == 1001);
            var persons = GetPersonCache().AsCacheQueryable().Where(x => x.Key < 20);
            var roles = GetRoleCache().AsCacheQueryable().Where(x => x.Key.Foo >= 0);

            var res = roles.Join(persons, role => role.Key.Foo, person => person.Key,
                (role, person) => new {person, role})
                .Join(organizations, pr => pr.person.Value.OrganizationId, org => org.Value.Id,
                    (pr, org) => new {org, pr.person, pr.role}).ToArray();

            Assert.AreEqual(2, res.Length);
        }

        /// <summary>
        /// Tests the outer join.
        /// </summary>
        [Test]
        public void TestOuterJoin()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();

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

        /// <summary>
        /// Tests the subquery join.
        /// </summary>
        [Test]
        public void TestSubqueryJoin()
        {
            var persons = GetPersonCache().AsCacheQueryable().Where(x => x.Key >= 0);

            var orgs = GetOrgCache().AsCacheQueryable().Where(x => x.Key > 10);

            var res = persons.Join(orgs,
                p => p.Value.OrganizationId,
                o => o.Value.Id, (p, o) => p)
                .Where(x => x.Key >= 0)
                .ToList();

            Assert.AreEqual(PersonCount, res.Count);
        }

        /// <summary>
        /// Tests the invalid join.
        /// </summary>
        [Test]
        public void TestInvalidJoin()
        {
            // Join on non-IQueryable
            var ex = Assert.Throws<NotSupportedException>(() =>
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                GetPersonCache().AsCacheQueryable().Join(GetOrgCache(), p => p.Key, o => o.Key, (p, o) => p).ToList());

            Assert.IsTrue(ex.Message.StartsWith("Unexpected query source"));
        }

        /// <summary>
        /// Tests query with multiple from clause.
        /// </summary>
        [Test]
        public void TestMultipleFrom()
        {
            var persons = GetPersonCache().AsCacheQueryable().Where(x => x.Key < PersonCount);
            var roles = GetRoleCache().AsCacheQueryable().Where(x => x.Value.Name != "1");

            var all = persons.SelectMany(person => roles.Select(role => new { role, person }));
            Assert.AreEqual(RoleCount * PersonCount, all.Count());

            var filtered = 
                from person in persons
                from role in roles
                where person.Key == role.Key.Foo
                select new {Person = person.Value.Name, Role = role.Value.Name};

            var res = filtered.ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        /// <summary>
        /// Tests query with multiple from clause with inline query sources.
        /// </summary>
        [Test]
        public void TestMultipleFromInline()
        {
            var filtered =
                from person in GetPersonCache().AsCacheQueryable()
                from role in GetRoleCache().AsCacheQueryable()
                where person.Key == role.Key.Foo
                select new {Person = person.Value.Name, Role = role.Value.Name};

            var res = filtered.ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        /// <summary>
        /// Tests the join of a table to itself.
        /// </summary>
        [Test]
        public void TestSelfJoin()
        {
            // Different queryables
            var p1 = GetPersonCache().AsCacheQueryable();
            var p2 = GetPersonCache().AsCacheQueryable();

            var qry = p1.Join(p2, x => x.Value.Age, x => x.Key, (x, y) => x.Key);
            Assert.AreEqual(PersonCount, qry.ToArray().Distinct().Count());

            // Same queryables
            var qry2 = p1.Join(p1, x => x.Value.Age, x => x.Key, (x, y) => x.Key);
            Assert.AreEqual(PersonCount, qry2.ToArray().Distinct().Count());
        }

        /// <summary>
        /// Tests the join of a table to itself with inline queryable.
        /// </summary>
        [Test]
        public void TestSelfJoinInline()
        {
            var qry = GetPersonCache().AsCacheQueryable().Join(GetPersonCache().AsCacheQueryable(), 
                x => x.Value.Age, x => x.Key, (x, y) => x.Key);

            Assert.AreEqual(PersonCount, qry.ToArray().Distinct().Count());
        }

        /// <summary>
        /// Tests the group by.
        /// </summary>
        [Test]
        public void TestGroupBy()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var orgs = GetOrgCache().AsCacheQueryable();

            // Simple, unordered
            CollectionAssert.AreEquivalent(new[] {1000, 1001},
                persons.GroupBy(x => x.Value.OrganizationId).Select(x => x.Key).ToArray());

            // Aggregate
            Assert.AreEqual(1000,
                persons.GroupBy(x => x.Value.OrganizationId).Select(x => x.Key).OrderBy(x => x).First());

            // Ordering and count
            var res1 =
                from p in persons
                orderby p.Value.Name
                group p by p.Value.OrganizationId
                into gs
                orderby gs.Key
                where gs.Count() > 10
                select new {Count = gs.Count(), OrgId = gs.Key, AvgAge = gs.Average(x => x.Value.Age)};

            var resArr = res1.ToArray();

            Assert.AreEqual(new[]
            {
                new {Count = PersonCount/2, OrgId = 1000, AvgAge = (double) PersonCount/2 - 1},
                new {Count = PersonCount/2, OrgId = 1001, AvgAge = (double) PersonCount/2}
            }, resArr);

            // Join and sum
            var res2 = persons.Join(orgs.Where(o => o.Key > 10), p => p.Value.OrganizationId, o => o.Key,
                (p, o) => new {p, o})
                .GroupBy(x => x.o.Value.Name)
                .Select(g => new {Org = g.Key, AgeSum = g.Select(x => x.p.Value.Age).Sum()});

            var resArr2 = res2.ToArray();

            Assert.AreEqual(new[]
            {
                new {Org = "Org_0", AgeSum = persons.Where(x => x.Value.OrganizationId == 1000).Sum(x => x.Value.Age)},
                new {Org = "Org_1", AgeSum = persons.Where(x => x.Value.OrganizationId == 1001).Sum(x => x.Value.Age)}
            }, resArr2);
        }

        /// <summary>
        /// Tests the union.
        /// </summary>
        [Test]
        public void TestUnion()
        {
            // Direct union
            var persons = GetPersonCache().AsCacheQueryable();
            var persons2 = GetSecondPersonCache().AsCacheQueryable();

            var res = persons.Union(persons2).ToArray();

            Assert.AreEqual(PersonCount * 2, res.Length);

            // Subquery
            var roles = GetRoleCache().AsCacheQueryable().Select(x => -x.Key.Foo);
            var ids = GetPersonCache().AsCacheQueryable().Select(x => x.Key).Union(roles).ToArray();

            Assert.AreEqual(RoleCount + PersonCount, ids.Length);
        }

        /// <summary>
        /// Tests intersect.
        /// </summary>
        [Test]
        public void TestIntersect()
        {
            // Direct intersect
            var persons = GetPersonCache().AsCacheQueryable();
            var persons2 = GetSecondPersonCache().AsCacheQueryable();

            var res = persons.Intersect(persons2).ToArray();

            Assert.AreEqual(0, res.Length);

            // Subquery
            var roles = GetRoleCache().AsCacheQueryable().Select(x => x.Key.Foo);
            var ids = GetPersonCache().AsCacheQueryable().Select(x => x.Key).Intersect(roles).ToArray();

            Assert.AreEqual(RoleCount, ids.Length);
        }

        /// <summary>
        /// Tests except.
        /// </summary>
        [Test]
        public void TestExcept()
        {
            // Direct except
            var persons = GetPersonCache().AsCacheQueryable();
            var persons2 = GetSecondPersonCache().AsCacheQueryable();

            var res = persons.Except(persons2).ToArray();

            Assert.AreEqual(PersonCount, res.Length);

            // Subquery
            var roles = GetRoleCache().AsCacheQueryable().Select(x => x.Key.Foo);
            var ids = GetPersonCache().AsCacheQueryable().Select(x => x.Key).Except(roles).ToArray();

            Assert.AreEqual(PersonCount - RoleCount, ids.Length);
        }

        /// <summary>
        /// Tests ordering.
        /// </summary>
        [Test]
        public void TestOrdering()
        {
            var persons = GetPersonCache().AsCacheQueryable()
                .OrderByDescending(x => x.Key)
                .ThenBy(x => x.Value.Age)
                .ToArray();

            Assert.AreEqual(Enumerable.Range(0, PersonCount).Reverse().ToArray(), persons.Select(x => x.Key).ToArray());

            var personsByOrg = GetPersonCache().AsCacheQueryable()
                .Join(GetOrgCache().AsCacheQueryable(), p => p.Value.OrganizationId, o => o.Value.Id,
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

        /// <summary>
        /// Tests IEnumerable.Contains.
        /// </summary>
        [Test]
        public void TestContains()
        {
            var cache = GetPersonCache().AsCacheQueryable();
            var orgCache = GetOrgCache().AsCacheQueryable();

            var keys = new[] { 1, 2, 3 };
            var emptyKeys = new int[0];

            var bigNumberOfKeys = 10000;
            var aLotOfKeys = Enumerable.Range(-bigNumberOfKeys + 10 - PersonCount, bigNumberOfKeys + PersonCount)
                .ToArray();
            var hashSetKeys = new HashSet<int>(keys);

            CheckWhereFunc(cache, e => new[] { 1, 2, 3 }.Contains(e.Key));
            CheckWhereFunc(cache, e => emptyKeys.Contains(e.Key));
            CheckWhereFunc(cache, e => new int[0].Contains(e.Key));
            CheckWhereFunc(cache, e => new int[0].Contains(e.Key));
            CheckWhereFunc(cache, e => new List<int> { 1, 2, 3 }.Contains(e.Key));
            CheckWhereFunc(cache, e => new List<int>(keys).Contains(e.Key));
            CheckWhereFunc(cache, e => aLotOfKeys.Contains(e.Key));
            CheckWhereFunc(cache, e => hashSetKeys.Contains(e.Key));
            CheckWhereFunc(cache, e => !keys.Contains(e.Key));
            CheckWhereFunc(orgCache, e => new[] { "Org_1", "NonExistentName", null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => !new[] { "Org_1", "NonExistentName", null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => new[] { "Org_1", null, null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => !new[] { "Org_1", null, null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => new string[] { null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => !new string[] { null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => !new string[] { null, null }.Contains(e.Value.Name));
            CheckWhereFunc(orgCache, e => new string[] { null, null }.Contains(e.Value.Name));

            //check passing a null object as collection
            int[] nullKeys = null;
            var nullKeysEntries = cache
                .Where(e => nullKeys.Contains(e.Key))
                .ToArray();
            Assert.AreEqual(0, nullKeysEntries.Length, "Evaluating 'null.Contains' should return zero results");


            Func<int[]> getKeysFunc = () => null;
            var funcNullKeyEntries = cache
                .Where(e => getKeysFunc().Contains(e.Key))
                .ToArray();
            Assert.AreEqual(0, funcNullKeyEntries.Length, "Evaluating 'null.Contains' should return zero results");


            // Check subselect from other cache
            var subSelectCount = cache
                .Count(entry => orgCache
                    .Where(orgEntry => orgEntry.Value.Name == "Org_1")
                    .Select(orgEntry => orgEntry.Key)
                    .Contains(entry.Value.OrganizationId));
            var orgNumberOne = orgCache
                .Where(orgEntry => orgEntry.Value.Name == "Org_1")
                .Select(orgEntry => orgEntry.Key)
                .First();
            var subSelectCheckCount = cache.Count(entry => entry.Value.OrganizationId == orgNumberOne);
            Assert.AreEqual(subSelectCheckCount, subSelectCount, "subselecting another CacheQueryable failed");

            var ex = Assert.Throws<NotSupportedException>(() =>
                CompiledQuery2.Compile((int[] k) => cache.Where(x => k.Contains(x.Key))));
            Assert.AreEqual("'Contains' clause coming from compiled query parameter is not supported.", ex.Message);
        }

        /// <summary>
        /// Tests nulls.
        /// </summary>
        [Test]
        public void TestNulls()
        {
            var roles = GetRoleCache().AsCacheQueryable();

            var nullNameRole = roles.Single(x => x.Value.Name == null);
            Assert.AreEqual(null, nullNameRole.Value.Name);

            var nonNullNameRoles = roles.Where(x => x.Value.Name != null);
            Assert.AreEqual(RoleCount - 1, nonNullNameRoles.Count());
        }

        /// <summary>
        /// Tests date time.
        /// </summary>
        [Test]
        public void TestDateTime()
        {
            var roles = GetRoleCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            // Invalid dateTime
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var ex = Assert.Throws<InvalidOperationException>(() =>
                roles.Where(x => x.Value.Date > DateTime.Now).ToArray());
            Assert.AreEqual("DateTime is not UTC. Only UTC DateTime can be used for interop with other platforms.", 
                ex.Message);

            // Test retrieval
            var dates = roles.OrderBy(x => x.Value.Date).Select(x => x.Value.Date);
            var expDates = GetRoleCache().Select(x => x.Value.Date).OrderBy(x => x).ToArray();
            Assert.AreEqual(expDates, dates.ToArray());

            // Filtering
            Assert.AreEqual(1, persons.Count(x => x.Value.Birthday == StartDateTime));
            Assert.AreEqual(PersonCount, persons.Count(x => x.Value.Birthday >= StartDateTime));
            Assert.Greater(persons.Count(x => x.Value.Birthday > DateTime.UtcNow), 1);

            // Joins
            var join = 
                from role in roles
                join person in persons on role.Value.Date equals person.Value.Birthday
                select person;

            Assert.AreEqual(2, join.Count());

            // Functions
            var strings = dates.Select(x => x.ToString("dd MM YYYY HH:mm:ss")).ToArray();
            Assert.AreEqual(new[] {"17 05 2000 15:04:05", "29 12 2000 23:04:05", "17 05 2001 15:04:05"}, strings);

            // Properties
            Assert.AreEqual(new[] {2000, 2000, 2001}, dates.Select(x => x.Year).ToArray());
            Assert.AreEqual(new[] {5, 12, 5}, dates.Select(x => x.Month).ToArray());
            Assert.AreEqual(new[] {17, 29, 17}, dates.Select(x => x.Day).ToArray());
            Assert.AreEqual(expDates.Select(x => x.DayOfYear).ToArray(), dates.Select(x => x.DayOfYear).ToArray());
            Assert.AreEqual(expDates.Select(x => x.DayOfWeek).ToArray(), dates.Select(x => x.DayOfWeek).ToArray());
            Assert.AreEqual(new[] {15, 23, 15}, dates.Select(x => x.Hour).ToArray());
            Assert.AreEqual(new[] { 4, 4, 4 }, dates.Select(x => x.Minute).ToArray());
            Assert.AreEqual(new[] { 5, 5, 5 }, dates.Select(x => x.Second).ToArray());
        }

        /// <summary>
        /// Tests numerics.
        /// </summary>
        [Test]
        public void TestNumerics()
        {
            var cache = Ignition.GetIgnite()
                    .GetOrCreateCache<int, Numerics>(new CacheConfiguration("numerics", typeof (Numerics)));

            for (var i = 0; i < 100; i++)
                cache[i] = new Numerics(((double) i - 50)/3);

            var query = cache.AsCacheQueryable().Select(x => x.Value);

            var bytes = query.Select(x => x.Byte);
            var sbytes = query.Select(x => x.Sbyte);
            var shorts = query.Select(x => x.Short);
            var ushorts = query.Select(x => x.Ushort);
            var ints = query.Select(x => x.Int);
            var uints = query.Select(x => x.Uint);
            var longs = query.Select(x => x.Long);
            var ulongs = query.Select(x => x.Ulong);
            var doubles = query.Select(x => x.Double);
            var decimals = query.Select(x => x.Decimal);
            var floats = query.Select(x => x.Float);

            CheckFunc(x => Math.Abs(x), doubles);
            CheckFunc(x => Math.Abs((sbyte) x), bytes);
            CheckFunc(x => Math.Abs(x), sbytes);
            CheckFunc(x => Math.Abs(x), shorts);
            CheckFunc(x => Math.Abs((short) x), ushorts);
            CheckFunc(x => Math.Abs(x), ints);
            CheckFunc(x => Math.Abs((int) x), uints);
            CheckFunc(x => Math.Abs(x), longs);
            CheckFunc(x => Math.Abs((long) x), ulongs);
            CheckFunc(x => Math.Abs(x), decimals);
            CheckFunc(x => Math.Abs(x), floats);

            CheckFunc(x => Math.Acos(x), doubles);
            CheckFunc(x => Math.Asin(x), doubles);
            CheckFunc(x => Math.Atan(x), doubles);
            CheckFunc(x => Math.Atan2(x, 0.5), doubles);

            CheckFunc(x => Math.Ceiling(x), doubles);
            CheckFunc(x => Math.Ceiling(x), decimals);

            CheckFunc(x => Math.Cos(x), doubles);
            CheckFunc(x => Math.Cosh(x), doubles);
            CheckFunc(x => Math.Exp(x), doubles);

            CheckFunc(x => Math.Floor(x), doubles);
            CheckFunc(x => Math.Floor(x), decimals);

            CheckFunc(x => Math.Log(x), doubles);
            CheckFunc(x => Math.Log10(x), doubles);

            CheckFunc(x => Math.Pow(x, 3.7), doubles);

            CheckFunc(x => Math.Round(x), doubles);
            CheckFunc(x => Math.Round(x, 3), doubles);
            CheckFunc(x => Math.Round(x), decimals);
            CheckFunc(x => Math.Round(x, 3), decimals);

            CheckFunc(x => Math.Sign(x), doubles);
            CheckFunc(x => Math.Sign(x), decimals);
            CheckFunc(x => Math.Sign(x), floats);
            CheckFunc(x => Math.Sign(x), ints);
            CheckFunc(x => Math.Sign(x), longs);
            CheckFunc(x => Math.Sign(x), shorts);
            CheckFunc(x => Math.Sign(x), sbytes);

            CheckFunc(x => Math.Sin(x), doubles);
            CheckFunc(x => Math.Sinh(x), doubles);
            CheckFunc(x => Math.Sqrt(x), doubles);
            CheckFunc(x => Math.Tan(x), doubles);
            CheckFunc(x => Math.Tanh(x), doubles);

            CheckFunc(x => Math.Truncate(x), doubles);
            CheckFunc(x => Math.Truncate(x), decimals);

            // Operators
            CheckFunc(x => x*7, doubles);
            CheckFunc(x => x/7, doubles);
            CheckFunc(x => x%7, doubles);
            CheckFunc(x => x+7, doubles);
            CheckFunc(x => x-7, doubles);
        }

        /// <summary>
        /// Tests aliases.
        /// </summary>
        [Test]
        public void TestAliases()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            var res = cache.Where(x => x.Key == 1)
                .Select(x => new {X = x.Value.AliasTest, Y = x.Value.Address.AliasTest})
                .Single();

            Assert.AreEqual(new {X = -1, Y = 1}, res);
        }

        /// <summary>
        /// Tests the compiled query with various constructs.
        /// </summary>
        [Test]
        public void TestCompiledQuery()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();

            // Embedded args
            var qry0 = CompiledQuery2.Compile(() => persons.Where(x => x.Key < 3 && x.Value.Name.Contains("son")));
            Assert.AreEqual(3, qry0().Count());

            // Lambda args
            var qry1 = CompiledQuery2.Compile((int minKey, int take, int skip) => persons.Where(x => x.Key > minKey)
                .Take(take).Skip(skip));
            Assert.AreEqual(3, qry1(-1, 3, 1).GetAll().Count);

            qry1 = CompiledQuery2.Compile((int skip, int take, int minKey) => persons.Where(x => x.Key > minKey)
                .Take(take).Skip(skip));

            Assert.AreEqual(5, qry1(2, 5, 20).GetAll().Count);

            // Mixed args
            var qry2 = CompiledQuery2.Compile((int maxKey, int minKey) =>
                persons.Where(x => x.Key < maxKey
                                   && x.Value.Name.Contains("er")
                                   && x.Value.Age < maxKey
                                   && x.Key > minKey).Skip(2));

            Assert.AreEqual(6, qry2(10, 1).Count());

            // Join
            var qry3 = CompiledQuery2.Compile(() =>
                roles.Join(persons, r => r.Key.Foo, p => p.Key, (r, p) => r.Value.Name));

            Assert.AreEqual(RoleCount, qry3().Count());

            // Join with subquery
            var qry4 = CompiledQuery2.Compile(
                (int a, int b, string sep) =>
                    roles
                        .Where(x => x.Key.Bar > a)
                        .Join(persons.Where(x => x.Key < b && x.Key > 0),
                            r => r.Key.Foo,
                            p => p.Value.Address.Zip,
                            (r, p) => p.Value.Name + sep + r.Value.Name + "|")
                        .Skip(a).Take(1000)
                );

            Assert.AreEqual(new[] { " Person_2  =Role_2|", " Person_3  =|"}, qry4(1, PersonCount, "=").ToArray());

            // Union
            var qry5 = CompiledQuery2.Compile(() => roles.Select(x => -x.Key.Foo).Union(persons.Select(x => x.Key)));

            Assert.AreEqual(RoleCount + PersonCount, qry5().Count());

            // Projection
            var qry6 = CompiledQuery2.Compile((int minAge) => persons
                .Select(x => x.Value)
                .Where(x => x.Age >= minAge)
                .Select(x => new {x.Name, x.Age})
                .OrderBy(x => x.Name));

            var res = qry6(PersonCount - 3).GetAll();

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(PersonCount - 3, res[0].Age);
        }

        /// <summary>
        /// Tests the compiled query overloads.
        /// </summary>
        [Test]
        public void TestCompiledQueryOverloads()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // const args are allowed
            Assert.AreEqual(5, CompiledQuery2.Compile(() => cache.Where(x => x.Key < 5))().GetAll().Count);

            // 0 arg
            var qry0 = CompiledQuery2.Compile(() => cache.Select(x => x.Value.Name));
            Assert.AreEqual(PersonCount, qry0().ToArray().Length);

            // 1 arg
            var qry1 = CompiledQuery2.Compile((int k) => cache.Where(x => x.Key < k));
            Assert.AreEqual(3, qry1(3).ToArray().Length);

            // 1 arg twice
            var qry1T = CompiledQuery2.Compile((int k) => cache.Where(x => x.Key < k && x.Value.Age < k));
            Assert.AreEqual(3, qry1T(3).ToArray().Length);

            // 2 arg
            var qry2 =
                CompiledQuery2.Compile((int i, string s) => cache.Where(x => x.Key < i && x.Value.Name.StartsWith(s)));
            Assert.AreEqual(5, qry2(5, " Pe").ToArray().Length);

            // Changed param order
            var qry2R =
                CompiledQuery2.Compile((string s, int i) => cache.Where(x => x.Key < i && x.Value.Name.StartsWith(s)));
            Assert.AreEqual(5, qry2R(" Pe", 5).ToArray().Length);

            // 3 arg
            var qry3 = CompiledQuery2.Compile((int i, string s, double d) =>
                cache.Where(x => x.Value.Address.Zip > d && x.Key < i && x.Value.Name.Contains(s)));
            Assert.AreEqual(5, qry3(5, "son", -10).ToArray().Length);

            // 4 arg
            var qry4 = CompiledQuery2.Compile((int a, int b, int c, int d) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d));
            Assert.AreEqual(new[] {3, 4}, qry4(0, 1, 2, 5).ToArray());

            // 5 arg
            var qry5 = CompiledQuery2.Compile((int a, int b, int c, int d, int e) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e));
            Assert.AreEqual(new[] {3, 4}, qry5(0, 1, 2, 5, 6).ToArray());

            // 6 arg
            var qry6 = CompiledQuery2.Compile((int a, int b, int c, int d, int e, int f) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e && k < f));
            Assert.AreEqual(new[] {3, 4}, qry6(0, 1, 2, 5, 6, 7).ToArray());

            // 7 arg
            var qry7 = CompiledQuery2.Compile((int a, int b, int c, int d, int e, int f, int g) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e && k < f && k < g));
            Assert.AreEqual(new[] {3, 4}, qry7(0, 1, 2, 5, 6, 7, 8).ToArray());

            // 8 arg
            var qry8 = CompiledQuery2.Compile((int a, int b, int c, int d, int e, int f, int g, int h) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e && k < f && k < g && k < h));
            Assert.AreEqual(new[] {3, 4}, qry8(0, 1, 2, 5, 6, 7, 8, 9).ToArray());
        }

        /// <summary>
        /// Tests the free-form compiled query, where user provides an array of arguments.
        /// </summary>
        [Test]
        public void TestCompiledQueryFreeform()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            var qry = cache.Where(x => x.Key < 5);

            // Simple
            var compiled = CompiledQuery2.Compile(qry);

            Assert.AreEqual(5, compiled(5).Count());
            Assert.AreEqual(6, compiled(6).Count());

            // Select
            var compiledSelect = CompiledQuery2.Compile(qry.Select(x => x.Value.Name).OrderBy(x => x));

            Assert.AreEqual(3, compiledSelect(3).Count());
            Assert.AreEqual(" Person_0  ", compiledSelect(1).Single());

            // Join
            var compiledJoin = CompiledQuery2.Compile(qry.Join(
                GetOrgCache().AsCacheQueryable().Where(x => x.Value.Name.StartsWith("Org")),
                p => p.Value.OrganizationId, o => o.Value.Id, (p, o) => o.Key));

            Assert.AreEqual(1000, compiledJoin("Org", 1).Single());

            // Many parameters
            var qry2 = cache.Where(x => x.Key < 3)
                .Where(x => x.Key > 0)
                .Where(x => x.Value.Name.Contains(""))
                .Where(x => x.Value.Address.Zip > 0)
                .Where(x => x.Value.Age == 7);

            var compiled2 = CompiledQuery2.Compile(qry2);

            Assert.AreEqual(17, compiled2(18, 16, "ers", 13, 17).Single().Key);
        }

#pragma warning disable 618  // obsolete class
        /// <summary>
        /// Tests the old, deprecated compiled query.
        /// </summary>
        [Test]
        public void TestCompiledQueryOld()
        {
            var cache = GetPersonCache().AsCacheQueryable();

            // const args are not allowed
            Assert.Throws<InvalidOperationException>(() => CompiledQuery.Compile(() => cache.Where(x => x.Key < 5)));

            // 0 arg
            var qry0 = CompiledQuery.Compile(() => cache.Select(x => x.Value.Name));
            Assert.AreEqual(PersonCount, qry0().ToArray().Length);

            // 1 arg
            var qry1 = CompiledQuery.Compile((int k) => cache.Where(x => x.Key < k));
            Assert.AreEqual(3, qry1(3).ToArray().Length);

            // 2 arg
            var qry2 =
                CompiledQuery.Compile((int i, string s) => cache.Where(x => x.Key < i && x.Value.Name.StartsWith(s)));
            Assert.AreEqual(5, qry2(5, " Pe").ToArray().Length);

            // Changed param order
            var qry2R =
                CompiledQuery.Compile((string s, int i) => cache.Where(x => x.Key < i && x.Value.Name.StartsWith(s)));
            Assert.AreEqual(5, qry2R(" Pe", 5).ToArray().Length);

            // 3 arg
            var qry3 = CompiledQuery.Compile((int i, string s, double d) =>
                cache.Where(x => x.Value.Address.Zip > d && x.Key < i && x.Value.Name.Contains(s)));
            Assert.AreEqual(5, qry3(5, "son", -10).ToArray().Length);

            // 4 arg
            var keys = cache.Select(x => x.Key);
            var qry4 = CompiledQuery.Compile((int a, int b, int c, int d) =>
                keys.Where(k => k > a && k > b && k > c && k < d));
            Assert.AreEqual(new[] { 3, 4 }, qry4(0, 1, 2, 5).ToArray());

            // 5 arg
            var qry5 = CompiledQuery.Compile((int a, int b, int c, int d, int e) =>
                keys.Where(k => k > a && k > b && k > c && k < d && k < e));
            Assert.AreEqual(new[] { 3, 4 }, qry5(0, 1, 2, 5, 6).ToArray());

            // 6 arg
            var qry6 = CompiledQuery.Compile((int a, int b, int c, int d, int e, int f) =>
                keys.Where(k => k > a && k > b && k > c && k < d && k < e && k < f));
            Assert.AreEqual(new[] { 3, 4 }, qry6(0, 1, 2, 5, 6, 7).ToArray());

            // 7 arg
            var qry7 = CompiledQuery.Compile((int a, int b, int c, int d, int e, int f, int g) =>
                keys.Where(k => k > a && k > b && k > c && k < d && k < e && k < f && k < g));
            Assert.AreEqual(new[] { 3, 4 }, qry7(0, 1, 2, 5, 6, 7, 8).ToArray());

            // 8 arg
            var qry8 = CompiledQuery.Compile((int a, int b, int c, int d, int e, int f, int g, int h) =>
                keys.Where(k => k > a && k > b && k > c && k < d && k < e && k < f && k < g && k < h));
            Assert.AreEqual(new[] { 3, 4 }, qry8(0, 1, 2, 5, 6, 7, 8, 9).ToArray());
        }
#pragma warning restore 618

        /// <summary>
        /// Tests the cache of primitive types.
        /// </summary>
        [Test]
        public void TestPrimitiveCache()
        {
            // Create partitioned cache
            var cache =
                Ignition.GetIgnite()
                    .GetOrCreateCache<int, string>(new CacheConfiguration("primitiveCache",
                        new QueryEntity(typeof (int), typeof (string))) {CacheMode = CacheMode.Replicated});

            var qry = cache.AsCacheQueryable();

            // Populate
            const int count = 100;
            cache.PutAll(Enumerable.Range(0, count).ToDictionary(x => x, x => x.ToString()));

            // Test
            Assert.AreEqual(count, qry.ToArray().Length);
            Assert.AreEqual(10, qry.Where(x => x.Key < 10).ToArray().Length);
            Assert.AreEqual(1, qry.Count(x => x.Value.Contains("99")));
        }

        /// <summary>
        /// Tests the local query.
        /// </summary>
        [Test]
        public void TestLocalQuery()
        {
            // Create partitioned cache
            var cache =
                Ignition.GetIgnite().GetOrCreateCache<int, int>(new CacheConfiguration("partCache", typeof (int)));

            // Populate
            const int count = 100;
            cache.PutAll(Enumerable.Range(0, count).ToDictionary(x => x, x => x));

            // Non-local query returns all records
            Assert.AreEqual(count, cache.AsCacheQueryable(false).ToArray().Length);

            // Local query returns only some of the records
            var localCount = cache.AsCacheQueryable(true).ToArray().Length;
            Assert.Less(localCount, count);
            Assert.Greater(localCount, 0);
        }

        /// <summary>
        /// Tests the introspection.
        /// </summary>
        [Test]
        public void TestIntrospection()
        {
            var cache = GetPersonCache();

            // Check regular query
            var query = (ICacheQueryable) cache.AsCacheQueryable(new QueryOptions
            {
                Local = true,
                PageSize = 999,
                EnforceJoinOrder = true
            }).Where(x => x.Key > 10);

            Assert.AreEqual(cache.Name, query.CacheName);
            Assert.AreEqual(cache.Ignite, query.Ignite);

            var fq = query.GetFieldsQuery();
            Assert.AreEqual("select _T0._key, _T0._val from \"\".Person as _T0 where (_T0._key > ?)", fq.Sql);
            Assert.AreEqual(new[] {10}, fq.Arguments);
            Assert.IsTrue(fq.Local);
            Assert.AreEqual(PersonCount - 11, cache.QueryFields(fq).GetAll().Count);
            Assert.AreEqual(999, fq.PageSize);
            Assert.IsFalse(fq.EnableDistributedJoins);
            Assert.IsTrue(fq.EnforceJoinOrder);

            var str = query.ToString();
            Assert.AreEqual("CacheQueryable [CacheName=, TableName=Person, Query=SqlFieldsQuery [Sql=select " +
                            "_T0._key, _T0._val from \"\".Person as _T0 where (_T0._key > ?), Arguments=[10], " +
                            "Local=True, PageSize=999, EnableDistributedJoins=False, EnforceJoinOrder=True]]", str);

            // Check fields query
            var fieldsQuery = (ICacheQueryable) cache.AsCacheQueryable().Select(x => x.Value.Name);

            Assert.AreEqual(cache.Name, fieldsQuery.CacheName);
            Assert.AreEqual(cache.Ignite, fieldsQuery.Ignite);

            fq = fieldsQuery.GetFieldsQuery();
            Assert.AreEqual("select _T0.Name from \"\".Person as _T0", fq.Sql);
            Assert.IsFalse(fq.Local);
            Assert.AreEqual(SqlFieldsQuery.DfltPageSize, fq.PageSize);
            Assert.IsFalse(fq.EnableDistributedJoins);
            Assert.IsFalse(fq.EnforceJoinOrder);

            str = fieldsQuery.ToString();
            Assert.AreEqual("CacheQueryable [CacheName=, TableName=Person, Query=SqlFieldsQuery [Sql=select " +
                            "_T0.Name from \"\".Person as _T0, Arguments=[], Local=False, PageSize=1024, " +
                            "EnableDistributedJoins=False, EnforceJoinOrder=False]]", str);
            
            // Check distributed joins flag propagation
            var distrQuery = cache.AsCacheQueryable(new QueryOptions {EnableDistributedJoins = true})
                .Where(x => x.Key > 10 && x.Value.Age > 20 && x.Value.Name.Contains("x"));

            query = (ICacheQueryable) distrQuery;

            Assert.IsTrue(query.GetFieldsQuery().EnableDistributedJoins);

            str = distrQuery.ToString();
            Assert.AreEqual("CacheQueryable [CacheName=, TableName=Person, Query=SqlFieldsQuery [Sql=select " +
                            "_T0._key, _T0._val from \"\".Person as _T0 where (((_T0._key > ?) and (_T0.age1 > ?)) " +
                            "and (_T0.Name like \'%\' || ? || \'%\') ), Arguments=[10, 20, x], Local=False, " +
                            "PageSize=1024, EnableDistributedJoins=True, EnforceJoinOrder=False]]", str);
        }

        /// <summary>
        /// Tests the table name inference.
        /// </summary>
        [Test]
        public void TestTableNameInference()
        {
            // Try with multi-type cache: explicit type is required
            var cache = GetCacheOf<IPerson>();

            Assert.Throws<CacheException>(() => cache.AsCacheQueryable());

            var names = cache.AsCacheQueryable(false, "Person").Select(x => x.Value.Name).ToArray();

            Assert.AreEqual(PersonCount, names.Length);

            // With single-type cache, interface inference works
            var roleCache = Ignition.GetIgnite().GetCache<object, IRole>(RoleCacheName).AsCacheQueryable();

            var roleNames = roleCache.Select(x => x.Value.Name).OrderBy(x => x).ToArray();

            CollectionAssert.AreEquivalent(new[] {"Role_1", "Role_2", null}, roleNames);

            // Check non-queryable cache
            var nonQueryableCache = Ignition.GetIgnite().GetOrCreateCache<Role, Person>("nonQueryable");

            Assert.Throws<CacheException>(() => nonQueryableCache.AsCacheQueryable());
        }

        /// <summary>
        /// Tests the distributed joins.
        /// </summary>
        [Test]
        public void TestDistributedJoins()
        {
            var ignite = Ignition.GetIgnite();

            // Create and populate partitioned caches
            var personCache = ignite.CreateCache<int, Person>(new CacheConfiguration("partitioned_persons",
                new QueryEntity(typeof(int), typeof(Person))));

            personCache.PutAll(GetSecondPersonCache().ToDictionary(x => x.Key, x => x.Value));

            var roleCache = ignite.CreateCache<int, Role>(new CacheConfiguration("partitioned_roles",
                    new QueryEntity(typeof(int), typeof(Role))));

            roleCache.PutAll(GetRoleCache().ToDictionary(x => x.Key.Foo, x => x.Value));

            // Test non-distributed join: returns partial results
            var persons = personCache.AsCacheQueryable();
            var roles = roleCache.AsCacheQueryable();

            var res = persons.Join(roles, person => person.Key - PersonCount, role => role.Key, (person, role) => role)
                .ToArray();

            Assert.Greater(res.Length, 0);
            Assert.Less(res.Length, RoleCount);

            // Test distributed join: returns complete results
            persons = personCache.AsCacheQueryable(new QueryOptions {EnableDistributedJoins = true});
            roles = roleCache.AsCacheQueryable(new QueryOptions {EnableDistributedJoins = true});

            res = persons.Join(roles, person => person.Key - PersonCount, role => role.Key, (person, role) => role)
                .ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        /// <summary>
        /// Gets the person cache.
        /// </summary>
        /// <returns></returns>
        private static ICache<int, Person> GetPersonCache()
        {
            return GetCacheOf<Person>();
        }

        /// <summary>
        /// Gets the org cache.
        /// </summary>
        /// <returns></returns>
        private static ICache<int, Organization> GetOrgCache()
        {
            return GetCacheOf<Organization>();
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private static ICache<int, T> GetCacheOf<T>()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, T>(new CacheConfiguration(PersonOrgCacheName,
                    new QueryEntity(typeof (int), typeof (Person))
                    {
                        Aliases = new[]
                        {
                            new QueryAlias("AliasTest", "Person_AliasTest"),
                            new QueryAlias("Address.AliasTest", "Addr_AliasTest")
                        }
                    },
                    new QueryEntity(typeof (int), typeof (Organization))) {CacheMode = CacheMode.Replicated});
        }

        /// <summary>
        /// Gets the role cache.
        /// </summary>
        private static ICache<RoleKey, Role> GetRoleCache()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<RoleKey, Role>(new CacheConfiguration(RoleCacheName,
                    new QueryEntity(typeof (RoleKey), typeof (Role))) {CacheMode = CacheMode.Replicated});
        }

        /// <summary>
        /// Gets the second person cache.
        /// </summary>
        private static ICache<int, Person> GetSecondPersonCache()
        {
            return Ignition.GetIgnite()
                .GetOrCreateCache<int, Person>(new CacheConfiguration(PersonSecondCacheName,
                    new QueryEntity(typeof (int), typeof (Person))) {CacheMode = CacheMode.Replicated});
        }

        /// <summary>
        /// Checks that function maps to SQL function properly.
        /// </summary>
        private static void CheckFunc<T, TR>(Expression<Func<T, TR>> exp, IQueryable<T> query, 
            Func<TR, TR> localResultFunc = null)
        {
            localResultFunc = localResultFunc ?? (x => x);

            // Calculate result locally, using real method invocation
            var expected = query.ToArray().AsQueryable().Select(exp).Select(localResultFunc).OrderBy(x => x).ToArray();

            // Perform SQL query
            var actual = query.Select(exp).ToArray().OrderBy(x => x).ToArray();

            // Compare results
            CollectionAssert.AreEqual(expected, actual, new NumericComparer());

            // Perform intermediate anonymous type conversion to check type projection
            actual = query.Select(exp).Select(x => new {Foo = x}).ToArray().Select(x => x.Foo)
                .OrderBy(x => x).ToArray();

            // Compare results
            CollectionAssert.AreEqual(expected, actual, new NumericComparer());
        }

        /// <summary>
        /// Checks that function used in Where Clause maps to SQL function properly
        /// </summary>
        private static void CheckWhereFunc<TKey, TEntry>(IQueryable<ICacheEntry<TKey,TEntry>> query, Expression<Func<ICacheEntry<TKey, TEntry>,bool>> whereExpression)
        {
            // Calculate result locally, using real method invocation
            var expected = query
                .ToArray()
                .AsQueryable()
                .Where(whereExpression)
                .Select(entry => entry.Key)
                .OrderBy(x => x)
                .ToArray();

            // Perform SQL query
            var actual = query
                .Where(whereExpression)
                .Select(entry => entry.Key)
                .ToArray()
                .OrderBy(x => x)
                .ToArray();

            // Compare results
            CollectionAssert.AreEqual(expected, actual, new NumericComparer());
        }

        public interface IPerson
        {
            int Age { get; set; }
            string Name { get; set; }
        }

        public class Person : IBinarizable, IPerson
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

            [QuerySqlField] public DateTime? Birthday { get; set; }

            [QuerySqlField] public int AliasTest { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("age1", Age);
                writer.WriteString("name", Name);
                writer.WriteInt("OrganizationId", OrganizationId);
                writer.WriteObject("Address", Address);
                writer.WriteTimestamp("Birthday", Birthday);
                writer.WriteInt("AliasTest", AliasTest);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Age = reader.ReadInt("age1");
                Name = reader.ReadString("name");
                OrganizationId = reader.ReadInt("OrganizationId");
                Address = reader.ReadObject<Address>("Address");
                Birthday = reader.ReadTimestamp("Birthday");
                AliasTest = reader.ReadInt("AliasTest");
            }
        }

        public class Address
        {
            [QuerySqlField] public int Zip { get; set; }
            [QuerySqlField] public string Street { get; set; }
            [QuerySqlField] public int AliasTest { get; set; }
        }

        public class Organization
        {
            [QuerySqlField] public int Id { get; set; }
            [QuerySqlField] public string Name { get; set; }
        }

        public interface IRole
        {
            string Name { get; }
            DateTime Date { get; }
        }

        public class Role : IRole
        {
            [QuerySqlField] public string Name { get; set; }
            [QuerySqlField] public DateTime Date { get; set; }
        }

        public class Numerics
        {
            public Numerics(double val)
            {
                Double = val;
                Float = (float) val;
                Decimal = (decimal) val;
                Int = (int) val;
                Uint = (uint) val;
                Long = (long) val;
                Ulong = (ulong) val;
                Short = (short) val;
                Ushort = (ushort) val;
                Byte = (byte) val;
                Sbyte =  (sbyte) val;
            }

            [QuerySqlField] public double Double { get; set; }
            [QuerySqlField] public float Float { get; set; }
            [QuerySqlField] public decimal Decimal { get; set; }
            [QuerySqlField] public int Int { get; set; }
            [QuerySqlField] public uint Uint { get; set; }
            [QuerySqlField] public long Long { get; set; }
            [QuerySqlField] public ulong Ulong { get; set; }
            [QuerySqlField] public short Short { get; set; }
            [QuerySqlField] public ushort Ushort { get; set; }
            [QuerySqlField] public byte Byte { get; set; }
            [QuerySqlField] public sbyte Sbyte { get; set; }
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

        /// <summary>
        /// Epsilon comparer.
        /// </summary>
        private class NumericComparer : IComparer
        {
            /** <inheritdoc /> */
            public int Compare(object x, object y)
            {
                if (Equals(x, y))
                    return 0;

                if (x is double)
                {
                    var dx = (double) x;
                    var dy = (double) y;

                    // Epsilon is proportional to the min value, but not too small.
                    const double epsilon = 2E-10d;
                    var min = Math.Min(Math.Abs(dx), Math.Abs(dy));
                    var relEpsilon = Math.Max(min*epsilon, epsilon);

                    // Compare with epsilon because some funcs return slightly different results.
                    return Math.Abs((double) x - (double) y) < relEpsilon ? 0 : 1;
                }

                return ((IComparable) x).CompareTo(y);
            }
        }
    }
}

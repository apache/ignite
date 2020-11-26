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
namespace Apache.Ignite.Core.Tests.Cache.Query.Linq
{
    using System;
    using System.Linq;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
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
                    (person, org) => new { Person = person.Value, Org = org.Value })
                .Where(x => x.Org.Name == "Org_1")
                .ToList();

            Assert.AreEqual(PersonCount / 2, res.Count);

            Assert.IsTrue(res.All(r => r.Person.OrganizationId == r.Org.Id));

            // Test full projection (selects pair of ICacheEntry)
            var res2 = persons.Join(organizations, person => person.Value.OrganizationId - 1, org => org.Value.Id - 1,
                    (person, org) => new { Person = person, Org = org })
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
        /// Tests the multi key join inline
        /// </summary>
        [Test]
        public void TestMultiKeyJoinInline()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var multiKey = persons.Where(p => p.Key == 1).Join(organizations,
                p => new { OrgId = p.Value.OrganizationId, p.Key },
                o => new { OrgId = o.Value.Id, Key = o.Key - 1000 },
                (p, o) => new { PersonName = p.Value.Name, OrgName = o.Value.Name });

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
                .OrderBy(x => x.Key.Bar)
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
                    person => person.Key, role => role.Key.Foo, (person, role) => role)
                .OrderBy(x => x.Key.Bar).ToArray();

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
                    (role, person) => new { person, role })
                .Join(organizations, pr => pr.person.Value.OrganizationId, org => org.Value.Id,
                    (pr, org) => new { org, pr.person, pr.role }).ToArray();

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
                    (role, person) => new { person, role })
                .Join(organizations, pr => pr.person.Value.OrganizationId, org => org.Value.Id,
                    (pr, org) => new { org, pr.person, pr.role }).ToArray();

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

            var qry1 = persons.Join(orgs,
                    p => p.Value.OrganizationId,
                    o => o.Value.Id,
                    (p, o) => p)
                .Where(x => x.Key >= 0)
                .ToList();

            Assert.AreEqual(PersonCount, qry1.Count);

            // With selector inline
            var qry2 = persons
                .Join(orgs.Select(orgEntry => orgEntry.Key),
                    e => e.Value.OrganizationId,
                    i => i,
                    (e, i) => e)
                .ToList();

            Assert.AreEqual(PersonCount, qry2.Count);

            // With selector from variable
            var innerSequence = orgs
                .Select(orgEntry => orgEntry.Key);

            var qry3 = persons
                .Join(innerSequence,
                    e => e.Value.OrganizationId,
                    i => i,
                    (e, i) => e)
                .ToList();

            Assert.AreEqual(PersonCount, qry3.Count);
        }

        /// <summary>
        /// Tests the invalid join.
        /// </summary>
        [Test]
        public void TestInvalidJoin()
        {
            var localComplexTypeCollection = GetOrgCache().AsCacheQueryable()
                .Select(e => e.Value)
                .ToArray();

            // Join on non-IQueryable with complex(not supported) type
            var ex = Assert.Throws<NotSupportedException>(() =>
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            {
                GetPersonCache().AsCacheQueryable().Join(localComplexTypeCollection, p => p.Value.OrganizationId,
                    o => o.Id, (p, o) => p).ToList();
            });

            Assert.IsTrue(ex.Message.StartsWith("Not supported item type for Join with local collection"));
        }

        /// <summary>
        /// Tests query with multiple from clause.
        /// </summary>
        [Test]
        public void TestMultipleFrom()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();
            var organizations = GetOrgCache().AsCacheQueryable();

            var filtered =
                from person in persons
                from role in roles
                from org in organizations
                where person.Key == role.Key.Foo && person.Value.OrganizationId == org.Value.Id
                select new
                {
                    PersonKey = person.Key,
                    Person = person.Value.Name,
                    RoleFoo = role.Key.Foo,
                    Role = role.Value.Name,
                    Org = org.Value.Name
                };

            var res = filtered.ToArray();

            Assert.AreEqual(RoleCount, res.Length);
        }

        /// <summary>
        /// Tests query with two from clause.
        /// </summary>
        [Test]
        public void TestTwoFromSubquery()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();
            var personsSubquery = persons.Where(x => x.Key < PersonCount);
            var rolesSubquery = roles.Where(x => x.Value.Name == "Role_2");

            var filtered =
                from person in persons
                from role in rolesSubquery
                where person.Key == role.Key.Foo
                select new
                {
                    PersonKey = person.Key, Person = person.Value.Name,
                    RoleFoo = role.Key.Foo, Role = role.Value.Name
                };

            var res = filtered.ToArray();
            Assert.AreEqual(1, res.Length);

            filtered =
                from person in personsSubquery
                from role in rolesSubquery
                where person.Key == role.Key.Foo
                select new
                {
                    PersonKey = person.Key, Person = person.Value.Name,
                    RoleFoo = role.Key.Foo, Role = role.Value.Name
                };

            res = filtered.ToArray();
            Assert.AreEqual(1, res.Length);

            filtered =
                from person in personsSubquery
                from role in roles
                where person.Key == role.Key.Foo
                select new
                {
                    PersonKey = person.Key, Person = person.Value.Name,
                    RoleFoo = role.Key.Foo, Role = role.Value.Name
                };

            res = filtered.ToArray();
            Assert.AreEqual(RoleCount, res.Length);
        }


        /// <summary>
        /// Tests query with two from clause.
        /// </summary>
        [Test]
        public void TestMultipleFromSubquery()
        {
            var organizations = GetOrgCache().AsCacheQueryable().Where(x => x.Key == 1001);
            var persons = GetPersonCache().AsCacheQueryable().Where(x => x.Key < 20);
            var roles = GetRoleCache().AsCacheQueryable().Where(x => x.Key.Foo >= 0);

            var filtered =
                from person in persons
                from role in roles
                from org in organizations
                where person.Key == role.Key.Foo && person.Value.OrganizationId == org.Value.Id
                select new
                {
                    PersonKey = person.Key,
                    Person = person.Value.Name,
                    RoleFoo = role.Key.Foo,
                    Role = role.Value.Name,
                    Org = org.Value.Name
                };

            var res = filtered.ToArray();
            Assert.AreEqual(2, res.Length);
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
    }
}
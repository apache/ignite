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

namespace Apache.Ignite.Core.Tests.Cache.Query.Linq
{
    using System.Linq;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// GroupBy tests.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests grouping.
        /// </summary>
        [Test]
        public void TestGroupBy()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var orgs = GetOrgCache().AsCacheQueryable();

            // Simple, unordered
            CollectionAssert.AreEquivalent(new[] { 1000, 1001 },
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
                select new { Count = gs.Count(), OrgId = gs.Key, AvgAge = gs.Average(x => x.Value.Age) };

            var resArr = res1.ToArray();

            Assert.AreEqual(new[]
            {
                new {Count = PersonCount/2, OrgId = 1000, AvgAge = (double) PersonCount/2 - 1},
                new {Count = PersonCount/2, OrgId = 1001, AvgAge = (double) PersonCount/2}
            }, resArr);

            // Join and sum
            var res2 = persons.Join(orgs.Where(o => o.Key > 10), p => p.Value.OrganizationId, o => o.Key,
                    (p, o) => new { p, o })
                .GroupBy(x => x.o.Value.Name)
                .Select(g => new { Org = g.Key, AgeSum = g.Select(x => x.p.Value.Age).Sum() });

            var resArr2 = res2.ToArray();

            Assert.AreEqual(new[]
            {
                new {Org = "Org_0", AgeSum = persons.Where(x => x.Value.OrganizationId == 1000).Sum(x => x.Value.Age)},
                new {Org = "Org_1", AgeSum = persons.Where(x => x.Value.OrganizationId == 1001).Sum(x => x.Value.Age)}
            }, resArr2);
        }

        /// <summary>
        /// Tests the GroupBy with Where sub-query.
        /// </summary>
        [Test]
        public void TestGroupBySubQuery()
        {
            var persons = GetPersonCache().AsCacheQueryable().Where(p => p.Value.OrganizationId == 1000);
            var orgs = GetOrgCache().AsCacheQueryable();

            // Simple, unordered
            CollectionAssert.AreEquivalent(new[] { 1000 },
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
                select new { Count = gs.Count(), OrgId = gs.Key, AvgAge = gs.Average(x => x.Value.Age) };

            var resArr = res1.ToArray();

            Assert.AreEqual(new[]
            {
                new {Count = PersonCount/2, OrgId = 1000, AvgAge = (double) PersonCount/2 - 1},
            }, resArr);

            // Join and sum
            var res2 = persons.Join(orgs.Where(o => o.Key > 10), p => p.Value.OrganizationId, o => o.Key,
                    (p, o) => new { p, o })
                .GroupBy(x => x.o.Value.Name)
                .Select(g => new { Org = g.Key, AgeSum = g.Select(x => x.p.Value.Age).Sum() });

            var resArr2 = res2.ToArray();

            Assert.AreEqual(new[]
            {
                new {Org = "Org_0", AgeSum = persons.Where(x => x.Value.OrganizationId == 1000).Sum(x => x.Value.Age)},
            }, resArr2);
        }

        /// <summary>
        /// Tests grouping combined with join.
        /// </summary>
        [Test]
        public void TestGroupByWithJoin()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var qry = persons.Join(
                    organizations,
                    p => p.Value.OrganizationId,
                    o => o.Value.Id,
                    (person, org) => new {person, org})
                .GroupBy(x => x.person.Value.OrganizationId)
                .Select(g => new {OrgId = g.Key, MaxAge = g.Max(x => x.person.Value.Age)})
                .OrderBy(x => x.MaxAge);

            var res = qry.ToArray();

            Assert.AreEqual(2, res.Length);

            Assert.AreEqual(1000, res[0].OrgId);
            Assert.AreEqual(898, res[0].MaxAge);

            Assert.AreEqual(1001, res[1].OrgId);
            Assert.AreEqual(899, res[1].MaxAge);
        }

        /// <summary>
        /// Tests grouping combined with join in a reverse order.
        /// </summary>
        [Test]
        public void TestGroupByWithReverseJoin()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var qry = organizations.Join(
                    persons,
                    o => o.Value.Id,
                    p => p.Value.OrganizationId,
                    (org, person) => new {person, org})
                .GroupBy(x => x.person.Value.OrganizationId)
                .Select(g =>
                    new {OrgId = g.Key, MaxAge = g.Max(x => x.person.Value.Age)})
                .OrderBy(x => x.MaxAge);

            var res = qry.ToArray();

            Assert.AreEqual(2, res.Length);

            Assert.AreEqual(1000, res[0].OrgId);
            Assert.AreEqual(898, res[0].MaxAge);

            Assert.AreEqual(1001, res[1].OrgId);
            Assert.AreEqual(899, res[1].MaxAge);
        }
        
        /// <summary>
        /// Tests grouping combined with join in a reverse order followed by a projection to an anonymous type.
        /// </summary>
        [Test]
        public void TestGroupByWithReverseJoinAndProjection()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var qry = organizations.Join(
                    persons,
                    o => o.Value.Id,
                    p => p.Value.OrganizationId,
                    (org, person) => new
                    {
                        OrgName = org.Value.Name,
                        person.Value.Age
                    })
                .GroupBy(x => x.OrgName)
                .Select(g => new {OrgId = g.Key, MaxAge = g.Max(x => x.Age)})
                .OrderBy(x => x.MaxAge);

            var res = qry.ToArray();

            Assert.AreEqual(2, res.Length);

            Assert.AreEqual("Org_0", res[0].OrgId);
            Assert.AreEqual(898, res[0].MaxAge);

            Assert.AreEqual("Org_1", res[1].OrgId);
            Assert.AreEqual(899, res[1].MaxAge);
        }
        
        /// <summary>
        /// Tests grouping combined with join in a reverse order followed by a projection to an anonymous type with
        /// custom projected column names.
        /// </summary>
        [Test]
        public void TestGroupByWithReverseJoinAndProjectionWithRename()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var qry = organizations.Join(
                    persons,
                    o => o.Value.Id,
                    p => p.Value.OrganizationId,
                    (org, person) => new Projection
                    {
                        OrgName = org.Value.Name,
                        PersonAge = person.Value.Age
                    })
                .GroupBy(x => x.OrgName)
                .Select(g => new {OrgId = g.Key, MaxAge = g.Max(x => x.PersonAge)})
                .OrderBy(x => x.MaxAge);

            var res = qry.ToArray();

            Assert.AreEqual(2, res.Length);

            Assert.AreEqual("Org_0", res[0].OrgId);
            Assert.AreEqual(898, res[0].MaxAge);

            Assert.AreEqual("Org_1", res[1].OrgId);
            Assert.AreEqual(899, res[1].MaxAge);
        }
        
        /// <summary>
        /// Tests grouping combined with join in a reverse order followed by a projection to an anonymous type with
        /// custom projected column names.
        /// </summary>
        [Test]
        public void TestGroupByWithReverseJoinAndAnonymousProjectionWithRename()
        {
            var organizations = GetOrgCache().AsCacheQueryable();
            var persons = GetPersonCache().AsCacheQueryable();

            var qry = organizations.Join(
                    persons,
                    o => o.Value.Id,
                    p => p.Value.OrganizationId,
                    (org, person) => new
                    {
                        OrgName = org.Value.Name,
                        PersonAge = person.Value.Age
                    })
                .GroupBy(x => x.OrgName)
                .Select(g => new {OrgId = g.Key, MaxAge = g.Max(x => x.PersonAge)})
                .OrderBy(x => x.MaxAge);

            var res = qry.ToArray();

            Assert.AreEqual(2, res.Length);

            Assert.AreEqual("Org_0", res[0].OrgId);
            Assert.AreEqual(898, res[0].MaxAge);

            Assert.AreEqual("Org_1", res[1].OrgId);
            Assert.AreEqual(899, res[1].MaxAge);
        }
        
        private class Projection
        {
            public int PersonAge { get; set; }
            
            public string OrgName { get; set; }
        }
    }
}

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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests the compiled query with various constructs.
        /// </summary>
        [Test]
        public void TestCompiledQuery()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var roles = GetRoleCache().AsCacheQueryable();

            // Embedded args
            var qry0 = CompiledQuery.Compile(() => persons.Where(x => x.Key < 3 && x.Value.Name.Contains("son")));
            Assert.AreEqual(3, qry0().Count());

            // Lambda args
            var qry1 = CompiledQuery.Compile((int minKey, int take, int skip) => persons.Where(x => x.Key > minKey)
                .Take(take).Skip(skip));
            Assert.AreEqual(3, qry1(-1, 3, 1).GetAll().Count);

            qry1 = CompiledQuery.Compile((int skip, int take, int minKey) => persons.Where(x => x.Key > minKey)
                .Take(take).Skip(skip));

            Assert.AreEqual(5, qry1(2, 5, 20).GetAll().Count);

            // Mixed args
            var qry2 = CompiledQuery.Compile((int maxKey, int minKey) =>
                persons.Where(x => x.Key < maxKey
                                   && x.Value.Name.Contains("er")
                                   && x.Value.Age < maxKey
                                   && x.Key > minKey).Skip(2));

            Assert.AreEqual(6, qry2(10, 1).Count());

            // Join
            var qry3 = CompiledQuery.Compile(() =>
                roles.Join(persons, r => r.Key.Foo, p => p.Key, (r, p) => r.Value.Name));

            Assert.AreEqual(RoleCount, qry3().Count());

            // Join with subquery
            var qry4 = CompiledQuery.Compile(
                (int a, int b, string sep) =>
                    roles
                        .Where(x => x.Key.Bar > a)
                        .OrderBy(x => x.Key.Bar)
                        .Join(persons.Where(x => x.Key < b && x.Key > 0),
                            r => r.Key.Foo,
                            p => p.Value.Address.Zip,
                            (r, p) => p.Value.Name + sep + r.Value.Name + "|")
                        .Skip(a).Take(1000)
            );

            Assert.AreEqual(new[] { " Person_2  =Role_2|", " Person_3  =|" }, qry4(1, PersonCount, "=").ToArray());

            // Union
            var qry5 = CompiledQuery.Compile(() => roles.Select(x => -x.Key.Foo).Union(persons.Select(x => x.Key)));

            Assert.AreEqual(RoleCount + PersonCount, qry5().Count());

            // Projection
            var qry6 = CompiledQuery.Compile((int minAge) => persons
                .Select(x => x.Value)
                .Where(x => x.Age >= minAge)
                .Select(x => new { x.Name, x.Age })
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
            Assert.AreEqual(5, CompiledQuery.Compile(() => cache.Where(x => x.Key < 5))().GetAll().Count);

            // 0 arg
            var qry0 = CompiledQuery.Compile(() => cache.Select(x => x.Value.Name));
            Assert.AreEqual(PersonCount, qry0().ToArray().Length);

            // 1 arg
            var qry1 = CompiledQuery.Compile((int k) => cache.Where(x => x.Key < k));
            Assert.AreEqual(3, qry1(3).ToArray().Length);

            // 1 arg twice
            var qry1T = CompiledQuery.Compile((int k) => cache.Where(x => x.Key < k && x.Value.Age < k));
            Assert.AreEqual(3, qry1T(3).ToArray().Length);

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
            var qry4 = CompiledQuery.Compile((int a, int b, int c, int d) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d));
            Assert.AreEqual(new[] { 3, 4 }, qry4(0, 1, 2, 5).ToArray());

            // 5 arg
            var qry5 = CompiledQuery.Compile((int a, int b, int c, int d, int e) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e));
            Assert.AreEqual(new[] { 3, 4 }, qry5(0, 1, 2, 5, 6).ToArray());

            // 6 arg
            var qry6 = CompiledQuery.Compile((int a, int b, int c, int d, int e, int f) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e && k < f));
            Assert.AreEqual(new[] { 3, 4 }, qry6(0, 1, 2, 5, 6, 7).ToArray());

            // 7 arg
            var qry7 = CompiledQuery.Compile((int a, int b, int c, int d, int e, int f, int g) =>
                cache.Select(x => x.Key).Where(k => k > a && k > b && k > c && k < d && k < e && k < f && k < g));
            Assert.AreEqual(new[] { 3, 4 }, qry7(0, 1, 2, 5, 6, 7, 8).ToArray());

            // 8 arg
            var qry8 = CompiledQuery.Compile((int a, int b, int c, int d, int e, int f, int g, int h) =>
                cache.Select(x => x.Key)
                    .Where(k => k > a && k > b && k > c && k < d && k < e && k < f && k < g && k < h));
            Assert.AreEqual(new[] { 3, 4 }, qry8(0, 1, 2, 5, 6, 7, 8, 9).ToArray());
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
            var compiled = CompiledQuery.Compile(qry);

            Assert.AreEqual(5, compiled(5).Count());
            Assert.AreEqual(6, compiled(6).Count());

            // Select
            var compiledSelect = CompiledQuery.Compile(qry.Select(x => x.Value.Name).OrderBy(x => x));

            Assert.AreEqual(3, compiledSelect(3).Count());
            Assert.AreEqual(" Person_0  ", compiledSelect(1).Single());

            // Join
            var compiledJoin = CompiledQuery.Compile(qry.Join(
                GetOrgCache().AsCacheQueryable().Where(x => x.Value.Name.StartsWith("Org")),
                p => p.Value.OrganizationId, o => o.Value.Id, (p, o) => o.Key));

            Assert.AreEqual(1000, compiledJoin("Org", 1).Single());

            // Many parameters
            var qry2 = cache.Where(x => x.Key < 3)
                .Where(x => x.Key > 0)
                .Where(x => x.Value.Name.Contains(""))
                .Where(x => x.Value.Address.Zip > 0)
                .Where(x => x.Value.Age == 7);

            var compiled2 = CompiledQuery.Compile(qry2);

            Assert.AreEqual(17, compiled2(18, 16, "ers", 13, 17).Single().Key);
        }

        [Test]
        public void TestCompiledQueryStringEquals()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var qry0 = CompiledQuery.Compile((string empName) => persons.Where(x => x.Value.Name == empName));
            Assert.AreEqual(1, qry0(" Person_1  ").Count());
            Assert.AreEqual(0, qry0(null).Count());
        }

        [Test]
        public void TestCompiledQueryStringNotEquals()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var qry0 = CompiledQuery.Compile((string empName) => persons.Where(x => x.Value.Name != empName));
            Assert.AreEqual(PersonCount - 1, qry0(" Person_1  ").Count());
            Assert.AreEqual(PersonCount, qry0(null).Count());
        }

        [Test]
        public void TestCompiledQueryStringEqualsNull()
        {
            var roles = GetRoleCache().AsCacheQueryable();

            var nullNameRoles = CompiledQuery.Compile((string roleName) => roles.Where(x => x.Value.Name == roleName));
            Assert.AreEqual(1, nullNameRoles(null).Count());
        }

        [Test]
        public void TestCompiledQueryStringNotEqualsNull()
        {
            var roles = GetRoleCache().AsCacheQueryable();

            var nonNullNameRoles = CompiledQuery.Compile((string roleName) => roles.Where(x => x.Value.Name != roleName));
            Assert.AreEqual(RoleCount - 1, nonNullNameRoles(null).Count());
        }

        [Test]
        public void TestCompiledQueryStringEqualsFreeForm()
        {
            var persons = GetPersonCache().AsCacheQueryable().Where(emp => emp.Value.Name == "unused");
            var compiledQuery = CompiledQuery.Compile(persons);

            Func<string, IQueryCursor<ICacheEntry<int, Person>>> parametrizedCompiledQuery =
                empName => compiledQuery(empName);

            Assert.AreEqual(1, parametrizedCompiledQuery(" Person_0  ").Count());
        }

        [Test]
        public void TestCompiledQueryStringNotEqualsFreeForm()
        {
            var persons = GetPersonCache().AsCacheQueryable().Where(emp => emp.Value.Name != "unused");
            var compiledQuery = CompiledQuery.Compile(persons);

            Func<string, IQueryCursor<ICacheEntry<int, Person>>> parametrizedCompiledQuery =
                empName => compiledQuery(empName);

            Assert.AreEqual(PersonCount - 1, parametrizedCompiledQuery(" Person_0  ").Count());
        }
    }
}

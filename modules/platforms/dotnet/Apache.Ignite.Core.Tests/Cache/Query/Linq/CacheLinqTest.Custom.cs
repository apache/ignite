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
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests the RemoveAll extension.
        /// </summary>
        [Test]
        public void TestRemoveAll()
        {
            // Use new cache to avoid touching static data.
            var cache = Ignition.GetIgnite().CreateCache<int, Person>(new CacheConfiguration("deleteAllTest",
                new QueryEntity(typeof(int), typeof(Person)))
            {
                SqlEscapeAll = GetSqlEscapeAll()
            });

            Enumerable.Range(1, 10).ToList().ForEach(x => cache.Put(x, new Person(x, x.ToString())));

            var queryable = cache.AsCacheQueryable();

            Func<int[]> getKeys = () => cache.Select(x => x.Key).OrderBy(x => x).ToArray();

            // Without predicate.
            var res = queryable.Where(x => x.Key < 3).RemoveAll();
            Assert.AreEqual(2, res);
            Assert.AreEqual(Enumerable.Range(3, 8), getKeys());

            // With predicate.
            res = queryable.RemoveAll(x => x.Key < 7);
            Assert.AreEqual(4, res);
            Assert.AreEqual(Enumerable.Range(7, 4), getKeys());

            // Subquery-style join.
            var ids = GetPersonCache().AsCacheQueryable().Where(x => x.Key == 7).Select(x => x.Key);

            res = queryable.Where(x => ids.Contains(x.Key)).RemoveAll();
            Assert.AreEqual(1, res);
            Assert.AreEqual(Enumerable.Range(8, 3), getKeys());

            // Row number limit.
            res = queryable.Take(2).RemoveAll();
            Assert.AreEqual(2, res);
            Assert.AreEqual(1, getKeys().Length);

            // Unconditional.
            queryable.RemoveAll();
            Assert.AreEqual(0, cache.GetSize());

            // Skip is not supported with DELETE.
            var nex = Assert.Throws<NotSupportedException>(() => queryable.Skip(1).RemoveAll());
            Assert.AreEqual(
                "RemoveAll can not be combined with result operators (other than Take): SkipResultOperator",
                nex.Message);

            // Multiple result operators are not supported with DELETE.
            nex = Assert.Throws<NotSupportedException>(() => queryable.Skip(1).Take(1).RemoveAll());
            Assert.AreEqual(
                "RemoveAll can not be combined with result operators (other than Take): SkipResultOperator, " +
                "TakeResultOperator, RemoveAllResultOperator", nex.Message);

            // Joins are not supported in H2.
            var qry = queryable
                .Where(x => x.Key == 7)
                .Join(GetPersonCache().AsCacheQueryable(), p => p.Key, p => p.Key, (p1, p2) => p1);

            var ex = Assert.Throws<IgniteException>(() => qry.RemoveAll());
            Assert.AreEqual("Failed to parse query", ex.Message.Substring(0, 21));
        }

        /// <summary>
        /// Tests the UpdateAll extension.
        /// </summary>
        [Test]
        public void TestUpdateAll()
        {
            var orgQueryable = GetOrgCache().AsCacheQueryable();
            var allOrgs = orgQueryable.ToArray();

            // Use new cache to avoid touching static data.
            var personCount = 10;

            var cache = Ignition.GetIgnite().GetOrCreateCache<int, Person>(new CacheConfiguration("updateAllTest",
                new QueryEntity(typeof(int), typeof(Person)), new QueryEntity(typeof(int), typeof(Organization)))
            {
                SqlEscapeAll = GetSqlEscapeAll(),
                CacheMode = CacheMode.Replicated
            });

            Enumerable.Range(1, personCount).ToList().ForEach(x =>
                cache.Put(x, new Person(x, x.ToString()) {OrganizationId = 1000 + x % allOrgs.Length}));

            var personQueryable = cache.AsCacheQueryable();

            Action<Func<ICacheEntry<int, Person>, bool>> assertAll = func =>
                Assert.IsTrue(personQueryable.ToArray().All(func));

            Action<int, Func<ICacheEntry<int, Person>, bool>> assertAllCount = (i, func) =>
            {
                Assert.AreEqual(personCount, i);
                assertAll(func);
            };

            // ***
            // Unconditional
            // ***

            // Constant value
            var updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, 7));
            assertAllCount(updated, e => e.Value.AliasTest == 7);

            // Expression value - from self
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, e => e.Key));
            assertAllCount(updated, e => e.Value.AliasTest == e.Key);

            // Multiple sets
            var aliasValue = 3;
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, aliasValue).Set(p => p.Name, aliasValue.ToString()));
            assertAllCount(updated, e => e.Value.AliasTest == aliasValue && e.Value.Name == aliasValue.ToString());

            // Expression value - subquery with same cache
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest,
                    e => personQueryable.Where(ie => ie.Key == e.Key).Select(ie => ie.Key).First()));
            assertAllCount(updated, e => e.Value.AliasTest == e.Key);

            // Expression value - subquery with other cache
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, e => orgQueryable.Count(o => o.Key > e.Key)));
            assertAllCount(updated, e => e.Value.AliasTest == allOrgs.Count(o => o.Key > e.Key));

            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.Name,
                    e => orgQueryable.Where(o => o.Key == e.Value.OrganizationId).Select(o => o.Value.Name).First()));
            assertAllCount(updated,
                e => e.Value.Name == allOrgs.Where(o => o.Key == e.Value.OrganizationId).Select(o => o.Value.Name)
                         .First());

            // Row number limit.
            var count = 2;
            var name = "rowLimit" + count;
            updated = personQueryable
                .Take(count)
                .UpdateAll(d => d.Set(p => p.Name, name));
            Assert.AreEqual(count, updated);
            Assert.AreEqual(count, personQueryable.Count(e => e.Value.Name == name));

            // ***
            // Conditional
            // ***

            // Simple conditional
            aliasValue = 777;
            updated = personQueryable
                .Where(p => p.Key > 8)
                .UpdateAll(d => d.Set(p => p.AliasTest, aliasValue));
            Assert.AreEqual(2, updated);
            assertAll(e => e.Key <= 8 && e.Value.AliasTest != aliasValue || e.Value.AliasTest == aliasValue);

            // Conditional with limit
            aliasValue = 8888;
            updated = personQueryable
                .Where(p => p.Key > 1)
                .Take(1)
                .UpdateAll(d => d.Set(p => p.AliasTest, aliasValue));
            Assert.AreEqual(1, updated);
            assertAll(e => e.Key != 2 && e.Value.AliasTest != aliasValue || e.Value.AliasTest == aliasValue);

            // ***
            // Not supported
            // ***

            // Skip is not supported with DELETE.
            var nex = Assert.Throws<NotSupportedException>(
                () => personQueryable.Skip(1).UpdateAll(d => d.Set(p => p.Age, 15)));
            Assert.AreEqual("UpdateAll can not be combined with result operators (other than Take): SkipResultOperator",
                nex.Message);

            // Multiple result operators are not supported with DELETE.
            nex = Assert.Throws<NotSupportedException>(() =>
                personQueryable.Skip(1).Take(1).UpdateAll(d => d.Set(p => p.Age, 15)));
            Assert.AreEqual("UpdateAll can not be combined with result operators (other than Take): SkipResultOperator, " +
                "TakeResultOperator, UpdateAllResultOperator", nex.Message);

            // Joins are not supported in H2.
            var qry = personQueryable
                .Where(x => x.Key == 7)
                .Join(GetPersonCache().AsCacheQueryable(), p => p.Key, p => p.Key, (p1, p2) => p1);

            var ex = Assert.Throws<IgniteException>(() => qry.UpdateAll(d => d.Set(p => p.Age, 15)));
            Assert.AreEqual("Failed to parse query", ex.Message.Substring(0, 21));
        }
    }
}
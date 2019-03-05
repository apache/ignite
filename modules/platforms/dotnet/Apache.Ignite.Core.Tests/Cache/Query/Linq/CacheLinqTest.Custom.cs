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
        /// Tests the UpdateAll extension without condition.
        /// </summary>
        [Test]
        public void TestUpdateAllUnconditional()
        {
            // Use new cache to avoid touching static data.
            var personCount = 10;
            var orgCount = 3;
            var personQueryable = GetPersonCacheQueryable("updateAllTest_Unconditional_Persons", personCount, orgCount);
            var orgQueryable = GetOrgCacheQueryable("updateAllTest_Unconditional_Org", orgCount);

            var allOrg = orgQueryable.ToArray();

            // Constant value
            var updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, 7));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable, p => p.Value.AliasTest == 7);

            // Expression value - from self
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, e => e.Key));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable, p => p.Value.AliasTest == p.Key);

            // Multiple sets
            var aliasValue = 3;
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, aliasValue).Set(p => p.Name, aliasValue.ToString()));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable, p => p.Value.AliasTest == aliasValue && p.Value.Name == aliasValue.ToString());

            // Expression value - subquery with same cache
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest,
                    e => personQueryable.Where(ie => ie.Key == e.Key).Select(ie => ie.Key).First()));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable, p => p.Value.AliasTest == p.Key);

            // Expression value - subquery with other cache
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.AliasTest, p => orgQueryable.Count(o => o.Value.Id > p.Key)));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable, p => p.Value.AliasTest == allOrg.Count(o => o.Value.Id > p.Key));

            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.Name,
                    e => orgQueryable.Where(o => o.Key == e.Value.OrganizationId).Select(o => o.Value.Name).First()));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable,
                p => p.Value.Name == allOrg.Where(o => o.Key == p.Value.OrganizationId).Select(o => o.Value.Name)
                         .FirstOrDefault());

            // Expression value - Contains subquery with other cache
            updated = personQueryable
                .UpdateAll(d => d.Set(p => p.Bool,
                    p => orgQueryable.Select(o => o.Key).Contains(p.Value.OrganizationId)));
            Assert.AreEqual(personCount, updated);
            AssertAll(personQueryable,
                 p => p.Value.Bool == allOrg.Select(o => o.Key).Contains(p.Value.OrganizationId));

            // Row number limit.
            var name = "rowLimit" + 2;
            updated = personQueryable
                .Take(2)
                .UpdateAll(d => d.Set(p => p.Name, name));
            Assert.AreEqual(2, updated);
            Assert.AreEqual(2, personQueryable.Count(p => p.Value.Name == name));
        }

        /// <summary>
        /// Tests the UpdateAll extension with condition.
        /// </summary>
        [Test]
        public void TestUpdateAllWithCondition()
        {
            // ReSharper disable AccessToModifiedClosure
            // Use new cache to avoid touching static data.
            var personQueryable = GetPersonCacheQueryable("updateAllTest_WithCondition_Persons", 10);

            // Simple conditional
            var aliasValue = 777;
            var updated = personQueryable
                .Where(p => p.Key > 8)
                .UpdateAll(d => d.Set(p => p.AliasTest, aliasValue));
            Assert.AreEqual(2, updated);
            AssertAll(personQueryable,
                p => p.Key <= 8 && p.Value.AliasTest != aliasValue || p.Value.AliasTest == aliasValue);

            // Conditional with limit
            aliasValue = 8888;
            updated = personQueryable
                .Where(p => p.Key > 1)
                .Take(3)
                .UpdateAll(d => d.Set(p => p.AliasTest, aliasValue));
            Assert.AreEqual(3, updated);
            Assert.AreEqual(3, personQueryable.ToArray().Count(p => p.Value.AliasTest == aliasValue));
            // ReSharper restore AccessToModifiedClosure
        }

        /// <summary>
        /// Tests not supported queries for the UpdateAll extension .
        /// </summary>
        [Test]
        public void TestUpdateAllUnsupported()
        {
            // Use new cache to avoid touching static data.
            var personQueryable = GetPersonCacheQueryable("updateAllTest_Unsupported_Persons", 10);
            
            // Skip is not supported with DELETE.
            var nex = Assert.Throws<NotSupportedException>(
                () => personQueryable.Skip(1).UpdateAll(d => d.Set(p => p.Age, 15)));
            Assert.AreEqual("UpdateAll can not be combined with result operators (other than Take): SkipResultOperator",
                nex.Message);

            // Multiple result operators are not supported with DELETE.
            nex = Assert.Throws<NotSupportedException>(() =>
                personQueryable.Skip(1).Take(1).UpdateAll(d => d.Set(p => p.Age, 15)));
            Assert.AreEqual(
                "UpdateAll can not be combined with result operators (other than Take): SkipResultOperator, " +
                "TakeResultOperator, UpdateAllResultOperator", nex.Message);

            // Joins are not supported in H2.
            var qry = personQueryable
                .Where(x => x.Key == 7)
                .Join(GetPersonCache().AsCacheQueryable(), p => p.Key, p => p.Key, (p1, p2) => p1);

            var ex = Assert.Throws<IgniteException>(() => qry.UpdateAll(d => d.Set(p => p.Age, 15)));
            Assert.AreEqual("Failed to parse query", ex.Message.Substring(0, 21));
        }

        /// <summary>
        /// Gets filled persons cache  queryable
        /// </summary>
        private IQueryable<ICacheEntry<int, Person>> GetPersonCacheQueryable(string cacheName, int personCount,
            int? orgCount = null)
        {
            var cache = GetCache<Person>(cacheName);

            cache.PutAll(Enumerable.Range(1, personCount).ToDictionary(x => x,
                x => new Person(x, x.ToString())
                {
                    Birthday = DateTime.UtcNow.AddDays(personCount - x),
                    OrganizationId = x % orgCount ?? 0
                }));

            return cache.AsCacheQueryable();
        }

        /// <summary>
        /// Asserts that all values in cache correspond to predicate
        /// </summary>
        private static void AssertAll(
            // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
            IQueryable<ICacheEntry<int, Person>> personQueryable, 
            // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
            Func<ICacheEntry<int, Person>, bool> predicate)
        {
            Assert.IsTrue(personQueryable.ToArray().All(predicate));
        }

        /// <summary>
        /// Gets filled organization cache queryable
        /// </summary>
        private IQueryable<ICacheEntry<int, Organization>> GetOrgCacheQueryable(string cacheName, int orgCount)
        {
            var cache = GetCache<Organization>(cacheName);
            var allOrg = Enumerable.Range(1, orgCount)
                .Select(x => new Organization
                {
                    Id = x,
                    Name = x.ToString()
                })
                .ToList();

            allOrg.ForEach(x => cache.Put(x.Id, x));

            return cache.AsCacheQueryable();
        }

        /// <summary>
        /// Gets cache of <see cref="T"/>
        /// </summary>
        private ICache<int, T> GetCache<T>(string cacheName)
        {
            return Ignition.GetIgnite().GetOrCreateCache<int, T>(new CacheConfiguration(cacheName,
                new QueryEntity(typeof(int), typeof(T)))
            {
                SqlEscapeAll = GetSqlEscapeAll(),
                CacheMode = CacheMode.Replicated
            });
        }
    }
}
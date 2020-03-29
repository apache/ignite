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
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
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
            var defferedCollection = Enumerable.Range(1, 10)
                .Select(i => new { Id = i })
                .Select(arg => arg.Id);

            CheckWhereFunc(cache, e => new[] { 1, 2, 3 }.Contains(e.Key));
            CheckWhereFunc(cache, e => emptyKeys.Contains(e.Key));
            CheckWhereFunc(cache, e => new int[0].Contains(e.Key));
            CheckWhereFunc(cache, e => new int[0].Contains(e.Key));
            CheckWhereFunc(cache, e => new List<int> { 1, 2, 3 }.Contains(e.Key));
            CheckWhereFunc(cache, e => new List<int>(keys).Contains(e.Key));
            CheckWhereFunc(cache, e => aLotOfKeys.Contains(e.Key));
            CheckWhereFunc(cache, e => hashSetKeys.Contains(e.Key));
            CheckWhereFunc(cache, e => !keys.Contains(e.Key));
            CheckWhereFunc(cache, e => defferedCollection.Contains(e.Key));
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
                CompiledQuery.Compile((int[] k) => cache.Where(x => k.Contains(x.Key))));
            Assert.AreEqual("'Contains' clause on compiled query parameter is not supported.", ex.Message);

            // check subquery from another cache put in separate variable
            var orgIds = orgCache
                .Where(o => o.Value.Name == "Org_1")
                .Select(o => o.Key);

            var subQueryFromVar = cache
                .Where(x => orgIds.Contains(x.Value.OrganizationId))
                .ToArray();

            var subQueryInline = cache
                .Where(x => orgCache.Where(o => o.Value.Name == "Org_1")
                    .Select(o => o.Key).Contains(x.Value.OrganizationId))
                .ToArray();

            Assert.AreEqual(subQueryInline.Length, subQueryFromVar.Length);
        }
    }
}
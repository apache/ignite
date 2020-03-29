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
        /// Tests the join with local collection.
        /// </summary>
        [Test]
        public void TestLocalJoin()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var orgs = GetOrgCache().AsCacheQueryable();

            var localOrgs = orgs
                .Select(e => e.Value)
                .ToArray();

            var allOrganizationIds = localOrgs
                .Select(e => e.Id)
                .ToArray();

            // Join with local collection 
            var qry1 = persons.Join(allOrganizationIds,
                    pe => pe.Value.OrganizationId,
                    i => i,
                    (pe, o) => pe
                )
                .ToArray();

            Assert.AreEqual(PersonCount, qry1.Length);

            // Join using expression in innerKeySelector
            var qry2 = persons.Join(allOrganizationIds,
                    pe => pe.Value.OrganizationId,
                    i => i + 1 - 1,
                    (pe, o) => pe
                )
                .ToArray();

            Assert.AreEqual(PersonCount, qry2.Length);

            // Local collection subquery
            var qry3 = persons.Join(localOrgs.Select(e => e.Id),
                    pe => pe.Value.OrganizationId,
                    i => i,
                    (pe, o) => pe
                )
                .ToArray();

            Assert.AreEqual(PersonCount, qry3.Length);

            // Compiled query
            var qry4 = CompiledQuery.Compile(() => persons.Join(allOrganizationIds,
                pe => pe.Value.OrganizationId,
                i => i,
                (pe, o) => pe
            ));

            Assert.AreEqual(PersonCount, qry4().Count());

            // Compiled query with outer join
            var qry4A = CompiledQuery.Compile(() => persons.Join(new int[] { }.DefaultIfEmpty(),
                pe => pe.Value.OrganizationId,
                i => i,
                (pe, o) => pe
            ));

            Assert.AreEqual(PersonCount, qry4A().Count());

            // Compiled query
            var qry5 = CompiledQuery.Compile(() => persons.Join(new[] { -1, -2 }.DefaultIfEmpty(),
                pe => pe.Value.OrganizationId,
                i => i,
                (pe, o) => pe
            ));

            Assert.AreEqual(PersonCount, qry5().Count());

            // Outer join
            var qry6 = persons.Join(new[] { -1, -2 }.DefaultIfEmpty(),
                    pe => pe.Value.OrganizationId,
                    i => i,
                    (pe, o) => pe
                )
                .ToArray();

            Assert.AreEqual(PersonCount, qry6.Length);

            // Join with local list
            var qry7 = persons.Join(new List<int> { 1000, 1001, 1002, 1003 },
                    pe => pe.Value.OrganizationId,
                    i => i,
                    (pe, o) => pe
                )
                .ToArray();

            Assert.AreEqual(PersonCount, qry7.Length);

            // Join with local list variable
            var list = new List<int> { 1000, 1001, 1002, 1003 };
            var qry8 = persons.Join(list,
                    pe => pe.Value.OrganizationId,
                    i => i,
                    (pe, o) => pe
                )
                .ToArray();

            Assert.AreEqual(PersonCount, qry8.Length);
        }

        /// <summary>
        /// Tests the compiled query containing join with local collection passed as parameter.
        /// </summary>
        [Test]
        [Ignore("IGNITE-5404")]
        public void TestLocalJoinCompiledQueryParameter()
        {
            var persons = GetPersonCache().AsCacheQueryable();
            var orgs = GetOrgCache().AsCacheQueryable();

            var localOrgs = orgs
                .Select(e => e.Value)
                .ToArray();

            var allOrganizationIds = localOrgs
                .Select(e => e.Id)
                .ToArray();

            // Join with local collection passed as parameter
            var qry1 = CompiledQuery.Compile((IEnumerable<int> lc) => persons.Join(lc,
                pe => pe.Value.OrganizationId,
                i => i,
                (pe, o) => pe
            ));

            Assert.AreEqual(PersonCount, qry1(allOrganizationIds).Count());

            //Compiled query with outer join
            var qry2 = CompiledQuery.Compile((IEnumerable<int> lc) => persons.Join(lc.DefaultIfEmpty(),
                pe => pe.Value.OrganizationId,
                i => i,
                (pe, o) => pe
            ));

            Assert.AreEqual(PersonCount, qry2(new[] { -11 }).Count());
        }
    }
}
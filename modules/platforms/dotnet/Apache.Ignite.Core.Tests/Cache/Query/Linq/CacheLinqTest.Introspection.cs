/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ.
    /// </summary>
    public partial class CacheLinqTest
    {
        /// <summary>
        /// Tests the introspection.
        /// </summary>
        [Test]
        public void TestIntrospection()
        {
            var cache = GetPersonCache();

            // Check regular query
            var query = cache.AsCacheQueryable(new QueryOptions
            {
                Local = true,
                PageSize = 999,
                EnforceJoinOrder = true,
                Timeout = TimeSpan.FromSeconds(2.5),
#pragma warning disable 618
                ReplicatedOnly = true,
#pragma warning restore 618
                Colocated = true,
                Lazy = true
            }).Where(x => x.Key > 10).ToCacheQueryable();

            Assert.AreEqual(cache.Name, query.CacheName);
#pragma warning disable 618 // Type or member is obsolete
            Assert.AreEqual(cache.Ignite, query.Ignite);
#pragma warning restore 618 // Type or member is obsolete

            var fq = query.GetFieldsQuery();

            Assert.AreEqual(
                GetSqlEscapeAll()
                    ? "select _T0._KEY, _T0._VAL from PERSON_ORG_SCHEMA.\"Person\" as _T0 where (_T0.\"_KEY\" > ?)"
                    : "select _T0._KEY, _T0._VAL from PERSON_ORG_SCHEMA.Person as _T0 where (_T0._KEY > ?)",
                fq.Sql);

            Assert.AreEqual(new[] { 10 }, fq.Arguments);
            Assert.IsTrue(fq.Local);
            Assert.AreEqual(PersonCount - 11, cache.Query(fq).GetAll().Count);
            Assert.AreEqual(999, fq.PageSize);
            Assert.IsFalse(fq.EnableDistributedJoins);
            Assert.IsTrue(fq.EnforceJoinOrder);
#pragma warning disable 618
            Assert.IsTrue(fq.ReplicatedOnly);
#pragma warning restore 618
            Assert.IsTrue(fq.Colocated);
            Assert.AreEqual(TimeSpan.FromSeconds(2.5), fq.Timeout);
            Assert.IsTrue(fq.Lazy);

            var str = query.ToString();
            Assert.AreEqual(GetSqlEscapeAll()
                ? "CacheQueryable [CacheName=person_org, TableName=Person, Query=SqlFieldsQuery " +
                  "[Sql=select _T0._KEY, _T0._VAL from PERSON_ORG_SCHEMA.\"Person\" as _T0 where " +
                  "(_T0.\"_KEY\" > ?), Arguments=[10], " +
                  "Local=True, PageSize=999, EnableDistributedJoins=False, EnforceJoinOrder=True, " +
                  "Timeout=00:00:02.5000000, ReplicatedOnly=True, Colocated=True, Schema=, Lazy=True]]"
                : "CacheQueryable [CacheName=person_org, TableName=Person, Query=SqlFieldsQuery " +
                  "[Sql=select _T0._KEY, _T0._VAL from PERSON_ORG_SCHEMA.Person as _T0 where " +
                  "(_T0._KEY > ?), Arguments=[10], " +
                  "Local=True, PageSize=999, EnableDistributedJoins=False, EnforceJoinOrder=True, " +
                  "Timeout=00:00:02.5000000, ReplicatedOnly=True, Colocated=True, Schema=, Lazy=True]]", str);

            // Check fields query
            var fieldsQuery = cache.AsCacheQueryable().Select(x => x.Value.Name).ToCacheQueryable();

            Assert.AreEqual(cache.Name, fieldsQuery.CacheName);
#pragma warning disable 618 // Type or member is obsolete
            Assert.AreEqual(cache.Ignite, query.Ignite);
#pragma warning restore 618 // Type or member is obsolete

            fq = fieldsQuery.GetFieldsQuery();
            Assert.AreEqual(GetSqlEscapeAll()
                    ? "select _T0.\"Name\" from PERSON_ORG_SCHEMA.\"Person\" as _T0"
                    : "select _T0.NAME from PERSON_ORG_SCHEMA.Person as _T0",
                fq.Sql);

            Assert.IsFalse(fq.Local);
            Assert.AreEqual(SqlFieldsQuery.DefaultPageSize, fq.PageSize);
            Assert.IsFalse(fq.EnableDistributedJoins);
            Assert.IsFalse(fq.EnforceJoinOrder);
            Assert.IsFalse(fq.Lazy);

            str = fieldsQuery.ToString();
            Assert.AreEqual(GetSqlEscapeAll()
                ? "CacheQueryable [CacheName=person_org, TableName=Person, Query=SqlFieldsQuery " +
                  "[Sql=select _T0.\"Name\" from PERSON_ORG_SCHEMA.\"Person\" as _T0, Arguments=[], Local=False, " +
                  "PageSize=1024, EnableDistributedJoins=False, EnforceJoinOrder=False, " +
                  "Timeout=00:00:00, ReplicatedOnly=False, Colocated=False, Schema=, Lazy=False]]"
                : "CacheQueryable [CacheName=person_org, TableName=Person, Query=SqlFieldsQuery " +
                  "[Sql=select _T0.NAME from PERSON_ORG_SCHEMA.Person as _T0, Arguments=[], Local=False, " +
                  "PageSize=1024, EnableDistributedJoins=False, EnforceJoinOrder=False, " +
                  "Timeout=00:00:00, ReplicatedOnly=False, Colocated=False, Schema=, Lazy=False]]", str);

            // Check distributed joins flag propagation
            var distrQuery = cache.AsCacheQueryable(new QueryOptions { EnableDistributedJoins = true })
                .Where(x => x.Key > 10 && x.Value.Age > 20 && x.Value.Name.Contains("x"));

            query = distrQuery.ToCacheQueryable();

            Assert.IsTrue(query.GetFieldsQuery().EnableDistributedJoins);

            str = distrQuery.ToString();
            Assert.AreEqual(GetSqlEscapeAll()
                ? "CacheQueryable [CacheName=person_org, TableName=Person, Query=SqlFieldsQuery " +
                  "[Sql=select _T0._KEY, _T0._VAL from PERSON_ORG_SCHEMA.\"Person\" as _T0 where " +
                  "(((_T0.\"_KEY\" > ?) and (_T0.\"age1\" > ?)) " +
                  "and (_T0.\"Name\" like \'%\' || ? || \'%\') ), Arguments=[10, 20, x], Local=False, " +
                  "PageSize=1024, EnableDistributedJoins=True, EnforceJoinOrder=False, " +
                  "Timeout=00:00:00, ReplicatedOnly=False, Colocated=False, Schema=, Lazy=False]]"
                : "CacheQueryable [CacheName=person_org, TableName=Person, Query=SqlFieldsQuery " +
                  "[Sql=select _T0._KEY, _T0._VAL from PERSON_ORG_SCHEMA.Person as _T0 where " +
                  "(((_T0._KEY > ?) and (_T0.AGE1 > ?)) " +
                  "and (_T0.NAME like \'%\' || ? || \'%\') ), Arguments=[10, 20, x], Local=False, " +
                  "PageSize=1024, EnableDistributedJoins=True, EnforceJoinOrder=False, " +
                  "Timeout=00:00:00, ReplicatedOnly=False, Colocated=False, Schema=, Lazy=False]]", str);
        }
    }
}

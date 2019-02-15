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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Linq;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ in thin client.
    /// </summary>
    public class LinqTest : SqlQueryTestBase
    {
        /// <summary>
        /// Tests basic queries.
        /// </summary>
        [Test]
        public void TestBasicQueries()
        {
            var cache = GetClientCache<Person>();

            // All items.
            var qry = cache.AsCacheQueryable();
            Assert.AreEqual(Count, qry.Count());

            // Filter.
            qry = cache.AsCacheQueryable().Where(x => x.Value.Name.EndsWith("7"));
            Assert.AreEqual(7, qry.Single().Key);
            Assert.AreEqual("select _T0._KEY, _T0._VAL from \"cache\".PERSON as _T0 where (_T0.NAME like '%' || ?) ",
                qry.ToCacheQueryable().GetFieldsQuery().Sql);

            // DateTime.
            var arg = DateTime.UtcNow.AddDays(Count - 1);
            var qry2 = cache.AsCacheQueryable(false, "Person")
                .Where(x => x.Value.DateTime > arg).Select(x => x.Key);
            Assert.AreEqual(Count, qry2.Single());
        }

        /// <summary>
        /// Tests joins.
        /// </summary>
        [Test]
        public void TestJoins()
        {
            var cache1 = Client.GetCache<int, Person>(CacheName);
            var cache2 = Client.GetCache<int, Person>(CacheName2);

            // Non-distributed join returns incomplete results.
            var persons1 = cache1.AsCacheQueryable(false);
            var persons2 = cache2.AsCacheQueryable();

            var qry = persons1
                .Join(persons2, p1 => p1.Value.Id, p2 => Count + 1 - p2.Value.Id, (p1, p2) => p2.Value.Name);

            Assert.Greater(Count, qry.ToArray().Length);


            // Distributed join fixes the problem.
            persons1 = cache1.AsCacheQueryable(new QueryOptions {EnableDistributedJoins = true});
            persons2 = cache2.AsCacheQueryable(new QueryOptions {EnableDistributedJoins = true});

            var qry2 =
                from p1 in persons1
                join p2 in persons2 on p1.Value.Id equals Count + 1 - p2.Value.Id
                select p2.Value.DateTime;

            Assert.AreEqual(Count, qry2.ToArray().Length);
        }

        /// <summary>
        /// Tests DML via LINQ.
        /// </summary>
        [Test]
        public void TestDml()
        {
            var cache = GetClientCache<Person>();

            Assert.AreEqual(Count, cache.GetSize());

            var res = cache.AsCacheQueryable().Where(x => x.Key % 3 == 0).RemoveAll();
            Assert.AreEqual(Count / 3, res);

            Assert.AreEqual(Count - res, cache.GetSize());
        }

        /// <summary>
        /// Tests the compiled query.
        /// </summary>
        [Test]
        public void TestCompiledQuery()
        {
            var cache = GetClientCache<Person>();
            var persons = cache.AsCacheQueryable();

            var qry = CompiledQuery.Compile((int id) => persons.Where(x => x.Value.Id == id));

            Assert.AreEqual(1, qry(1).Single().Key);
            Assert.AreEqual(3, qry(3).Single().Key);
        }
    }
}

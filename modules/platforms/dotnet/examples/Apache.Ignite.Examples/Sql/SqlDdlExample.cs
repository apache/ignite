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

namespace Apache.Ignite.Examples.Sql
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// This example showcases DDL capabilities of Ignite's SQL engine.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config
    /// 2) Start example.
    /// </summary>
    public class SqlDdlExample
    {
        /// <summary>Dummy cache name.</summary>
        private const string DummyCacheName = "dummy_cache";

        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache query DDL example started.");
                
                // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
                // will appear in future versions, JDBC and ODBC drivers do not require it already).
                var cacheCfg = new CacheConfiguration(DummyCacheName)
                {
                    SqlSchema = "PUBLIC",
                    CacheMode = CacheMode.Replicated
                };

                ICache<object, object> cache = ignite.GetOrCreateCache<object, object>(cacheCfg);
                
                // Create reference City table based on REPLICATED template.
                cache.Query(new SqlFieldsQuery(
                    "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).GetAll();

                // Create table based on PARTITIONED template with one backup.
                cache.Query(new SqlFieldsQuery(
                    "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                    "WITH \"backups=1, affinity_key=city_id\"")).GetAll();

                // Create an index.
                cache.Query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).GetAll();
                
                Console.WriteLine("\n>>> Created database objects.");

                const string addCity = "INSERT INTO city (id, name) VALUES (?, ?)";
                
                cache.Query(new SqlFieldsQuery(addCity, 1L, "Forest Hill"));
                cache.Query(new SqlFieldsQuery(addCity, 2L, "Denver"));
                cache.Query(new SqlFieldsQuery(addCity, 3L, "St. Petersburg"));

                const string addPerson = "INSERT INTO person (id, name, city_id) values (?, ?, ?)";
                
                cache.Query(new SqlFieldsQuery(addPerson, 1L, "John Doe", 3L));
                cache.Query(new SqlFieldsQuery(addPerson, 2L, "Jane Roe", 2L));
                cache.Query(new SqlFieldsQuery(addPerson, 3L, "Mary Major", 1L));
                cache.Query(new SqlFieldsQuery(addPerson, 4L, "Richard Miles", 2L));
                
                Console.WriteLine("\n>>> Populated data.");
                
                IFieldsQueryCursor res = cache.Query(new SqlFieldsQuery(
                    "SELECT p.name, c.name FROM Person p INNER JOIN City c on c.id = p.city_id"));

                Console.WriteLine("\n>>> Query results:");

                foreach (var row in res)
                {
                    Console.WriteLine("{0}, {1}", row[0], row[1]);
                }
                
                cache.Query(new SqlFieldsQuery("drop table Person")).GetAll();
                cache.Query(new SqlFieldsQuery("drop table City")).GetAll();

                Console.WriteLine("\n>>> Dropped database objects.");
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
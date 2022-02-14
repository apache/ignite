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

namespace Apache.Ignite.Examples.Thin.Sql.DdlThin
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Examples.Shared;

    /// <summary>
    /// This example showcases DDL capabilities of the Ignite SQL engine with thin client.
    /// <para />
    /// This example requires an Ignite server node. You can start the node in any of the following ways:
    /// * docker run -p 10800:10800 apacheignite/ignite
    /// * dotnet run -p ServerNode.csproj
    /// * ignite.sh/ignite.bat from the distribution
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgniteClient ignite = Ignition.StartClient(Utils.GetThinClientConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache query DDL example started.");

                // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
                // will appear in future versions, JDBC and ODBC drivers do not require it already).
                var cacheCfg = new CacheClientConfiguration("dummy_cache")
                {
                    SqlSchema = "PUBLIC",
                    CacheMode = CacheMode.Replicated
                };

                ICacheClient<object, object> cache = ignite.GetOrCreateCache<object, object>(cacheCfg);

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

                Console.WriteLine("\n>>> Dropped database objects.");            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}

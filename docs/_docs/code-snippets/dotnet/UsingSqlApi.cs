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

using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class UsingSqlApi
    {
        // tag::sqlQueryFields[]
        class Person
        {
            // Indexed field. Will be visible to the SQL engine.
            [QuerySqlField(IsIndexed = true)] public long Id;

            //Queryable field. Will be visible to the SQL engine
            [QuerySqlField] public string Name;

            //Will NOT be visible to the SQL engine.
            public int Age;

            /**
              * Indexed field sorted in descending order.
              * Will be visible to the SQL engine.
            */
            [QuerySqlField(IsIndexed = true, IsDescending = true)]
            public float Salary;
        }

        public static void SqlQueryFieldDemo()
        {
            var cacheCfg = new CacheConfiguration
            {
                Name = "cacheName",
                QueryEntities = new[]
                {
                    new QueryEntity(typeof(int), typeof(Person))
                }
            };

            var ignite = Ignition.Start();
            var cache = ignite.CreateCache<int, Person>(cacheCfg);
        }
        // end::sqlQueryFields[]

        public class Inner
        {
            // tag::queryEntities[]
            private class Person
            {
                public long Id;

                public string Name;

                public int Age;

                public float Salary;
            }

            public static void QueryEntitiesDemo()
            {
                var personCacheCfg = new CacheConfiguration
                {
                    Name = "Person",
                    QueryEntities = new[]
                    {
                        new QueryEntity
                        {
                            KeyType = typeof(long),
                            ValueType = typeof(Person),
                            Fields = new[]
                            {
                                new QueryField("Id", typeof(long)),
                                new QueryField("Name", typeof(string)),
                                new QueryField("Age", typeof(int)),
                                new QueryField("Salary", typeof(float))
                            },
                            Indexes = new[]
                            {
                                new QueryIndex("Id"),
                                new QueryIndex(true, "Salary"),
                            }
                        }
                    }
                };
                var ignite = Ignition.Start();
                var personCache = ignite.CreateCache<int, Person>(personCacheCfg);
            }
            // end::queryEntities[]

            public static void QueryingDemo()
            {
                var ignite = Ignition.Start();
                // tag::querying[]
                var cache = ignite.GetCache<long, Person>("Person");

                var sql = new SqlFieldsQuery("select concat(FirstName, ' ', LastName) from Person");

                using (var cursor = cache.Query(sql))
                {
                    foreach (var row in cursor)
                    {
                        Console.WriteLine("personName=" + row[0]);
                    }
                }

                // end::querying[]

                // tag::schema[]
                var sqlFieldsQuery = new SqlFieldsQuery("select name from City") {Schema = "PERSON"};
                // end::schema[]
            }

            public static void CreateTableDdlDemo()
            {
                var ignite = Ignition.Start(new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
                    {
                        LocalPort = 48500,
                        LocalPortRange = 20,
                        IpFinder = new TcpDiscoveryStaticIpFinder
                        {
                            Endpoints = new[]
                            {
                                "127.0.0.1:48500..48520"
                            }
                        }
                    }
                });

                // tag::creatingTables[]
                var cache = ignite.GetOrCreateCache<long, Person>(
                    new CacheConfiguration
                    {
                        Name = "Person"
                    }
                );

                //Creating City table
                cache.Query(new SqlFieldsQuery("CREATE TABLE City (id int primary key, name varchar, region varchar)"));
                // end::creatingTables[]

                var qry = new SqlFieldsQuery("select name from City") {Schema = "PERSON"};
                cache.Query(qry).GetAll();
            }

            public static void CancellingQueries()
            {
                var ignite = Ignition.Start(
                    new IgniteConfiguration
                    {
                        DiscoverySpi = new TcpDiscoverySpi
                        {
                            LocalPort = 48500,
                            LocalPortRange = 20,
                            IpFinder = new TcpDiscoveryStaticIpFinder
                            {
                                Endpoints = new[]
                                {
                                    "127.0.0.1:48500..48520"
                                }
                            }
                        },
                        CacheConfiguration = new[]
                        {
                            new CacheConfiguration
                            {
                                Name = "personCache",
                                QueryEntities = new[] {new QueryEntity(typeof(long), typeof(Person)),}
                            },
                        }
                    }
                );
                var cache = ignite.GetOrCreateCache<long, Person>("personCache");
                // tag::qryTimeout[]
                var query = new SqlFieldsQuery("select * from Person") {Timeout = TimeSpan.FromSeconds(10)};
                // end::qryTimeout[]

                // tag::cursorDispose[]
                var qry = new SqlFieldsQuery("select * from Person");
                var cursor = cache.Query(qry);

                //Executing query

                //Halting the query that might be still in progress
                cursor.Dispose();
                // end::cursorDispose[]
            }
        }
    }
}

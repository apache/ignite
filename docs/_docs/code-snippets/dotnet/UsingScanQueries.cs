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
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;

namespace dotnet_helloworld_queries
{

    class Person
    {
        public string Name { get; set; }
        public int Salary { get; set; }
    }

    public class UsingScanQueries
    {
        public static void ExecutingScanQueries()
        {
            var ignite = Ignition.Start();
            var cache = ignite.GetOrCreateCache<int, Person>("person_cache");
            // tag::scanQry1[]
            var cursor = cache.Query(new ScanQuery<int, Person>());
            // end::scanQry1[]
        }

        // tag::scanQry2[]
        class SalaryFilter : ICacheEntryFilter<int, Person>
        {
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return entry.Value.Salary > 1000;
            }
        }

        public static void ScanQueryFilterDemo()
        {
            var ignite = Ignition.Start();
            var cache = ignite.GetOrCreateCache<int, Person>("person_cache");

            cache.Put(1, new Person {Name = "person1", Salary = 1001});
            cache.Put(2, new Person {Name = "person2", Salary = 999});

            using (var cursor = cache.Query(new ScanQuery<int, Person>(new SalaryFilter())))
            {
                foreach (var entry in cursor)
                {
                    Console.WriteLine("Key = " + entry.Key + ", Value = " + entry.Value);
                }
            }
        }

        // end::scanQry2[]

        public static void LocalScanQuery()
        {
            var ignite = Ignition.Start();
            var cache = ignite.GetOrCreateCache<int, Person>("person_cache");

            // tag::scanQryLocal[]
            var query = new ScanQuery<int, Person> {Local = true};
            var cursor = cache.Query(query);
            // end::scanQryLocal[]
        }
    }
}

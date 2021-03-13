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

namespace Apache.Ignite.Examples.Thick.Cache.QueryFullText
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Models;

    /// <summary>
    /// This example demonstrates full-text search capabilities.
    /// </summary>
    public static class Program
    {
        private const string EmployeeCacheName = "dotnet_cache_query_full_text_employee";

        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache full-text query example started.");

                var employeeCache = ignite.GetOrCreateCache<int, Employee>(
                    new CacheConfiguration(EmployeeCacheName, new QueryEntity(typeof(int), typeof(Employee))));

                Utils.PopulateCache(employeeCache);

                var qry = employeeCache.Query(new TextQuery(queryType: "Employee", text: "TX"));

                Console.WriteLine();
                Console.WriteLine(">>> Employees living in Texas:");

                foreach (var entry in qry)
                    Console.WriteLine(">>> " + entry.Value);

                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}

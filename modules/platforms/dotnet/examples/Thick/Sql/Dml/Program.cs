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

namespace Apache.Ignite.Examples.Thick.Sql.Dml
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Models;

    /// <summary>
    /// This example showcases DML capabilities of the Ignite SQL engine.
    /// </summary>
    public static class Program
    {
        private const string OrganizationCacheName = "dotnet_cache_query_dml_organization";

        private const string EmployeeCacheName = "dotnet_cache_query_dml_employee";

        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache query DML example started.");

                var employeeCache = ignite.GetOrCreateCache<int, Employee>(
                    new CacheConfiguration(EmployeeCacheName, new QueryEntity(typeof(int), typeof(Employee))));

                var organizationCache = ignite.GetOrCreateCache<int, Organization>(new CacheConfiguration(
                    OrganizationCacheName, new QueryEntity(typeof(int), typeof(Organization))));

                employeeCache.Clear();
                organizationCache.Clear();

                Insert(organizationCache, employeeCache);
                Select(employeeCache, "Inserted data");

                Update(employeeCache);
                Select(employeeCache, "Update salary for ASF employees");

                Delete(employeeCache);
                Select(employeeCache, "Delete non-ASF employees");

                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Selects and displays Employee data.
        /// </summary>
        private static void Select(ICache<int, Employee> employeeCache, string message)
        {
            Console.WriteLine("\n>>> {0}", message);

            var qry = new SqlFieldsQuery(string.Format(
                "select emp._key, emp.name, org.name, emp.salary " +
                "from Employee as emp, " +
                "\"{0}\".Organization as org " +
                "where emp.organizationId = org._key", OrganizationCacheName))
            {
                EnableDistributedJoins = true
            };

            using (var cursor = employeeCache.Query(qry))
            {
                foreach (var row in cursor)
                {
                    Console.WriteLine(">>> {0}: {1}, {2}, {3}", row[0], row[1], row[2], row[3]);
                }
            }
        }

        /// <summary>
        /// Populates cache with test data.
        /// </summary>
        private static void Insert(ICache<int, Organization> organizationCache, ICache<int, Employee> employeeCache)
        {
            // Insert organizations.
            var qry = new SqlFieldsQuery("insert into Organization (_key, name) values (?, ?)", 1, "ASF");
            organizationCache.Query(qry);

            qry.Arguments = new object[] {2, "Eclipse"};
            organizationCache.Query(qry);

            // Insert employees.
            qry = new SqlFieldsQuery("insert into Employee (_key, name, organizationId, salary) values (?, ?, ?, ?)");

            qry.Arguments = new object[] {1, "John Doe", 1, 4000};
            employeeCache.Query(qry);

            qry.Arguments = new object[] {2, "Jane Roe", 1, 5000};
            employeeCache.Query(qry);

            qry.Arguments = new object[] {3, "Mary Major", 2, 2000};
            employeeCache.Query(qry);

            qry.Arguments = new object[] {4, "Richard Miles", 2, 3000};
            employeeCache.Query(qry);
        }

        /// <summary>
        /// Conditional UPDATE query: raise salary for ASF employees.
        /// </summary>
        /// <param name="employeeCache">Employee cache.</param>
        private static void Update(ICache<int, Employee> employeeCache)
        {
            var qry = new SqlFieldsQuery("update Employee set salary = salary * 1.1 where organizationId = ?", 1);

            employeeCache.Query(qry);
        }

        /// <summary>
        /// Conditional DELETE query: remove non-ASF employees.
        /// </summary>
        /// <param name="employeeCache">Employee cache.</param>
        private static void Delete(ICache<int, Employee> employeeCache)
        {
            var qry = new SqlFieldsQuery("delete from Employee where organizationId != ?", 1);

            employeeCache.Query(qry);
        }
    }
}

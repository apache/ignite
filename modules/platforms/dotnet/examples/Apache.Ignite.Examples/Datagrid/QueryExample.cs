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
using System.Collections;
using System.Collections.Generic;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.ExamplesDll.Binary;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// This example populates cache with sample data and runs several SQL and
    /// full text queries over this data.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public class QueryExample
    {
        /// <summary>Cache name.</summary>
        private const string CacheName = "dotnet_cache_query";

        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache query example started.");

                var cache = ignite.GetOrCreateCache<object, object>(new CacheConfiguration
                {
                    Name = CacheName,
                    QueryEntities = new[]
                    {
                        new QueryEntity(typeof(int), typeof(Organization)),
                        new QueryEntity(typeof(EmployeeKey), typeof(Employee))
                    }
                });

                // Clean up caches on all nodes before run.
                cache.Clear();

                // Populate cache with sample data entries.
                PopulateCache(cache);

                // Create cache that will work with specific types.
                var employeeCache = ignite.GetCache<EmployeeKey, Employee>(CacheName);

                // Run SQL query example.
                SqlQueryExample(employeeCache);

                // Run SQL query with join example.
                SqlJoinQueryExample(employeeCache);

                // Run SQL fields query example.
                SqlFieldsQueryExample(employeeCache);

                // Run full text query example.
                FullTextQueryExample(employeeCache);

                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Queries employees that have provided ZIP code in address.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlQueryExample(ICache<EmployeeKey, Employee> cache)
        {
            const int zip = 94109;

            var qry = cache.Query(new SqlQuery(typeof(Employee), "zip = ?", zip));

            Console.WriteLine();
            Console.WriteLine(">>> Employees with zipcode " + zip + ":");

            foreach (var entry in qry)
                Console.WriteLine(">>>    " + entry.Value);
        }

        /// <summary>
        /// Queries employees that work for organization with provided name.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlJoinQueryExample(ICache<EmployeeKey, Employee> cache)
        {
            const string orgName = "Apache";

            var qry = cache.Query(new SqlQuery("Employee",
                "from Employee, Organization " +
                "where Employee.organizationId = Organization._key and Organization.name = ?", orgName));

            Console.WriteLine();
            Console.WriteLine(">>> Employees working for " + orgName + ":");

            foreach (var entry in qry)
                Console.WriteLine(">>>     " + entry.Value);
        }

        /// <summary>
        /// Queries names and salaries for all employees.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlFieldsQueryExample(ICache<EmployeeKey, Employee> cache)
        {
            var qry = cache.QueryFields(new SqlFieldsQuery("select name, salary from Employee"));

            Console.WriteLine();
            Console.WriteLine(">>> Employee names and their salaries:");

            foreach (IList row in qry)
                Console.WriteLine(">>>     [Name=" + row[0] + ", salary=" + row[1] + ']');
        }

        /// <summary>
        /// Queries employees that live in Texas using full-text query API.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void FullTextQueryExample(ICache<EmployeeKey, Employee> cache)
        {
            var qry = cache.Query(new TextQuery("Employee", "TX"));

            Console.WriteLine();
            Console.WriteLine(">>> Employees living in Texas:");

            foreach (var entry in qry)
                Console.WriteLine(">>> " + entry.Value);
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICache<object, object> cache)
        {
            cache.Put(1, new Organization(
                "Apache",
                new Address("1065 East Hillsdale Blvd, Foster City, CA", 94404),
                OrganizationType.Private,
                DateTime.Now
            ));

            cache.Put(2, new Organization(
                "Microsoft",
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                OrganizationType.Private,
                DateTime.Now
            ));

            cache.Put(new EmployeeKey(1, 1), new Employee(
                "James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                new List<string> { "Human Resources", "Customer Service" }
            ));

            cache.Put(new EmployeeKey(2, 1), new Employee(
                "Daniel Adams",
                11000,
                new Address("184 Fidler Drive, San Antonio, TX", 78130),
                new List<string> { "Development", "QA" }
            ));

            cache.Put(new EmployeeKey(3, 1), new Employee(
                "Cristian Moss",
                12500,
                new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                new List<string> { "Logistics" }
            ));

            cache.Put(new EmployeeKey(4, 2), new Employee(
                "Allison Mathis",
                25300,
                new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                new List<string> { "Development" }
            ));

            cache.Put(new EmployeeKey(5, 2), new Employee(
                "Breana Robbin",
                6500,
                new Address("3960 Sundown Lane, Austin, TX", 78130),
                new List<string> { "Sales" }
            ));

            cache.Put(new EmployeeKey(6, 2), new Employee(
                "Philip Horsley",
                19800,
                new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                new List<string> { "Sales" }
            ));

            cache.Put(new EmployeeKey(7, 2), new Employee(
                "Brian Peters",
                10600,
                new Address("1407 Pearlman Avenue, Boston, MA", 12110),
                new List<string> { "Development", "QA" }
            ));
        }
    }
}

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

namespace Apache.Ignite.Examples.Sql
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.ExamplesDll.Binary;

    /// <summary>
    /// This example populates cache with sample data and runs SQL queries.
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
    public class SqlExample
    {
        /// <summary>Organization cache name.</summary>
        private const string OrganizationCacheName = "dotnet_cache_query_organization";

        /// <summary>Employee cache name.</summary>
        private const string EmployeeCacheName = "dotnet_cache_query_employee";

        /// <summary>Employee cache name.</summary>
        private const string EmployeeCacheNameColocated = "dotnet_cache_query_employee_colocated";

        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache query example started.");

                var employeeCache = ignite.GetOrCreateCache<int, Employee>(
                    new CacheConfiguration(EmployeeCacheName, new QueryEntity(typeof(int), typeof(Employee))));

                var employeeCacheColocated = ignite.GetOrCreateCache<AffinityKey, Employee>(
                    new CacheConfiguration(EmployeeCacheNameColocated,
                        new QueryEntity(typeof(AffinityKey), typeof(Employee))));

                var organizationCache = ignite.GetOrCreateCache<int, Organization>(
                    new CacheConfiguration(OrganizationCacheName, new QueryEntity(typeof(int), typeof(Organization))));

                // Populate cache with sample data entries.
                PopulateCache(employeeCache);
                PopulateCache(employeeCacheColocated);
                PopulateCache(organizationCache);

                // Run SQL query example.
                SqlQueryExample(employeeCache);

                // Run SQL query with join example.
                SqlJoinQueryExample(employeeCacheColocated);

                // Run SQL query with distributed join example.
                SqlDistributedJoinQueryExample(employeeCache);

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
        private static void SqlQueryExample(ICache<int, Employee> cache)
        {
            const int zip = 94109;

            var qry = cache.Query(new SqlFieldsQuery("select name, salary from Employee where zip = ?", zip));

            Console.WriteLine();
            Console.WriteLine(">>> Employees with zipcode {0} (SQL):", zip);

            foreach (var row in qry)
                Console.WriteLine(">>>     [Name=" + row[0] + ", salary=" + row[1] + ']');
        }

        /// <summary>
        /// Queries employees that work for organization with provided name.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlJoinQueryExample(ICache<AffinityKey, Employee> cache)
        {
            const string orgName = "Apache";

            var qry = cache.Query(new SqlFieldsQuery(
                "select Employee.name from Employee, \"dotnet_cache_query_organization\".Organization " +
                "where Employee.organizationId = Organization._key and Organization.name = ?", orgName));

            Console.WriteLine();
            Console.WriteLine(">>> Employees working for " + orgName + ":");

            foreach (var entry in qry)
                Console.WriteLine(">>>     " + entry[0]);
        }

        /// <summary>
        /// Queries employees that work for organization with provided name.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlDistributedJoinQueryExample(ICache<int, Employee> cache)
        {
            const string orgName = "Apache";

            var qry = cache.Query(new SqlFieldsQuery(
                "select Employee.name from Employee, \"dotnet_cache_query_organization\".Organization " +
                "where Employee.organizationId = Organization._key and Organization.name = ?", orgName)
            {
                EnableDistributedJoins = true,
                Timeout = new TimeSpan(0, 1, 0)
            });

            Console.WriteLine();
            Console.WriteLine(">>> Employees working for " + orgName + " (distributed joins enabled):");

            foreach (var entry in qry)
                Console.WriteLine(">>>     " + entry[0]);
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICache<int, Organization> cache)
        {
            cache.Put(1, new Organization(
                "Apache",
                new Address("1065 East Hillsdale Blvd, Foster City, CA", 94404),
                OrganizationType.Private,
                DateTime.Now));

            cache.Put(2, new Organization("Microsoft",
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                OrganizationType.Private,
                DateTime.Now));
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICache<AffinityKey, Employee> cache)
        {
            cache.Put(new AffinityKey(1, 1), new Employee(
                "James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                new[] {"Human Resources", "Customer Service"},
                1));

            cache.Put(new AffinityKey(2, 1), new Employee(
                "Daniel Adams",
                11000,
                new Address("184 Fidler Drive, San Antonio, TX", 78130),
                new[] {"Development", "QA"},
                1));

            cache.Put(new AffinityKey(3, 1), new Employee(
                "Cristian Moss",
                12500,
                new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                new[] {"Logistics"},
                1));

            cache.Put(new AffinityKey(4, 2), new Employee(
                "Allison Mathis",
                25300,
                new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                new[] {"Development"},
                2));

            cache.Put(new AffinityKey(5, 2), new Employee(
                "Breana Robbin",
                6500,
                new Address("3960 Sundown Lane, Austin, TX", 78130),
                new[] {"Sales"},
                2));

            cache.Put(new AffinityKey(6, 2), new Employee(
                "Philip Horsley",
                19800,
                new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                new[] {"Sales"},
                2));

            cache.Put(new AffinityKey(7, 2), new Employee(
                "Brian Peters",
                10600,
                new Address("1407 Pearlman Avenue, Boston, MA", 12110),
                new[] {"Development", "QA"},
                2));
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICache<int, Employee> cache)
        {
            cache.Put(1, new Employee(
                "James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                new[] {"Human Resources", "Customer Service"},
                1));

            cache.Put(2, new Employee(
                "Daniel Adams",
                11000,
                new Address("184 Fidler Drive, San Antonio, TX", 78130),
                new[] {"Development", "QA"},
                1));

            cache.Put(3, new Employee(
                "Cristian Moss",
                12500,
                new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                new[] {"Logistics"},
                1));

            cache.Put(4, new Employee(
                "Allison Mathis",
                25300,
                new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                new[] {"Development"},
                2));

            cache.Put(5, new Employee(
                "Breana Robbin",
                6500,
                new Address("3960 Sundown Lane, Austin, TX", 78130),
                new[] {"Sales"},
                2));

            cache.Put(6, new Employee(
                "Philip Horsley",
                19800,
                new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                new[] {"Sales"},
                2));

            cache.Put(7, new Employee(
                "Brian Peters",
                10600,
                new Address("1407 Pearlman Avenue, Boston, MA", 12110),
                new[] {"Development", "QA"},
                2));
        }
    }
}

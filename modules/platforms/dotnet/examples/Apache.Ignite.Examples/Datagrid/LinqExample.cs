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
using System.Linq;
using System.Collections.Generic;

using Apache.Ignite.Core;
using Apache.Ignite.Linq;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.ExamplesDll.Binary;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// This example populates cache with sample data and runs several LINQ queries over this data.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -IgniteHome="%IGNITE_HOME%" -springConfigUrl=platforms\dotnet\examples\config\examples-config.xml -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public class LinqExample
    {
        /// <summary>Cache name.</summary>
        private const string CacheName = "dotnet_cache_query";

        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.Start(@"platforms\dotnet\examples\config\examples-config.xml"))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache LINQ example started.");

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
                var organizationCache = ignite.GetCache<int, Organization>(CacheName);

                // Run SQL query example.
                QueryExample(employeeCache);

                // Run compiled SQL query example.
                CompiledQueryExample(employeeCache);

                // Run SQL query with join example.
                JoinQueryExample(employeeCache, organizationCache);

                // Run SQL fields query example.
                FieldsQueryExample(employeeCache);

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
        private static void QueryExample(ICache<EmployeeKey, Employee> cache)
        {
            const int zip = 94109;

            IQueryable<ICacheEntry<EmployeeKey, Employee>> qry = 
                cache.AsCacheQueryable().Where(emp => emp.Value.Address.Zip == zip);

            Console.WriteLine();
            Console.WriteLine(">>> Employees with zipcode " + zip + ":");

            foreach (ICacheEntry<EmployeeKey, Employee> entry in qry)
                Console.WriteLine(">>>    " + entry.Value);
        }

        /// <summary>
        /// Queries employees that have provided ZIP code in address with a compiled query.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void CompiledQueryExample(ICache<EmployeeKey, Employee> cache)
        {
            const int zip = 94109;

            // Compile cache query to eliminate LINQ overhead on multiple runs.
            Func<int, IQueryCursor<ICacheEntry<EmployeeKey, Employee>>> qry = 
                CompiledQuery.Compile((int z) => cache.AsCacheQueryable().Where(emp => emp.Value.Address.Zip == z));

            Console.WriteLine();
            Console.WriteLine(">>> Employees with zipcode using compiled query " + zip + ":");

            foreach (ICacheEntry<EmployeeKey, Employee> entry in qry(zip))
                Console.WriteLine(">>>    " + entry.Value);
        }

        /// <summary>
        /// Queries employees that work for organization with provided name.
        /// </summary>
        /// <param name="employeeCache">Employee cache.</param>
        /// <param name="organizationCache">Organization cache.</param>
        private static void JoinQueryExample(ICache<EmployeeKey, Employee> employeeCache, 
            ICache<int, Organization> organizationCache)
        {
            const string orgName = "Apache";

            IQueryable<ICacheEntry<EmployeeKey, Employee>> employees = employeeCache.AsCacheQueryable();
            IQueryable<ICacheEntry<int, Organization>> organizations = organizationCache.AsCacheQueryable();

            IQueryable<ICacheEntry<EmployeeKey, Employee>> qry = 
                from employee in employees
                from organization in organizations
                where employee.Key.OrganizationId == organization.Key && organization.Value.Name == orgName
                select employee;


            Console.WriteLine();
            Console.WriteLine(">>> Employees working for " + orgName + ":");

            foreach (ICacheEntry<EmployeeKey, Employee> entry in qry)
                Console.WriteLine(">>>     " + entry.Value);
        }

        /// <summary>
        /// Queries names and salaries for all employees.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void FieldsQueryExample(ICache<EmployeeKey, Employee> cache)
        {
            var qry = cache.AsCacheQueryable().Select(entry => new {entry.Value.Name, entry.Value.Salary});

            Console.WriteLine();
            Console.WriteLine(">>> Employee names and their salaries:");

            foreach (var row in qry)
                Console.WriteLine(">>>     [Name=" + row.Name + ", salary=" + row.Salary + ']');
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

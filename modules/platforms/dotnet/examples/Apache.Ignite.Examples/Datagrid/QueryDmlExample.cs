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

namespace Apache.Ignite.Examples.Datagrid
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.ExamplesDll.Binary;

    /// <summary>
    /// This example showcases DML capabilities of Ignite's SQL engine.
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
    public class QueryDmlExample
    {
        /// <summary>Organization cache name.</summary>
        private const string OrganizationCacheName = "dotnet_cache_query_dml_organization";

        /// <summary>Employee cache name.</summary>
        private const string EmployeeCacheName = "dotnet_cache_query_dml_employee";

        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
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
        /// <param name="employeeCache">Employee cache.</param>
        /// <param name="message">Message.</param>
        private static void Select(ICache<int, Employee> employeeCache, string message)
        {
            Console.WriteLine("\n>>> {0}", message);

            var qry = new SqlFieldsQuery(string.Format(
                "select emp._key, emp.name, org.name, emp.salary " +
                "from Employee as emp, " +
                "\"{0}\".Organization as org " +
                "where emp.organizationId = org._key", OrganizationCacheName));

            using (var cursor = employeeCache.QueryFields(qry))
            {
                foreach (var row in cursor)
                {
                    Console.WriteLine(">>> {0}: {1}, {2}, {3}", row[0], row[1], row[2], row[3]);
                }
            }
        }

        /// <summary>
        /// Populate cache with test data.
        /// </summary>
        /// <param name="organizationCache">Organization cache.</param>
        /// <param name="employeeCache">Employee cache.</param>
        private static void Insert(ICache<int, Organization> organizationCache, ICache<int, Employee> employeeCache)
        {
            // Insert organizations.
            var qry = new SqlFieldsQuery("insert into Organization (_key, name) values (?, ?)", 1, "ASF");
            organizationCache.QueryFields(qry);

            qry.Arguments = new object[] {2, "Eclipse"};
            organizationCache.QueryFields(qry);

            // Insert employees.
            qry = new SqlFieldsQuery("insert into Employee (_key, name, organizationId, salary) values (?, ?, ?, ?)");

            qry.Arguments = new object[] {1, "John Doe", 1, 4000};
            employeeCache.QueryFields(qry);

            qry.Arguments = new object[] {2, "Jane Roe", 1, 5000};
            employeeCache.QueryFields(qry);

            qry.Arguments = new object[] {3, "Mary Major", 2, 2000};
            employeeCache.QueryFields(qry);

            qry.Arguments = new object[] {4, "Richard Miles", 2, 3000};
            employeeCache.QueryFields(qry);
        }

        /// <summary>
        /// Conditional UPDATE query: raise salary for ASF employees.
        /// </summary>
        /// <param name="employeeCache">Employee cache.</param>
        private static void Update(ICache<int, Employee> employeeCache)
        {
            var qry = new SqlFieldsQuery("update Employee set salary = salary * 1.1 where organizationId = ?", 1);

            employeeCache.QueryFields(qry);
        }

        /// <summary>
        /// Conditional DELETE query: remove non-ASF employees.
        /// </summary>
        /// <param name="employeeCache">Employee cache.</param>
        private static void Delete(ICache<int, Employee> employeeCache)
        {
            var qry = new SqlFieldsQuery(string.Format(
                "delete from Employee where _key in (" +
                "select emp._key from Employee emp, \"{0}\".Organization org " +
                "where org.Name != ? and org._key = emp.organizationId)", OrganizationCacheName), "ASF");

            employeeCache.QueryFields(qry);
        }
    }
}

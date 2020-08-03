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

namespace Apache.Ignite.Examples.ThinClient
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.ExamplesDll.Binary;
    using Apache.Ignite.ExamplesDll.Datagrid;

    /// <summary>
    /// Demonstrates Ignite.NET "thin" client cache queries.
    /// <para />
    /// 1) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 2) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example must be run with standalone Apache Ignite node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public class ThinClientQueryExample
    {
        /// <summary> Cache name. </summary>
        private const string CacheName = "thinClientCache";

        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteClientConfiguration("127.0.0.1");

            using (IIgniteClient igniteClient = Ignition.StartClient(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache query client example started.");

                ICacheClient<int, Employee> cache = igniteClient.GetOrCreateCache<int, Employee>(CacheName);

                // Populate cache with sample data entries.
                PopulateCache(cache);

                // Run scan query example.
                ScanQueryExample(cache);
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Queries organizations of specified type.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void ScanQueryExample(ICacheClient<int, Employee> cache)
        {
            const int zip = 94109;

            var qry = cache.Query(new ScanQuery<int, Employee>(new ScanQueryFilter(zip)));

            Console.WriteLine();
            Console.WriteLine(">>> Private organizations (scan):");

            foreach (var entry in qry)
            {
                Console.WriteLine(">>>    " + entry.Value);
            }
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICacheClient<int, Employee> cache)
        {
            cache.Put(1, new Employee(
                "James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                new[] { "Human Resources", "Customer Service" },
                1));

            cache.Put(2, new Employee(
                "Daniel Adams",
                11000,
                new Address("184 Fidler Drive, San Antonio, TX", 78130),
                new[] { "Development", "QA" },
                1));

            cache.Put(3, new Employee(
                "Cristian Moss",
                12500,
                new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                new[] { "Logistics" },
                1));

            cache.Put(4, new Employee(
                "Allison Mathis",
                25300,
                new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                new[] { "Development" },
                2));

            cache.Put(5, new Employee(
                "Breana Robbin",
                6500,
                new Address("3960 Sundown Lane, Austin, TX", 78130),
                new[] { "Sales" },
                2));

            cache.Put(6, new Employee(
                "Philip Horsley",
                19800,
                new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                new[] { "Sales" },
                2));

            cache.Put(7, new Employee(
                "Brian Peters",
                10600,
                new Address("1407 Pearlman Avenue, Boston, MA", 12110),
                new[] { "Development", "QA" },
                2));
        }
    }
}

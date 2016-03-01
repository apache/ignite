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
using Apache.Ignite.Core;
using Apache.Ignite.Linq;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// This example configures uses attribute-based cache configuration, populates cache with sample data 
    /// and runs several LINQ queries over this data.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// </summary>
    public class LinqCodeConfigurationExample
    {
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.Start(GetConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache LINQ code configuration example started.");

                var cache = ignite.CreateCache<int, Person>(new CacheConfiguration("persons", typeof (Person)));

                // Populate cache with sample data entries.
                PopulateCache(cache);

                // Run SQL query example.
                QueryExample(cache);

                // Run SQL fields query example.
                FieldsQueryExample(cache);

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
        private static void QueryExample(ICache<int, Person> cache)
        {
            const int zip = 94109;

            IQueryable<ICacheEntry<int, Person>> qry = cache.AsCacheQueryable().Where(emp => emp.Value.Zip == zip);

            Console.WriteLine();
            Console.WriteLine(">>> Employees with zipcode " + zip + ":");

            foreach (ICacheEntry<int, Person> entry in qry)
                Console.WriteLine(">>>    " + entry.Value);
        }


        /// <summary>
        /// Queries names and salaries for all employees.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void FieldsQueryExample(ICache<int, Person> cache)
        {
            var qry = cache.AsCacheQueryable().Select(entry => new {entry.Value.Name, entry.Value.Street});

            Console.WriteLine();
            Console.WriteLine(">>> Employee names and their addresses:");

            foreach (var row in qry)
                Console.WriteLine(">>>     [Name=" + row.Name + ", address=" + row.Street + ']');
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICache<int, Person> cache)
        {
            cache.Put(1, new Person
            {
                Name = "James Wilson",
                Street = "1096 Eddy Street, San Francisco, CA",
                Zip = 94109
            });

            cache.Put(2, new Person
            {
                Name = "Daniel Adams",
                Street = "184 Fidler Drive, San Antonio, TX",
                Zip = 78130
            });

            cache.Put(3, new Person
            {
                Name = "Cristian Moss",
                Street = "667 Jerry Dove Drive, Florence, SC",
                Zip = 29501
            });

            cache.Put(4, new Person
            {
                Name = "Allison Mathis",
                Street = "2702 Freedom Lane, San Francisco, CA",
                Zip = 94109
            });

            cache.Put(5, new Person
            {
                Name = "Breana Robbin",
                Street = "3960 Sundown Lane, Austin, TX",
                Zip = 78130
            });

            cache.Put(6, new Person
            {
                Name = "Philip Horsley",
                Street = "2803 Elsie Drive, Sioux Falls, SD",
                Zip = 57104
            });

            cache.Put(7, new Person
            {
                Name = "Brian Peters",
                Street = "1407 Pearlman Avenue, Boston, MA",
                Zip = 12110
            });
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private static IgniteConfiguration GetConfiguration()
        {
            return new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof (Person)),
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500..47501"}
                    }
                }
            };
        }

        /// <summary>
        /// Person.
        /// </summary>
        private class Person
        {
            /// <summary>
            /// Gets or sets the name.
            /// </summary>
            [QuerySqlField]
            public string Name { get; set; }

            /// <summary>
            /// Gets or sets the street.
            /// </summary>
            [QuerySqlField]
            public string Street { get; set; }

            /// <summary>
            /// Gets or sets the zip.
            /// </summary>
            [QuerySqlField]
            public int Zip { get; set; }

            /// <summary>
            /// Returns a string that represents the current object.
            /// </summary>
            /// <returns>
            /// A string that represents the current object.
            /// </returns>
            override public string ToString()
            {
                return string.Format("{0} [name={1}, street={2}, zip={3}]", typeof(Person).Name, Name, Street, Zip);
            }
        }
    }
}

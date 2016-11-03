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
using System.Collections.Generic;
using Apache.Ignite.Core;
using Apache.Ignite.ExamplesDll.Compute;
using Apache.Ignite.ExamplesDll.Binary;

namespace Apache.Ignite.Examples.Compute
{
    /// <summary>
    /// Example demonstrating task execution.
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
    public class TaskExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Task execution example started.");

                // Generate employees to calculate average salary for.
                ICollection<Employee> employees = Employees();

                Console.WriteLine();
                Console.WriteLine(">>> Calculating average salary for employees:");

                foreach (Employee employee in employees)
                    Console.WriteLine(">>>     " + employee);

                // Execute task and get average salary.
                var avgSalary = ignite.GetCompute().Execute(new AverageSalaryTask(), employees);

                Console.WriteLine();
                Console.WriteLine(">>> Average salary for all employees: " + avgSalary);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Generates collection of employees for example.
        /// </summary>
        /// <returns>Collection of employees.</returns>
        private static ICollection<Employee> Employees()
        {
            return new []
            {
                new Employee(
                    "James Wilson",
                    12500,
                    new Address("1096 Eddy Street, San Francisco, CA", 94109),
                    new List<string> {"Human Resources", "Customer Service"}
                    ),
                new Employee(
                    "Daniel Adams",
                    11000,
                    new Address("184 Fidler Drive, San Antonio, TX", 78205),
                    new List<string> {"Development", "QA"}
                    ),
                new Employee(
                    "Cristian Moss",
                    12500,
                    new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                    new List<string> {"Logistics"}
                    ),
                new Employee(
                    "Allison Mathis",
                    25300,
                    new Address("2702 Freedom Lane, Hornitos, CA", 95325),
                    new List<string> {"Development"}
                    ),
                new Employee(
                    "Breana Robbin",
                    6500,
                    new Address("3960 Sundown Lane, Austin, TX", 78758),
                    new List<string> {"Sales"}
                    ),
                new Employee(
                    "Philip Horsley",
                    19800,
                    new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                    new List<string> {"Sales"}
                    ),
                new Employee(
                    "Brian Peters",
                    10600,
                    new Address("1407 Pearlman Avenue, Boston, MA", 02110),
                    new List<string> {"Development", "QA"}
                    ),
                new Employee(
                    "Jack Yang",
                    12900,
                    new Address("4425 Parrish Avenue Smithsons Valley, TX", 78130),
                    new List<string> {"Sales"}
                    )
            };
        }
    }
}

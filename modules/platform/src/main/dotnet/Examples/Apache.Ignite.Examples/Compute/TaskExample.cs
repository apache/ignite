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
using GridGain.Examples.Portable;

namespace GridGain.Examples.Compute
{
    /// <summary>
    /// Example demonstrating task execution.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project GridGainExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (GridGainExamples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project GridGainExamplesDll if you haven't done it yet (select it -> right-click -> Build);
    /// 2) Locate created GridGainExamplesDll.dll file (GridGainExamplesDll project -> right-click ->
    ///     Properties -> Build -> Output path);
    /// 3) Go to .Net binaries folder [GRIDGAIN_HOME]\platforms\dotnet and run GridGain.exe as follows:
    /// GridGain.exe -gridGainHome=[path_to_GRIDGAIN_HOME] -springConfigUrl=examples\config\dotnet\example-compute.xml -assembly=[path_to_GridGainExamplesDll.dll]
    /// <para />
    /// As a result you will see console jobs output on one or several nodes.
    /// </summary>
    public class TaskExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"examples\config\dotnet\example-compute.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var ignite = Ignition.Start(cfg))
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
                var avgSalary = ignite.Compute().Execute(new AverageSalaryTask(), employees);

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

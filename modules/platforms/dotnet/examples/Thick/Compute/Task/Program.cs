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

namespace Apache.Ignite.Examples.Thick.Compute.Task
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Compute;
    using Apache.Ignite.Examples.Shared.Models;

    /// <summary>
    /// This example demonstrates compute task execution.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Task execution example started.");

                // Generate employees to calculate average salary for.
                var employees = Utils.GetSampleEmployees().ToList();

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
    }
}

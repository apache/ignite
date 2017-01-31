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

namespace Apache.Ignite.ExamplesDll.Compute
{
    using System;
    using System.Collections;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.ExamplesDll.Binary;

    /// <summary>
    /// Average salary job.
    /// </summary>
    public class AverageSalaryJob : ComputeJobAdapter<Tuple<long, int>>
    {
        /// <summary> Employees. </summary>
        private readonly ArrayList _employees = new ArrayList();

        /// <summary>
        ///     Adds employee.
        /// </summary>
        /// <param name="employee">Employee.</param>
        public void Add(Employee employee)
        {
            _employees.Add(employee);
        }

        /// <summary>
        /// Execute the job.
        /// </summary>
        /// <returns>Job result: tuple with total salary in the first item and employees count in the second.</returns>
        public override Tuple<long, int> Execute()
        {
            long sum = 0;
            int count = 0;

            Console.WriteLine();
            Console.WriteLine(">>> Executing salary job for " + _employees.Count + " employee(s) ...");
            Console.WriteLine();

            foreach (Employee emp in _employees)
            {
                sum += emp.Salary;
                count++;
            }

            return new Tuple<long, int>(sum, count);
        }
    }
}

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

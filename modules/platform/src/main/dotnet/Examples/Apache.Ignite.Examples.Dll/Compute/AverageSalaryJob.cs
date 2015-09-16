/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Collections.Generic;
using GridGain.Compute;
using GridGain.Examples.Portable;

namespace GridGain.Examples.Compute
{
    /// <summary>
    /// Average salary job.
    /// </summary>
    [Serializable]
    public class AverageSalaryJob : ComputeJobAdapter<Tuple<long, int>>
    {
        /// <summary> Employees. </summary>
        private readonly ICollection<Employee> _employees = new List<Employee>();

        /// <summary>
        /// Adds employee.
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
        override public Tuple<long, int> Execute()
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

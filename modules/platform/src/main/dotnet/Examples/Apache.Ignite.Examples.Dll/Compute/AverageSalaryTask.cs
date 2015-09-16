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
using System.Linq;
using GridGain.Compute;
using GridGain.Examples.Portable;

namespace GridGain.Examples.Compute
{
    /// <summary>
    /// Average salary task.
    /// </summary>
    public class AverageSalaryTask : ComputeTaskSplitAdapter<ICollection<Employee>, Tuple<long, int>, long>
    {
        /// <summary>
        /// Split the task distributing employees between several jobs.
        /// </summary>
        /// <param name="gridSize">Number of available grid nodes.</param>
        /// <param name="arg">Task execution argument.</param>
        protected override ICollection<IComputeJob<Tuple<long, int>>> Split(int gridSize, ICollection<Employee> arg)
        {
            ICollection<Employee> employees = arg;

            var jobs = new List<IComputeJob<Tuple<long, int>>>(gridSize);

            int count = 0;

            foreach (Employee employee in employees)
            {
                int idx = count++ % gridSize;

                AverageSalaryJob job;

                if (idx >= jobs.Count)
                {
                    job = new AverageSalaryJob();

                    jobs.Add(job);
                }
                else
                    job = (AverageSalaryJob) jobs[idx];

                job.Add(employee);
            }

            return jobs;
        }

        /// <summary>
        /// Calculate average salary after all jobs are finished.
        /// </summary>
        /// <param name="results">Job results.</param>
        /// <returns>Average salary.</returns>
        public override long Reduce(IList<IComputeJobResult<Tuple<long, int>>> results)
        {
            long sum = 0;
            int count = 0;

            foreach (var t in results.Select(result => result.Data()))
            {
                sum += t.Item1;
                count += t.Item2;
            }

            return sum / count;
        }
    }
}

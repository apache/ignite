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
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.ExamplesDll.Binary;

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

            foreach (var t in results.Select(result => result.Data))
            {
                sum += t.Item1;
                count += t.Item2;
            }

            return sum / count;
        }
    }
}

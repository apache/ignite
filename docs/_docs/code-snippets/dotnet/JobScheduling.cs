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

using Apache.Ignite.Core;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Resource;

namespace dotnet_helloworld
{
    public class JobScheduling
    {
        public void Priority()
        {
            // tag::priority[]
            // PriorityQueueCollisionSpi must be configured in the Spring XML configuration file ignite-helloworld.xml
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "ignite-helloworld.xml"
            };

            // Start a node.
            using var ignite = Ignition.Start(cfg);
            // end::priority[]
        }

        // tag::task-priority[]
        // Compute tasks must be annotated with the ComputeTaskSessionFullSupport attribute to support distributing
        // the task's session attributes to compute jobs that the task creates.
        [ComputeTaskSessionFullSupport]
        public class MyUrgentTask : ComputeTaskSplitAdapter<int, bool, bool>
        {
            // Auto-injected task session.
            [TaskSessionResource] private IComputeTaskSession _taskSes;

            /// <inheritdoc />
            protected override ICollection<IComputeJob<bool>> Split(int gridSize, int arg)
            {
                // Set high task priority.
                _taskSes.SetAttribute("grid.task.priority", 10);

                var jobs = new List<IComputeJob<bool>>(gridSize);

                for (var i = 1; i <= gridSize; i++)
                {
                    jobs.Add(new MyUrgentJob());
                }

                // These jobs will be executed with higher priority.
                return jobs;
            }

            /// <inheritdoc />
            public override bool Reduce(IList<IComputeJobResult<bool>> results) => results.All(r => r.Data);
        }
        // end::task-priority[]

        private class MyUrgentJob : ComputeJobAdapter<bool>
        {
            public override bool Execute() => true;
        }
    }
}
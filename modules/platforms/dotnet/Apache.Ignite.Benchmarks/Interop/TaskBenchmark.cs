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

namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Compute task benchmark.
    /// </summary>
    internal class TaskBenchmark : PlatformBenchmarkBase
    {
        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("ExecuteEmptyTask", ExecuteEmptyTask, 1));
        }

        /// <summary>
        /// Executes task.
        /// </summary>
        private void ExecuteEmptyTask(BenchmarkState state)
        {
            Node.GetCompute().Execute(new MyEmptyTask(), "zzzz");
        }
    }

    /// <summary>
    /// Compute task.
    /// </summary>
    internal class MyEmptyTask : IComputeTask<object, object, object>
    {
        /** <inheritDoc /> */
        public IDictionary<IComputeJob<object>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
        {
            return new Dictionary<IComputeJob<object>, IClusterNode>
            {
                {new MyJob((string) arg), subgrid[0]}
            };
        }

        /** <inheritDoc /> */
        public ComputeJobResultPolicy OnResult(IComputeJobResult<object> res, IList<IComputeJobResult<object>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        /** <inheritDoc /> */
        public object Reduce(IList<IComputeJobResult<object>> results)
        {
            return results.Count == 0 ? null : results[0];
        }
    }

    /// <summary>
    /// Compute job.
    /// </summary>
    internal class MyJob : IComputeJob<object>
    {
        /** */
        private readonly string _s;

        /// <summary>
        /// Initializes a new instance of the <see cref="MyJob"/> class.
        /// </summary>
        /// <param name="s">The s.</param>
        public MyJob(string s)
        {
            _s = s;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            return _s.Length;
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            // No-op.
        }
    }
}

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

namespace GridGain.Client.Benchmark.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    ///
    /// </summary>
    internal class GridClientTaskBenchmark : GridClientAbstractInteropBenchmark
    {
        /** <inheritDoc /> */
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("ExecuteEmptyTask", ExecuteEmptyTask, 1));
        }

        /// <summary>
        /// Executes task.
        /// </summary>
        private void ExecuteEmptyTask(GridClientBenchmarkState state)
        {
            node.GetCompute().Execute(new MyEmptyTask(), "zzzz");
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class MyEmptyTask : IComputeTask<object, object, object>
    {
        /// <summary>
        ///
        /// </summary>
        /// <param name="subgrid"></param>
        /// <param name="arg"></param>
        /// <returns></returns>
        public IDictionary<IComputeJob<object>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
        {
            return new Dictionary<IComputeJob<object>, IClusterNode>
            {
                {new MyJob((string) arg), subgrid[0]}
            };
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="res"></param>
        /// <param name="rcvd"></param>
        /// <returns></returns>
        public ComputeJobResultPolicy Result(IComputeJobResult<object> res, IList<IComputeJobResult<object>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="results"></param>
        /// <returns></returns>
        public object Reduce(IList<IComputeJobResult<object>> results)
        {
            return results.Count == 0 ? null : results[0];
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class MyJob : IComputeJob<object>
    {
        /** */
        private readonly string s;

        public MyJob(string s)
        {
            this.s = s;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public object Execute()
        {
            return s.Length;
        }

        /// <summary>
        ///
        /// </summary>
        public void Cancel()
        {
            // No-op.
        }
    }
}

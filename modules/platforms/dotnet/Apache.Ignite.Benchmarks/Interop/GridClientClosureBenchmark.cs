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
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// 
    /// </summary>
    internal class GridClientClosureBenchmark : GridClientAbstractInteropBenchmark
    {
        /** <inheritDoc /> */
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("ExecuteClosureTask", ExecuteClosureTask, 1));
        }
        
        /// <summary>
        /// Executes closure.
        /// </summary>
        private void ExecuteClosureTask(GridClientBenchmarkState state)
        {
            node.GetCompute().Call(new MyClosure("zzzz"));
        }
    }

    public class MyClosure : IComputeFunc<int>
    {
        /** */
        private readonly string s;

        public MyClosure(string s)
        {
            this.s = s;
        }

        public int Invoke()
        {
            return s.Length;
        }
    }
}

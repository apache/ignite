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

namespace Apache.Ignite.Core.Impl.Compute.Closure
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Closure-based task producing only one job and thus having only single result.
    /// </summary>
    [ComputeTaskNoResultCache]
    internal class ComputeReducingClosureTask<TA, T, TR> 
        : ComputeAbstractClosureTask<TA, T, TR>, IComputeResourceInjector
    {
        /** Reducer. */
        private readonly IComputeReducer<T, TR> _rdc;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="rdc">Reducer.</param>
        public ComputeReducingClosureTask(IComputeReducer<T, TR> rdc)
        {
            _rdc = rdc;
        }

        /** <inheritDoc /> */
        protected override ComputeJobResultPolicy Result0(IComputeJobResult<T> res)
        {
            return _rdc.Collect(res.Data()) ? ComputeJobResultPolicy.Wait : ComputeJobResultPolicy.Reduce;
        }

        /** <inheritDoc /> */
        public override TR Reduce(IList<IComputeJobResult<T>> results)
        {
            return _rdc.Reduce();
        }

        /** <inheritDoc /> */
        public void Inject(Ignite grid)
        {
            ResourceProcessor.Inject(_rdc, grid);
        }
    }
}

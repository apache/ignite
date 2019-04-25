/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    using System.Diagnostics.CodeAnalysis;
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
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override ComputeJobResultPolicy Result0(IComputeJobResult<T> res)
        {
            return _rdc.Collect(res.Data) ? ComputeJobResultPolicy.Wait : ComputeJobResultPolicy.Reduce;
        }

        /** <inheritDoc /> */
        public override TR Reduce(IList<IComputeJobResult<T>> results)
        {
            return _rdc.Reduce();
        }

        /** <inheritDoc /> */
        public void Inject(IIgniteInternal grid)
        {
            ResourceProcessor.Inject(_rdc, grid);
        }
    }
}

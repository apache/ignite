/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
    internal class ComputeReducingClosureTask<A, T, R> 
        : ComputeAbstractClosureTask<A, T, R>, IComputeResourceInjector
    {
        /** Reducer. */
        private readonly IComputeReducer<T, R> rdc;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="rdc">Reducer.</param>
        public ComputeReducingClosureTask(IComputeReducer<T, R> rdc)
        {
            this.rdc = rdc;
        }

        /** <inheritDoc /> */
        protected override ComputeJobResultPolicy Result0(IComputeJobResult<T> res)
        {
            return rdc.Collect(res.Data()) ? ComputeJobResultPolicy.WAIT : ComputeJobResultPolicy.REDUCE;
        }

        /** <inheritDoc /> */
        public override R Reduce(IList<IComputeJobResult<T>> results)
        {
            return rdc.Reduce();
        }

        /** <inheritDoc /> */
        public void Inject(GridImpl grid)
        {
            ResourceProcessor.Inject(rdc, grid);
        }
    }
}

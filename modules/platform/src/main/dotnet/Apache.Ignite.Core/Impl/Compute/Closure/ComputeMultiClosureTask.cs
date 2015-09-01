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

    /// <summary>
    /// Closure-based task producing multiple jobs and returning a collection of job results.
    /// </summary>
    [ComputeTaskNoResultCache]
    internal class ComputeMultiClosureTask<A, T, R> : ComputeAbstractClosureTask<A, T, R> 
        where R : ICollection<T>
    {
        /** Result. */
        private readonly ICollection<T> res;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="size">Expected results count.</param>
        public ComputeMultiClosureTask(int size)
        {
            res = new List<T>(size);
        }

        /** <inheritDoc /> */
        protected override ComputeJobResultPolicy Result0(IComputeJobResult<T> res)
        {
            this.res.Add(res.Data());

            return ComputeJobResultPolicy.WAIT;
        }

        /** <inheritDoc /> */
        public override R Reduce(IList<IComputeJobResult<T>> results)
        {
            return (R) res;
        }
    }
}

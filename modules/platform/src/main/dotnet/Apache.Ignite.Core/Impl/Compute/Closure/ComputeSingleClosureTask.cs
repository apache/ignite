/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */


namespace GridGain.Impl.Compute
{
    using System.Collections.Generic;

    using GridGain.Compute;

    /// <summary>
    /// Closure-based task producing only one job and thus having only single result.
    /// </summary>
    [ComputeTaskNoResultCache]
    internal class ComputeSingleClosureTask<A, T, R> : ComputeAbstractClosureTask<A, T, R> where R : T
    {
        /** Result. */
        private R res;

        /** <inheritDoc /> */
        protected override ComputeJobResultPolicy Result0(IComputeJobResult<T> res)
        {
            this.res = (R) res.Data();

            // No more results are expected at this point, but we prefer not to alter regular
            // task flow.
            return ComputeJobResultPolicy.WAIT;
        }

        /** <inheritDoc /> */
        public override R Reduce(IList<IComputeJobResult<T>> results)
        {
            return res;
        }
    }
}

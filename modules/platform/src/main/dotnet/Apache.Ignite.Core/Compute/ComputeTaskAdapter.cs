/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Compute
{
    using System;
    using System.Collections.Generic;

    using GridGain.Cluster;
    using GridGain.Common;

    /// <summary>
    /// Convenience adapter for <see cref="GridGain.Compute.IComputeTask{A, T, R}"/> interface
    /// </summary>
    public abstract class ComputeTaskAdapter<A, T, R> : IComputeTask<A, T, R>
    {
        /// <summary>
        /// Default implementation which will wait for all jobs to complete before
        /// calling <see cref="GridGain.Compute.IComputeTask{A, T, R}.Reduce"/> method.
        /// <p/>
        /// If remote job resulted in exception <see cref="GridGain.Compute.IComputeJobResult{T}.Exception()"/> 
        /// is not <c>null</c>),
        /// then <see cref="GridGain.Compute.ComputeJobResultPolicy.FAILOVER"/>  policy will be returned if 
        /// the exception is instance of <see cref="ClusterTopologyException"/> 
        /// or <see cref="GridGain.Compute.ComputeExecutionRejectedException"/>, which means that
        /// remote node either failed or job execution was rejected before it got a chance to start. In all
        /// other cases the exception will be rethrown which will ultimately cause task to fail.
        /// </summary>
        /// <param name="res">Received remote grid executable result.</param>
        /// <param name="rcvd">All previously received results.</param>
        /// <returns>Result policy that dictates how to process further upcoming job results.</returns>
        public virtual ComputeJobResultPolicy Result(IComputeJobResult<T> res, IList<IComputeJobResult<T>> rcvd)
        {
            Exception err = res.Exception();

            if (err != null)
            {
                if (err is ComputeExecutionRejectedException || err is ClusterTopologyException ||
                    err is ComputeJobFailoverException)
                    return ComputeJobResultPolicy.FAILOVER;
                
                throw new IgniteException("Remote job threw user exception (override or implement IComputeTask.result(..) " +
                                        "method if you would like to have automatic failover for this exception).", err);
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /// <summary>
        /// This method is called to map or split grid task into multiple grid jobs. This is the
        /// first method that gets called when task execution starts.
        /// </summary>
        /// <param name="subgrid">Nodes available for this task execution. Note that order of nodes is
        /// guaranteed to be randomized by container. This ensures that every time you simply iterate
        /// through grid nodes, the order of nodes will be random which over time should result into
        /// all nodes being used equally.</param>
        /// <param name="arg">Task execution argument. Can be <c>null</c>. This is the same argument
        /// as the one passed into <c>ICompute.Execute()</c> methods.</param>
        /// <returns>
        /// Map of grid jobs assigned to subgrid node. If <c>null</c> or empty map is returned,
        /// exception will be thrown.
        /// </returns>
        public abstract IDictionary<IComputeJob<T>, IClusterNode> Map(IList<IClusterNode> subgrid, A arg);

        /// <summary>
        /// Reduces (or aggregates) results received so far into one compound result to be returned to
        /// caller via future.
        /// <para />
        /// Note, that if some jobs did not succeed and could not be failed over then the list of
        /// results passed into this method will include the failed results. Otherwise, failed
        /// results will not be in the list.
        /// </summary>
        /// <param name="results">Received job results. Note that if task class has
        /// <see cref="ComputeTaskNoResultCacheAttribute" /> attribute, then this list will be empty.</param>
        /// <returns>
        /// Task result constructed from results of remote executions.
        /// </returns>
        public abstract R Reduce(IList<IComputeJobResult<T>> results);
    }
}

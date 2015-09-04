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

namespace Apache.Ignite.Core.Compute
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Convenience adapter for <see cref="IComputeTask{A,T,R}"/> interface
    /// </summary>
    public abstract class ComputeTaskAdapter<TA, T, TR> : IComputeTask<TA, T, TR>
    {
        /// <summary>
        /// Default implementation which will wait for all jobs to complete before
        /// calling <see cref="IComputeTask{A,T,R}.Reduce"/> method.
        /// <p/>
        /// If remote job resulted in exception <see cref="IComputeJobResult{T}.Exception()"/> 
        /// is not <c>null</c>),
        /// then <see cref="ComputeJobResultPolicy.Failover"/>  policy will be returned if 
        /// the exception is instance of <see cref="ClusterTopologyException"/> 
        /// or <see cref="ComputeExecutionRejectedException"/>, which means that
        /// remote node either failed or job execution was rejected before it got a chance to start. In all
        /// other cases the exception will be rethrown which will ultimately cause task to fail.
        /// </summary>
        /// <param name="res">Received remote Ignite executable result.</param>
        /// <param name="rcvd">All previously received results.</param>
        /// <returns>Result policy that dictates how to process further upcoming job results.</returns>
        public virtual ComputeJobResultPolicy Result(IComputeJobResult<T> res, IList<IComputeJobResult<T>> rcvd)
        {
            Exception err = res.Exception();

            if (err != null)
            {
                if (err is ComputeExecutionRejectedException || err is ClusterTopologyException ||
                    err is ComputeJobFailoverException)
                    return ComputeJobResultPolicy.Failover;
                
                throw new IgniteException("Remote job threw user exception (override or implement IComputeTask.result(..) " +
                                        "method if you would like to have automatic failover for this exception).", err);
            }

            return ComputeJobResultPolicy.Wait;
        }

        /// <summary>
        /// This method is called to map or split Ignite task into multiple Ignite jobs. This is the
        /// first method that gets called when task execution starts.
        /// </summary>
        /// <param name="subgrid">Nodes available for this task execution. Note that order of nodes is
        /// guaranteed to be randomized by container. This ensures that every time you simply iterate
        /// through Ignite nodes, the order of nodes will be random which over time should result into
        /// all nodes being used equally.</param>
        /// <param name="arg">Task execution argument. Can be <c>null</c>. This is the same argument
        /// as the one passed into <c>ICompute.Execute()</c> methods.</param>
        /// <returns>
        /// Map of Ignite jobs assigned to subgrid node. If <c>null</c> or empty map is returned,
        /// exception will be thrown.
        /// </returns>
        public abstract IDictionary<IComputeJob<T>, IClusterNode> Map(IList<IClusterNode> subgrid, TA arg);

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
        public abstract TR Reduce(IList<IComputeJobResult<T>> results);
    }
}

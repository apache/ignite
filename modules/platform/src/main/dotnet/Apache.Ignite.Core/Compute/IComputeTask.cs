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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Ignite task interface defines a task that can be executed on the grid. Ignite task
    /// is responsible for splitting business logic into multiple Ignite jobs, receiving
    /// results from individual Ignite jobs executing on remote nodes, and reducing
    /// (aggregating) received jobs' results into final Ignite task result.
    /// <para />
    /// Upon request to execute a task, the system will do the following:
    /// <list type="bullet">
    ///     <item>
    ///         <description>Inject annotated resources into task instance.</description>
    ///     </item>
    ///     <item>
    ///         <description>Apply <see cref="IComputeTask{A,T,R}.Map(IList{IClusterNode}, TA)"/>.
    ///         This method is responsible for splitting business logic into multiple jobs 
    ///         (units of execution) and mapping them to Ignite nodes.</description>
    ///     </item>
    ///     <item>
    ///         <description>System will send mapped Ignite jobs to their respective nodes.</description>
    ///     </item>
    ///     <item>
    ///         <description>Once job execution results become available method 
    ///         <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
    ///         will be called for ech received job result. The policy returned by this method will
    ///         determine the way task reacts to every job result.
    ///         <para />
    ///         If <see cref="ComputeJobResultPolicy.Wait"/> is returned, task will continue to wait
    ///         for other job results. If this result is the last job result, then reduce phase will be
    ///         started.
    ///         <para />
    ///         If <see cref="ComputeJobResultPolicy.Reduce"/> is returned, reduce phase will be started
    ///         right away without waiting for other jobs completion (all remaining jobs will receive cancel 
    ///         request).
    ///         <para />
    ///         If <see cref="ComputeJobResultPolicy.Failover"/> is returned, job will be failed over to 
    ///         another node for execution. Note that if you use <see cref="ComputeTaskAdapter{A,T,R}"/>, it will
    ///         automatically fail jobs to another node for 2 well-known failure cases: 1) job has failed to due
    ///         to node crash (in this case <see cref="IComputeJobResult{T}.Exception()"/> will return 
    ///         <see cref="ClusterTopologyException"/>); 2) job execution was rejected, i.e. remote node 
    ///         has cancelled job before it got a chance to execute, while it still was on the waiting list. 
    ///         (in this case <see cref="IComputeJobResult{T}.Exception()"/> will return 
    ///         <see cref="ComputeExecutionRejectedException"/>).
    ///         </description>
    ///     </item>
    ///     <item>
    ///         <description>Once all results are received or 
    ///         <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
    ///         method returned <see cref="ComputeJobResultPolicy.Reduce"/> policy, method 
    ///         <see cref="IComputeTask{A,T,R}.Reduce(IList{IComputeJobResult{T}})"/>
    ///         is called to aggregate received results into one final result. Once this method is finished the 
    ///         execution of the Ignite task is complete. This result will be returned to the user through future.
    ///         </description>    
    ///     </item>
    /// </list>
    /// </summary>
    /// <typeparam name="TA">Argument type.</typeparam>
    /// <typeparam name="T">Type of job result.</typeparam>
    /// <typeparam name="TR">Type of reduce result.</typeparam>
    public interface IComputeTask<in TA, T, out TR>
    {
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
        /// <returns>Map of Ignite jobs assigned to subgrid node. If <c>null</c> or empty map is returned,
        /// exception will be thrown.</returns>
        IDictionary<IComputeJob<T>, IClusterNode> Map(IList<IClusterNode> subgrid, TA arg);

        /// <summary>
        /// Asynchronous callback invoked every time a result from remote execution is
        /// received. It is ultimately upto this method to return a policy based
        /// on which the system will either wait for more results, reduce results
        /// received so far, or failover this job to another node. See 
        /// <see cref="ComputeJobResultPolicy"/> for more information.
        /// </summary>
        /// <param name="res">Received remote Ignite executable result.</param>
        /// <param name="rcvd">All previously received results. Note that if task class has
        /// <see cref="ComputeTaskNoResultCacheAttribute"/> attribute, then this list will be empty.</param>
        /// <returns>Result policy that dictates how to process further upcoming job results.</returns>
        ComputeJobResultPolicy Result(IComputeJobResult<T> res, IList<IComputeJobResult<T>> rcvd);

        /// <summary>
        /// Reduces (or aggregates) results received so far into one compound result to be returned to 
        /// caller via future.
        /// <para />
        /// Note, that if some jobs did not succeed and could not be failed over then the list of
        /// results passed into this method will include the failed results. Otherwise, failed
        /// results will not be in the list.
        /// </summary>
        /// <param name="results">Received job results. Note that if task class has 
        /// <see cref="ComputeTaskNoResultCacheAttribute"/> attribute, then this list will be empty.</param>
        /// <returns>Task result constructed from results of remote executions.</returns>
        TR Reduce(IList<IComputeJobResult<T>> results);
    }

    /// <summary>
    /// IComputeTask without an argument.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface IComputeTask<T, out TR> : IComputeTask<object, T, TR>
    {
        // No-op.
    }
}

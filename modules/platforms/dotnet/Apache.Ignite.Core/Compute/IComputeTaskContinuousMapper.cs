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
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Defines a mapper for submitting jobs asynchronously. Useful for streaming jobs within the same task.
    /// If the number of jobs grows excessively large, it's advisable to utilize the
    /// <see cref="ComputeTaskNoResultCacheAttribute"/> to prevent infinite growth of job results and siblings.
    /// <para>
    /// Continuous mapper methods become usable immediately after the mapper is injected into a task. However,
    /// they cannot be utilized after the <see cref="IComputeTask{TArg,TJobRes,TRes}.OnResult"/> method returns the
    /// <see cref="ComputeJobResultPolicy.Reduce"/> policy. Also, if the
    /// <see cref="IComputeTask{TArg,TJobRes,TRes}.OnResult"/> method returns the
    /// <see cref="ComputeJobResultPolicy.Wait"/> policy and all jobs complete, the task transitions to reducing
    /// results, making further use of the continuous mapper impossible.
    /// </para>
    /// <para>
    /// When using a continuous mapper, the <see cref="IComputeTask{TArg,TJobRes,TRes}.Map"/> method may safely
    /// return <c>null</c>, assuming at least one job has already been submitted prior to its completion.
    /// </para>
    /// <para>
    /// The continuous mapper can be injected into tasks via dependency injection (IoC). Attach the
    /// <see cref="Apache.Ignite.Core.Resource.TaskContinuousMapperResourceAttribute"/> to either a field or a
    /// setter method within any <see cref="IComputeTask{TArg,TJobRes,TRes}"/> implementation like this:
    /// <code>
    /// ...
    /// // This field will be injected with task continuous mapper
    /// [TaskContinuousMapperResource]
    /// private IComputeTaskContinuousMapper mapper;
    /// ...
    /// </code>
    /// </para>
    /// </summary>
    public interface IComputeTaskContinuousMapper
    {
        /// <summary>
        /// Sends a job to specified node in the grid.
        /// </summary>
        void Send<TRes>(IComputeJob<TRes> job, IClusterNode node);
        
        /// <summary>
        /// Sends a collection of jobs to assigned nodes.
        /// </summary>
        void Send<TRes>(IDictionary<IComputeJob<TRes>, IClusterNode> mappedJobs);
        
        /// <summary>
        /// Sends a job using the grid's load balancer.
        /// </summary>
        void Send<TRes>(IComputeJob<TRes> job);
        
        /// <summary>
        /// Sends a collection of jobs using the grid's load balancer.
        /// </summary>
        void Send<TRes>(ICollection<IComputeJob<TRes>> jobs);
    }
}
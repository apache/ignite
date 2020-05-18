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

namespace Apache.Ignite.Core.Client.Compute
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Client Compute API. See <see cref="IIgniteClient.GetCompute"/>
    /// and <see cref="IClientClusterGroup.GetCompute"/>.
    /// </summary>
    public interface IComputeClient
    {
        /// <summary>
        /// Executes Java task by class name.
        /// </summary>
        /// <param name="taskName">Java task name.</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TRes">Type of task result.</typeparam>
        TRes ExecuteJavaTask<TRes>(string taskName, object taskArg);

        /// <summary>
        /// Executes Java task by class name.
        /// </summary>
        /// <param name="taskName">Java task name.</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TRes">Type of task result.</typeparam>
        Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg);

        /// <summary>
        /// Executes Java task by class name.
        /// </summary>
        /// <param name="taskName">Java task name.</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TRes">Type of task result.</typeparam>
        Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg, CancellationToken cancellationToken);

        /// <summary>
        /// Returns a new instance of <see cref="IComputeClient"/> with a timeout for all task executions.
        /// </summary>
        /// <param name="timeout">Timeout.</param>
        /// <returns>New Compute instance with timeout.</returns>
        IComputeClient WithTimeout(TimeSpan timeout);
        
        /// <summary>
        /// Returns a new instance of <see cref="IComputeClient"/> with disabled failover.
        /// When failover is disabled, compute jobs won't be retried in case of node crashes.
        /// </summary>
        /// <returns>New Compute instance with disabled failover.</returns>
        IComputeClient WithNoFailover();
        
        /// <summary>
        /// Returns a new instance of <see cref="IComputeClient"/> with disabled result cache.
        /// </summary>
        /// <returns>New Compute instance with disabled result cache.</returns>
        IComputeClient WithNoResultCache();
        
        /// <summary>
        /// Returns a new instance of <see cref="IComputeClient"/> with binary mode enabled:
        /// Java task argument (on server side) and result (on client side) won't be deserialized.
        /// </summary>
        /// <returns>New Compute instance with binary mode enabled.</returns>
        IComputeClient WithKeepBinary();
    }
}
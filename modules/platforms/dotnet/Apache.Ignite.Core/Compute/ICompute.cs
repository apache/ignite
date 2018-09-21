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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Defines Ignite functionality for executing tasks and closures over nodes
    /// in the <see cref="IClusterGroup"/>. Instance of <see cref="ICompute"/>
    /// is obtained from grid projection using <see cref="IClusterGroup.GetCompute"/> method.
    /// <para />
    /// Note that if attempt is made to execute a computation over an empty projection (i.e. projection that does
    /// not have any alive nodes), <c>ClusterGroupEmptyException</c> will be thrown out of result task.
    /// <para />
    /// Ignite must select a node for a computation to be executed. The node will be selected based on the
    /// underlying <c>GridLoadBalancingSpi</c>, which by default sequentially picks next available node from
    /// grid projection. Other load balancing policies, such as <c>random</c> or <c>adaptive</c>, can be
    /// configured as well by selecting different load balancing SPI in Ignite configuration. If your logic requires
    /// some custom load balancing behavior, consider implementing <c>ComputeTask</c> in Java directly.
    /// <para />
    /// Ignite guarantees that as long as there is at least one Ignite node standing, every job will be
    /// executed. Jobs will automatically failover to another node if a remote node crashed or has rejected
    /// execution due to lack of resources. By default, in case of failover, next load balanced node will be
    /// picked for job execution. Also jobs will never be re-routed to the nodes they have failed on. This
    /// behavior can be changed by configuring any of the existing or a custom <c>FailoverSpi</c> in Ignite
    /// configuration.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface ICompute
    {
        /// <summary>
        /// Grid projection to which this compute instance belongs.
        /// </summary>
        IClusterGroup ClusterGroup { get; }

        /// <summary>
        /// Sets no-failover flag for the next executed task on this projection in the current thread.
        /// If flag is set, job will be never failed over even if remote node crashes or rejects execution.
        /// When task starts execution, the no-failover flag is reset, so all other task will use default
        /// failover policy, unless this flag is set again.
        /// </summary>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithNoFailover();

        /// <summary>
        /// Disables caching for the next executed task in the current thread.
        /// </summary>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithNoResultCache();

        /// <summary>
        /// Sets task timeout for the next executed task on this projection in the current thread.
        /// When task starts execution, the timeout is reset, so one timeout is used only once.
        /// </summary>
        /// <param name="timeout">Computation timeout in milliseconds.</param>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithTimeout(long timeout);

        /// <summary>
        /// Sets keep-binary flag for the next executed Java task on this projection in the current
        /// thread so that task argument passed to Java and returned task results will not be
        /// deserialized.
        /// </summary>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithKeepBinary();

        /// <summary>
        /// Executes given Java task on the grid projection. If task for given name has not been deployed yet,
        /// then 'taskName' will be used as task class name to auto-deploy the task.
        /// </summary>
        /// <param name="taskName">Java task name</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TRes">Type of task result.</typeparam>
        TRes ExecuteJavaTask<TRes>(string taskName, object taskArg);

        /// <summary>
        /// Executes given Java task on the grid projection. If task for given name has not been deployed yet,
        /// then 'taskName' will be used as task class name to auto-deploy the task.
        /// </summary>
        /// <param name="taskName">Java task name</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TRes">Type of task result.</typeparam>
        Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg);

        /// <summary>
        /// Executes given Java task on the grid projection. If task for given name has not been deployed yet,
        /// then 'taskName' will be used as task class name to auto-deploy the task.
        /// </summary>
        /// <typeparam name="TRes">Type of task result.</typeparam>
        /// <param name="taskName">Java task name</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Task result.
        /// </returns>
        Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg, CancellationToken cancellationToken);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TArg">Argument type.</typeparam>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of final task result.</typeparam>
        TRes Execute<TArg, TJobRes, TRes>(IComputeTask<TArg, TJobRes, TRes> task, TArg taskArg);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TArg">Argument type.</typeparam>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of final task result.</typeparam>
        Task<TRes> ExecuteAsync<TArg, TJobRes, TRes>(IComputeTask<TArg, TJobRes, TRes> task, TArg taskArg);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}" /> documentation.
        /// </summary>
        /// <typeparam name="TArg">Argument type.</typeparam>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of final task result.</typeparam>
        /// <param name="task">Task to execute.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Task result.
        /// </returns>
        Task<TRes> ExecuteAsync<TArg, TJobRes, TRes>(IComputeTask<TArg, TJobRes, TRes> task, TArg taskArg, 
            CancellationToken cancellationToken);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        TRes Execute<TJobRes, TRes>(IComputeTask<TJobRes, TRes> task);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        Task<TRes> ExecuteAsync<TJobRes, TRes>(IComputeTask<TJobRes, TRes> task);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}" /> documentation.
        /// </summary>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        /// <param name="task">Task to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Task result.
        /// </returns>
        Task<TRes> ExecuteAsync<TJobRes, TRes>(IComputeTask<TJobRes, TRes> task, CancellationToken cancellationToken);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TArg">Argument type.</typeparam>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        TRes Execute<TArg, TJobRes, TRes>(Type taskType, TArg taskArg);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TArg">Argument type.</typeparam>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        Task<TRes> ExecuteAsync<TArg, TJobRes, TRes>(Type taskType, TArg taskArg);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}" /> documentation.
        /// </summary>
        /// <typeparam name="TArg">Argument type.</typeparam>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        /// <param name="taskType">Task type.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Task result.
        /// </returns>
        Task<TRes> ExecuteAsync<TArg, TJobRes, TRes>(Type taskType, TArg taskArg, CancellationToken cancellationToken);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        TRes Execute<TJobRes, TRes>(Type taskType);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        Task<TRes> ExecuteAsync<TJobRes, TRes>(Type taskType);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}" /> documentation.
        /// </summary>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        /// <typeparam name="TRes">Type of reduce result.</typeparam>
        /// <param name="taskType">Task type.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Task result.
        /// </returns>
        Task<TRes> ExecuteAsync<TJobRes, TRes>(Type taskType, CancellationToken cancellationToken);

        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// </summary>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        TRes Call<TRes>(IComputeFunc<TRes> clo);

        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// </summary>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        Task<TRes> CallAsync<TRes>(IComputeFunc<TRes> clo);

        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="clo">Job to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        Task<TRes> CallAsync<TRes>(IComputeFunc<TRes> clo, CancellationToken cancellationToken);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        TRes AffinityCall<TRes>(string cacheName, object affinityKey, IComputeFunc<TRes> clo);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        Task<TRes> AffinityCallAsync<TRes>(string cacheName, object affinityKey, IComputeFunc<TRes> clo);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="clo">Job to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        Task<TRes> AffinityCallAsync<TRes>(string cacheName, object affinityKey, IComputeFunc<TRes> clo, 
            CancellationToken cancellationToken);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <param name="reducer">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Reduced job result for this execution.</returns>
        /// <typeparam name="TFuncRes">Type of function result.</typeparam>
        /// <typeparam name="TRes">Type of result after reduce.</typeparam>
        TRes Call<TFuncRes, TRes>(IEnumerable<IComputeFunc<TFuncRes>> clos, IComputeReducer<TFuncRes, TRes> reducer);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <param name="reducer">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Reduced job result for this execution.</returns>
        /// <typeparam name="TFuncRes">Type of function result.</typeparam>
        /// <typeparam name="TRes">Type of result after reduce.</typeparam>
        Task<TRes> CallAsync<TFuncRes, TRes>(IEnumerable<IComputeFunc<TFuncRes>> clos, 
            IComputeReducer<TFuncRes, TRes> reducer);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <typeparam name="TFuncRes">Type of function result.</typeparam>
        /// <typeparam name="TRes">Type of result after reduce.</typeparam>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <param name="reducer">Reducer to reduce all job results into one individual return value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Reduced job result for this execution.
        /// </returns>
        Task<TRes> CallAsync<TFuncRes, TRes>(IEnumerable<IComputeFunc<TFuncRes>> clos, 
            IComputeReducer<TFuncRes, TRes> reducer, CancellationToken cancellationToken);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <returns>Collection of job results for this execution.</returns>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        ICollection<TRes> Call<TRes>(IEnumerable<IComputeFunc<TRes>> clos);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <returns>Collection of job results for this execution.</returns>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        Task<ICollection<TRes>> CallAsync<TRes>(IEnumerable<IComputeFunc<TRes>> clos);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Collection of job results for this execution.
        /// </returns>
        Task<ICollection<TRes>> CallAsync<TRes>(IEnumerable<IComputeFunc<TRes>> clos, 
            CancellationToken cancellationToken);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <returns>Collection of results for this execution.</returns>
        ICollection<TRes> Broadcast<TRes>(IComputeFunc<TRes> clo);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <returns>Collection of results for this execution.</returns>
        Task<ICollection<TRes>> BroadcastAsync<TRes>(IComputeFunc<TRes> clo);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result.
        /// </summary>
        /// <typeparam name="TRes">The type of the resource.</typeparam>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Collection of results for this execution.
        /// </returns>
        Task<ICollection<TRes>> BroadcastAsync<TRes>(IComputeFunc<TRes> clo, CancellationToken cancellationToken);

        /// <summary>
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection.
        /// Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <param name="arg">Job closure argument.</param>
        /// <returns>Collection of results for this execution.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        ICollection<TRes> Broadcast<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg);

        /// <summary>
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection.
        /// Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <param name="arg">Job closure argument.</param>
        /// <returns>Collection of results for this execution.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        Task<ICollection<TRes>> BroadcastAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg);

        /// <summary>
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection.
        /// Every participating node will return a job result.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <param name="arg">Job closure argument.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Collection of results for this execution.
        /// </returns>
        Task<ICollection<TRes>> BroadcastAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg, 
            CancellationToken cancellationToken);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection.
        /// </summary>
        /// <param name="action">Job to broadcast to all projection nodes.</param>
        void Broadcast(IComputeAction action);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection.
        /// </summary>
        /// <param name="action">Job to broadcast to all projection nodes.</param>
        Task BroadcastAsync(IComputeAction action);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection.
        /// </summary>
        /// <param name="action">Job to broadcast to all projection nodes.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        Task BroadcastAsync(IComputeAction action, CancellationToken cancellationToken);

        /// <summary>
        /// Executes provided job on a node in this grid projection.
        /// </summary>
        /// <param name="action">Job to execute.</param>
        void Run(IComputeAction action);

        /// <summary>
        /// Executes provided job on a node in this grid projection.
        /// </summary>
        /// <param name="action">Job to execute.</param>
        Task RunAsync(IComputeAction action);

        /// <summary>
        /// Executes provided job on a node in this grid projection.
        /// </summary>
        /// <param name="action">Job to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        Task RunAsync(IComputeAction action, CancellationToken cancellationToken);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="action">Job to execute.</param>
        void AffinityRun(string cacheName, object affinityKey, IComputeAction action);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="action">Job to execute.</param>
        Task AffinityRunAsync(string cacheName, object affinityKey, IComputeAction action);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="action">Job to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        Task AffinityRunAsync(string cacheName, object affinityKey, IComputeAction action, 
            CancellationToken cancellationToken);

        /// <summary>
        /// Executes collection of jobs on Ignite nodes within this grid projection.
        /// </summary>
        /// <param name="actions">Jobs to execute.</param>
        void Run(IEnumerable<IComputeAction> actions);

        /// <summary>
        /// Executes collection of jobs on Ignite nodes within this grid projection.
        /// </summary>
        /// <param name="actions">Jobs to execute.</param>
        Task RunAsync(IEnumerable<IComputeAction> actions);

        /// <summary>
        /// Executes collection of jobs on Ignite nodes within this grid projection.
        /// </summary>
        /// <param name="actions">Jobs to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        Task RunAsync(IEnumerable<IComputeAction> actions, CancellationToken cancellationToken);

        /// <summary>
        /// Executes provided closure job on a node in this grid projection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="arg">Job argument.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        TRes Apply<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg);

        /// <summary>
        /// Executes provided closure job on a node in this grid projection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="arg">Job argument.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        Task<TRes> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg);

        /// <summary>
        /// Executes provided closure job on a node in this grid projection.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="clo">Job to run.</param>
        /// <param name="arg">Job argument.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Job result for this execution.
        /// </returns>
        Task<TRes> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg, CancellationToken cancellationToken);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <returns>Collection of job results.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        ICollection<TRes> Apply<TArg, TRes>(IComputeFunc<TArg, TRes> clo, IEnumerable<TArg> args);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <returns>Collection of job results.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        Task<ICollection<TRes>> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, IEnumerable<TArg> args);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TRes">Type of job result.</typeparam>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Collection of job results.
        /// </returns>
        Task<ICollection<TRes>> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, IEnumerable<TArg> args,
            CancellationToken cancellationToken);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="rdc">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Reduced job result for this execution.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of function result.</typeparam>
        /// <typeparam name="TRes">Type of result after reduce.</typeparam>
        TRes Apply<TArg, TFuncRes, TRes>(IComputeFunc<TArg, TFuncRes> clo, IEnumerable<TArg> args, 
            IComputeReducer<TFuncRes, TRes> rdc);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="rdc">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Reduced job result for this execution.</returns>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of function result.</typeparam>
        /// <typeparam name="TRes">Type of result after reduce.</typeparam>
        Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(IComputeFunc<TArg, TFuncRes> clo, IEnumerable<TArg> args, 
            IComputeReducer<TFuncRes, TRes> rdc);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// </summary>
        /// <typeparam name="TArg">Type of argument.</typeparam>
        /// <typeparam name="TFuncRes">Type of function result.</typeparam>
        /// <typeparam name="TRes">Type of result after reduce.</typeparam>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="rdc">Reducer to reduce all job results into one individual return value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Reduced job result for this execution.
        /// </returns>
        Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(IComputeFunc<TArg, TFuncRes> clo, IEnumerable<TArg> args, 
            IComputeReducer<TFuncRes, TRes> rdc, CancellationToken cancellationToken);
    }
}

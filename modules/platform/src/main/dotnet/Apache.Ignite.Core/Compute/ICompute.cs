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
    /// Defines Ignite functionality for executing tasks and closures over nodes
    /// in the <see cref="IClusterGroup"/>. Instance of <see cref="ICompute"/>
    /// is obtained from grid projection using <see cref="IClusterGroup.Compute()"/> method.
    /// <para />
    /// Note that if attempt is made to execute a computation over an empty projection (i.e. projection that does
    /// not have any alive nodes), <c>ClusterGroupEmptyException</c> will be thrown out of result future.
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
    public interface ICompute : IAsyncSupport<ICompute>
    {
        /// <summary>
        /// Grid projection to which this compute instance belongs.
        /// </summary>
        IClusterGroup ClusterGroup
        {
            get;
        }

        /// <summary>
        /// Sets no-failover flag for the next executed task on this projection in the current thread.
        /// If flag is set, job will be never failed over even if remote node crashes or rejects execution.
        /// When task starts execution, the no-failover flag is reset, so all other task will use default
        /// failover policy, unless this flag is set again.
        /// </summary>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithNoFailover();

        /// <summary>
        /// Sets task timeout for the next executed task on this projection in the current thread.
        /// When task starts execution, the timeout is reset, so one timeout is used only once.
        /// </summary>
        /// <param name="timeout">Computation timeout in milliseconds.</param>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithTimeout(long timeout);

        /// <summary>
        /// Sets keep-portable flag for the next executed Java task on this projection in the current
        /// thread so that task argument passed to Java and returned task results will not be
        /// deserialized.
        /// </summary>
        /// <returns>This compute instance for chaining calls.</returns>
        ICompute WithKeepPortable();

        /// <summary>
        /// Executes given Java task on the grid projection. If task for given name has not been deployed yet,
        /// then 'taskName' will be used as task class name to auto-deploy the task.
        /// </summary>
        /// <param name="taskName">Java task name</param>
        /// <param name="taskArg">Optional argument of task execution, can be null.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="T">Type of task result.</typeparam>
        T ExecuteJavaTask<T>(string taskName, object taskArg);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TA">Argument type.</typeparam>
        /// <typeparam name="T">Type of job result.</typeparam>
        /// <typeparam name="TR">Type of reduce result.</typeparam>
        [AsyncSupported]
        TR Execute<TA, T, TR>(IComputeTask<TA, T, TR> task, TA taskArg);
        
        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="T">Type of job result.</typeparam>
        /// <typeparam name="TR">Type of reduce result.</typeparam>
        [AsyncSupported]
        TR Execute<T, TR>(IComputeTask<T, TR> task);

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="TA">Argument type.</typeparam>
        /// <typeparam name="T">Type of job result.</typeparam>
        /// <typeparam name="TR">Type of reduce result.</typeparam>
        [AsyncSupported]
        TR Execute<TA, T, TR>(Type taskType, TA taskArg);
        
        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <returns>Task result.</returns>
        /// <typeparam name="T">Type of job result.</typeparam>
        /// <typeparam name="TR">Type of reduce result.</typeparam>
        [AsyncSupported]
        TR Execute<T, TR>(Type taskType);

        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// </summary>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TR">Type of job result.</typeparam>
        [AsyncSupported]
        TR Call<TR>(IComputeFunc<TR> clo);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located 
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TR">Type of job result.</typeparam>
        [AsyncSupported]
        TR AffinityCall<TR>(string cacheName, object affinityKey, IComputeFunc<TR> clo);

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <param name="rdc">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Reduced job result for this execution.</returns>
        /// <typeparam name="TR1">Type of job result.</typeparam>
        /// <typeparam name="TR2">Type of reduced result.</typeparam>
        [AsyncSupported]
        TR2 Call<TR1, TR2>(IEnumerable<IComputeFunc<TR1>> clos, IComputeReducer<TR1, TR2> rdc);
        
        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <returns>Collection of job results for this execution.</returns>
        /// <typeparam name="TR">Type of job result.</typeparam>
        [AsyncSupported]
        ICollection<TR> Call<TR>(IEnumerable<IComputeFunc<TR>> clos);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result. 
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <returns>Collection of results for this execution.</returns>
        [AsyncSupported]
        ICollection<TR> Broadcast<TR>(IComputeFunc<TR> clo);

        /// <summary>
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection.
        /// Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <param name="arg">Job closure argument.</param>
        /// <returns>Collection of results for this execution.</returns>
        /// <typeparam name="T">Type of argument.</typeparam>
        /// <typeparam name="TR">Type of job result.</typeparam>
        [AsyncSupported]
        ICollection<TR> Broadcast<T, TR>(IComputeFunc<T, TR> clo, T arg);

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection.
        /// </summary>
        /// <param name="action">Job to broadcast to all projection nodes.</param>
        [AsyncSupported]
        void Broadcast(IComputeAction action);

        /// <summary>
        /// Executes provided job on a node in this grid projection.
        /// </summary>
        /// <param name="action">Job to execute.</param>
        [AsyncSupported]
        void Run(IComputeAction action);

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="action">Job to execute.</param>
        [AsyncSupported]
        void AffinityRun(string cacheName, object affinityKey, IComputeAction action);

        /// <summary>
        /// Executes collection of jobs on Ignite nodes within this grid projection.
        /// </summary>
        /// <param name="actions">Jobs to execute.</param>
        [AsyncSupported]
        void Run(IEnumerable<IComputeAction> actions);

        /// <summary>
        /// Executes provided closure job on a node in this grid projection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="arg">Job argument.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="T">Type of argument.</typeparam>
        /// <typeparam name="TR">Type of job result.</typeparam>
        [AsyncSupported]
        TR Apply<T, TR>(IComputeFunc<T, TR> clo, T arg);

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <returns>Сollection of job results.</returns>
        /// <typeparam name="T">Type of argument.</typeparam>
        /// <typeparam name="TR">Type of job result.</typeparam>
        [AsyncSupported]
        ICollection<TR> Apply<T, TR>(IComputeFunc<T, TR> clo, IEnumerable<T> args);

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
        /// <typeparam name="T">Type of argument.</typeparam>
        /// <typeparam name="TR1">Type of job result.</typeparam>
        /// <typeparam name="TR2">Type of reduced result.</typeparam>
        [AsyncSupported]
        TR2 Apply<T, TR1, TR2>(IComputeFunc<T, TR1> clo, IEnumerable<T> args, IComputeReducer<TR1, TR2> rdc);
    }
}

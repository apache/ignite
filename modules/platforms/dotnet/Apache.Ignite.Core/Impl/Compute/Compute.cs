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

namespace Apache.Ignite.Core.Impl.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Synchronous Compute facade.
    /// </summary>
    internal class Compute : ICompute
    {
        /** */
        private readonly ComputeImpl _compute;

        /// <summary>
        /// Initializes a new instance of the <see cref="Compute"/> class.
        /// </summary>
        /// <param name="computeImpl">The compute implementation.</param>
        public Compute(ComputeImpl computeImpl)
        {
            Debug.Assert(computeImpl != null);

            _compute = computeImpl;
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _compute.ClusterGroup; }
        }

        /** <inheritDoc /> */
        public ICompute WithNoFailover()
        {
            _compute.WithNoFailover();

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithNoResultCache()
        {
            _compute.WithNoResultCache();

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithTimeout(long timeout)
        {
            _compute.WithTimeout(timeout);

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithKeepBinary()
        {
            _compute.WithKeepBinary();

            return this;
        }

        /** <inheritDoc /> */
        public TReduceRes ExecuteJavaTask<TReduceRes>(string taskName, object taskArg)
        {
            return _compute.ExecuteJavaTask<TReduceRes>(taskName, taskArg);
        }

        /** <inheritDoc /> */
        public Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg)
        {
            return _compute.ExecuteJavaTaskAsync<TRes>(taskName, taskArg).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg,
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.ExecuteJavaTaskAsync<TRes>(taskName, taskArg).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TReduceRes Execute<TArg, TJobRes, TReduceRes>(IComputeTask<TArg, TJobRes, TReduceRes> task, TArg taskArg)
        {
            return _compute.Execute(task, taskArg).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> ExecuteAsync<TArg, TJobRes, TRes>(IComputeTask<TArg, TJobRes, TRes> task, TArg taskArg)
        {
            return _compute.Execute(task, taskArg).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> ExecuteAsync<TArg, TJobRes, TRes>(IComputeTask<TArg, TJobRes, TRes> task, TArg taskArg, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.Execute(task, taskArg).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TJobRes Execute<TArg, TJobRes>(IComputeTask<TArg, TJobRes> task)
        {
            return _compute.Execute(task, null).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> ExecuteAsync<TJobRes, TRes>(IComputeTask<TJobRes, TRes> task)
        {
            return _compute.Execute(task, null).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> ExecuteAsync<TJobRes, TRes>(IComputeTask<TJobRes, TRes> task, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.Execute(task, null).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TReduceRes Execute<TArg, TJobRes, TReduceRes>(Type taskType, TArg taskArg)
        {
            return _compute.Execute<TArg, TJobRes, TReduceRes>(taskType, taskArg).Get();
        }

        /** <inheritDoc /> */
        public Task<TReduceRes> ExecuteAsync<TArg, TJobRes, TReduceRes>(Type taskType, TArg taskArg)
        {
            return _compute.Execute<TArg, TJobRes, TReduceRes>(taskType, taskArg).Task;
        }

        /** <inheritDoc /> */
        public Task<TReduceRes> ExecuteAsync<TArg, TJobRes, TReduceRes>(Type taskType, TArg taskArg, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TReduceRes>(cancellationToken) ??
                _compute.Execute<TArg, TJobRes, TReduceRes>(taskType, taskArg).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TReduceRes Execute<TArg, TReduceRes>(Type taskType)
        {
            return _compute.Execute<object, TArg, TReduceRes>(taskType, null).Get();
        }

        /** <inheritDoc /> */
        public Task<TReduceRes> ExecuteAsync<TArg, TReduceRes>(Type taskType)
        {
            return _compute.Execute<object, TArg, TReduceRes>(taskType, null).Task;
        }

        /** <inheritDoc /> */
        public Task<TReduceRes> ExecuteAsync<TArg, TReduceRes>(Type taskType, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TReduceRes>(cancellationToken) ??
                _compute.Execute<object, TArg, TReduceRes>(taskType, null).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TJobRes Call<TJobRes>(IComputeFunc<TJobRes> clo)
        {
            return _compute.Execute(clo).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> CallAsync<TRes>(IComputeFunc<TRes> clo)
        {
            return _compute.Execute(clo).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> CallAsync<TRes>(IComputeFunc<TRes> clo, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.Execute(clo).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TJobRes AffinityCall<TJobRes>(string cacheName, object affinityKey, IComputeFunc<TJobRes> clo)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return _compute.AffinityCall(cacheName, affinityKey, clo).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> AffinityCallAsync<TRes>(string cacheName, object affinityKey, IComputeFunc<TRes> clo)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return _compute.AffinityCall(cacheName, affinityKey, clo).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> AffinityCallAsync<TRes>(string cacheName, object affinityKey, IComputeFunc<TRes> clo, 
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.AffinityCall(cacheName, affinityKey, clo).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TJobRes Call<TJobRes>(Func<TJobRes> func)
        {
            return _compute.Execute(func).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> CallAsync<TFuncRes, TRes>(IEnumerable<IComputeFunc<TFuncRes>> clos, 
            IComputeReducer<TFuncRes, TRes> reducer)
        {
            return _compute.Execute(clos, reducer).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> CallAsync<TFuncRes, TRes>(IEnumerable<IComputeFunc<TFuncRes>> clos, 
            IComputeReducer<TFuncRes, TRes> reducer, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.Execute(clos, reducer).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public ICollection<TJobRes> Call<TJobRes>(IEnumerable<IComputeFunc<TJobRes>> clos)
        {
            return _compute.Execute(clos).Get();
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> CallAsync<TRes>(IEnumerable<IComputeFunc<TRes>> clos)
        {
            return _compute.Execute(clos).Task;
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> CallAsync<TRes>(IEnumerable<IComputeFunc<TRes>> clos, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<ICollection<TRes>>(cancellationToken) ??
               _compute.Execute(clos).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TReduceRes Call<TJobRes, TReduceRes>(IEnumerable<IComputeFunc<TJobRes>> clos, 
            IComputeReducer<TJobRes, TReduceRes> reducer)
        {
            return _compute.Execute(clos, reducer).Get();
        }

        /** <inheritDoc /> */
        public ICollection<TJobRes> Broadcast<TJobRes>(IComputeFunc<TJobRes> clo)
        {
            return _compute.Broadcast(clo).Get();
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> BroadcastAsync<TRes>(IComputeFunc<TRes> clo)
        {
            return _compute.Broadcast(clo).Task;
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> BroadcastAsync<TRes>(IComputeFunc<TRes> clo, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<ICollection<TRes>>(cancellationToken) ??
                _compute.Broadcast(clo).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public ICollection<TJobRes> Broadcast<T, TJobRes>(IComputeFunc<T, TJobRes> clo, T arg)
        {
            return _compute.Broadcast(clo, arg).Get();
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> BroadcastAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg)
        {
            return _compute.Broadcast(clo, arg).Task;
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> BroadcastAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<ICollection<TRes>>(cancellationToken) ??
                _compute.Broadcast(clo, arg).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public void Broadcast(IComputeAction action)
        {
            _compute.Broadcast(action).Get();
        }

        /** <inheritDoc /> */
        public Task BroadcastAsync(IComputeAction action)
        {
            return _compute.Broadcast(action).Task;
        }

        /** <inheritDoc /> */
        public Task BroadcastAsync(IComputeAction action, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<object>(cancellationToken) ??
                _compute.Broadcast(action).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public void Run(IComputeAction action)
        {
            _compute.Run(action).Get();
        }

        /** <inheritDoc /> */
        public Task RunAsync(IComputeAction action)
        {
            return _compute.Run(action).Task;
        }

        /** <inheritDoc /> */
        public Task RunAsync(IComputeAction action, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<object>(cancellationToken) ??
                _compute.Run(action).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public void AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            _compute.AffinityRun(cacheName, affinityKey, action).Get();
        }

        /** <inheritDoc /> */
        public Task AffinityRunAsync(string cacheName, object affinityKey, IComputeAction action)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return _compute.AffinityRun(cacheName, affinityKey, action).Task;
        }

        /** <inheritDoc /> */
        public Task AffinityRunAsync(string cacheName, object affinityKey, IComputeAction action, 
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return GetTaskIfAlreadyCancelled<object>(cancellationToken) ??
                _compute.AffinityRun(cacheName, affinityKey, action).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public void Run(IEnumerable<IComputeAction> actions)
        {
            _compute.Run(actions).Get();
        }

        /** <inheritDoc /> */
        public Task RunAsync(IEnumerable<IComputeAction> actions)
        {
            return _compute.Run(actions).Task;
        }

        /** <inheritDoc /> */
        public Task RunAsync(IEnumerable<IComputeAction> actions, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<object>(cancellationToken) ??
                _compute.Run(actions).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TJobRes Apply<TArg, TJobRes>(IComputeFunc<TArg, TJobRes> clo, TArg arg)
        {
            return _compute.Apply(clo, arg).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg)
        {
            return _compute.Apply(clo, arg).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, TArg arg, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.Apply(clo, arg).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public ICollection<TJobRes> Apply<TArg, TJobRes>(IComputeFunc<TArg, TJobRes> clo, IEnumerable<TArg> args)
        {
            return _compute.Apply(clo, args).Get();
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, IEnumerable<TArg> args)
        {
            return _compute.Apply(clo, args).Task;
        }

        /** <inheritDoc /> */
        public Task<ICollection<TRes>> ApplyAsync<TArg, TRes>(IComputeFunc<TArg, TRes> clo, IEnumerable<TArg> args, 
            CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<ICollection<TRes>>(cancellationToken) ??
                _compute.Apply(clo, args).GetTask(cancellationToken);
        }

        /** <inheritDoc /> */
        public TReduceRes Apply<TArg, TJobRes, TReduceRes>(IComputeFunc<TArg, TJobRes> clo, 
            IEnumerable<TArg> args, IComputeReducer<TJobRes, TReduceRes> rdc)
        {
            return _compute.Apply(clo, args, rdc).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(IComputeFunc<TArg, TFuncRes> clo, IEnumerable<TArg> args, 
            IComputeReducer<TFuncRes, TRes> rdc)
        {
            return _compute.Apply(clo, args, rdc).Task;
        }

        /** <inheritDoc /> */
        public Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(IComputeFunc<TArg, TFuncRes> clo, IEnumerable<TArg> args, 
            IComputeReducer<TFuncRes, TRes> rdc, CancellationToken cancellationToken)
        {
            return GetTaskIfAlreadyCancelled<TRes>(cancellationToken) ??
                _compute.Apply(clo, args, rdc).GetTask(cancellationToken);
        }

        /// <summary>
        /// Gets the cancelled task if specified token is cancelled.
        /// </summary>
        private static Task<T> GetTaskIfAlreadyCancelled<T>(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return CancelledTask<T>.Instance;

            return null;
        }

        /// <summary>
        /// Determines whether specified exception should result in a job failover.
        /// </summary>
        internal static bool IsFailoverException(Exception err)
        {
            while (err != null)
            {
                if (err is ComputeExecutionRejectedException || err is ClusterTopologyException ||
                    err is ComputeJobFailoverException)
                    return true;

                err = err.InnerException;
            }

            return false;
        }
    }
}
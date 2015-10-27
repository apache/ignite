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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;

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
        public ICompute WithTimeout(long timeout)
        {
            _compute.WithTimeout(timeout);

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithKeepPortable()
        {
            _compute.WithKeepPortable();

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
        public TJobRes AffinityCall<TJobRes>(string cacheName, object affinityKey, IComputeFunc<TJobRes> clo)
        {
            return _compute.AffinityCall(cacheName, affinityKey, clo).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> AffinityCallAsync<TRes>(string cacheName, object affinityKey, IComputeFunc<TRes> clo)
        {
            return _compute.AffinityCall(cacheName, affinityKey, clo).Task;
        }

        /** <inheritDoc /> */
        public TJobRes Call<TJobRes>(Func<TJobRes> func)
        {
            return _compute.Execute(func).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> CallAsync<TFuncRes, TRes>(IEnumerable<IComputeFunc<TFuncRes>> clos, IComputeReducer<TFuncRes, TRes> reducer)
        {
            return _compute.Execute(clos, reducer).Task;
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
        public void AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            _compute.AffinityRun(cacheName, affinityKey, action).Get();
        }

        /** <inheritDoc /> */
        public Task AffinityRunAsync(string cacheName, object affinityKey, IComputeAction action)
        {
            return _compute.AffinityRun(cacheName, affinityKey, action).Task;
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
        public TReduceRes Apply<TArg, TJobRes, TReduceRes>(IComputeFunc<TArg, TJobRes> clo, 
            IEnumerable<TArg> args, IComputeReducer<TJobRes, TReduceRes> rdc)
        {
            return _compute.Apply(clo, args, rdc).Get();
        }

        /** <inheritDoc /> */
        public Task<TRes> ApplyAsync<TArg, TFuncRes, TRes>(IComputeFunc<TArg, TFuncRes> clo, IEnumerable<TArg> args, IComputeReducer<TFuncRes, TRes> rdc)
        {
            return _compute.Apply(clo, args, rdc).Task;
        }
    }
}
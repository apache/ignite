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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
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
        public ICompute WithAsync()
        {
            return new ComputeAsync(_compute);
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
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
        public T ExecuteJavaTask<T>(string taskName, object taskArg)
        {
            return _compute.ExecuteJavaTask<T>(taskName, taskArg);
        }

        /** <inheritDoc /> */
        public TR Execute<TA, T, TR>(IComputeTask<TA, T, TR> task, TA taskArg)
        {
            return _compute.Execute(task, taskArg).Get();
        }

        /** <inheritDoc /> */
        public TR Execute<T, TR>(IComputeTask<T, TR> task)
        {
            return _compute.Execute(task, null).Get();
        }

        /** <inheritDoc /> */
        public TR Execute<TA, T, TR>(Type taskType, TA taskArg)
        {
            return _compute.Execute<TA, T, TR>(taskType, taskArg).Get();
        }

        public TR Execute<T, TR>(Type taskType)
        {
            return _compute.Execute<object, T, TR>(taskType, null).Get();
        }

        /** <inheritDoc /> */
        public TR Call<TR>(IComputeFunc<TR> clo)
        {
            return _compute.Execute(clo).Get();
        }

        /** <inheritDoc /> */
        public TR AffinityCall<TR>(string cacheName, object affinityKey, IComputeFunc<TR> clo)
        {
            return _compute.AffinityCall(cacheName, affinityKey, clo).Get();
        }

        /** <inheritDoc /> */
        public TR Call<TR>(Func<TR> func)
        {
            return _compute.Execute(func).Get();
        }

        /** <inheritDoc /> */
        public ICollection<TR> Call<TR>(IEnumerable<IComputeFunc<TR>> clos)
        {
            return _compute.Execute(clos).Get();
        }

        /** <inheritDoc /> */
        public TR2 Call<TR1, TR2>(IEnumerable<IComputeFunc<TR1>> clos, IComputeReducer<TR1, TR2> rdc)
        {
            return _compute.Execute(clos, rdc).Get();
        }

        /** <inheritDoc /> */
        public ICollection<TR> Broadcast<TR>(IComputeFunc<TR> clo)
        {
            return _compute.Broadcast(clo).Get();
        }

        /** <inheritDoc /> */
        public ICollection<TR> Broadcast<T, TR>(IComputeFunc<T, TR> clo, T arg)
        {
            return _compute.Broadcast(clo, arg).Get();
        }

        /** <inheritDoc /> */
        public void Broadcast(IComputeAction action)
        {
            _compute.Broadcast(action).Get();
        }

        /** <inheritDoc /> */
        public void Run(IComputeAction action)
        {
            _compute.Run(action).Get();
        }

        /** <inheritDoc /> */
        public void AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            _compute.AffinityRun(cacheName, affinityKey, action).Get();
        }

        /** <inheritDoc /> */
        public void Run(IEnumerable<IComputeAction> actions)
        {
            _compute.Run(actions).Get();
        }

        /** <inheritDoc /> */
        public TR Apply<T, TR>(IComputeFunc<T, TR> clo, T arg)
        {
            return _compute.Apply(clo, arg).Get();
        }

        /** <inheritDoc /> */
        public ICollection<TR> Apply<T, TR>(IComputeFunc<T, TR> clo, IEnumerable<T> args)
        {
            return _compute.Apply(clo, args).Get();
        }

        /** <inheritDoc /> */
        public TR2 Apply<T, TR1, TR2>(IComputeFunc<T, TR1> clo, IEnumerable<T> args, IComputeReducer<TR1, TR2> rdc)
        {
            return _compute.Apply(clo, args, rdc).Get();
        }
    }
}
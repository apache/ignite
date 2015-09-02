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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Asynchronous Compute facade.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class ComputeAsync : ICompute
    {
        /** */
        protected readonly ComputeImpl compute;

        /** Current future. */
        private readonly ThreadLocal<IFuture> curFut = new ThreadLocal<IFuture>();

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeAsync"/> class.
        /// </summary>
        /// <param name="computeImpl">The compute implementation.</param>
        internal ComputeAsync(ComputeImpl computeImpl)
        {
            compute = computeImpl;
        }

        /** <inheritDoc /> */
        public ICompute WithAsync()
        {
            return this;
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            var fut = curFut.Value;

            if (fut == null)
                throw new InvalidOperationException("Asynchronous operation not started.");

            var fut0 = fut as IFuture<TResult>;

            if (fut0 == null)
                throw new InvalidOperationException(
                    string.Format("Requested future type {0} is incompatible with current future type {1}",
                        typeof(IFuture<TResult>), fut.GetType()));

            curFut.Value = null;

            return fut0;
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return compute.ClusterGroup; }
        }

        /** <inheritDoc /> */
        public ICompute WithNoFailover()
        {
            compute.WithNoFailover();

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithTimeout(long timeout)
        {
            compute.WithTimeout(timeout);

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithKeepPortable()
        {
            compute.WithKeepPortable();

            return this;
        }
        
        /** <inheritDoc /> */
        public T ExecuteJavaTask<T>(string taskName, object taskArg)
        {
            curFut.Value = compute.ExecuteJavaTaskAsync<T>(taskName, taskArg);

            return default(T);
        }

        /** <inheritDoc /> */
        public R Execute<A, T, R>(IComputeTask<A, T, R> task, A taskArg)
        {
            curFut.Value = compute.Execute(task, taskArg);

            return default(R);
        }

        /** <inheritDoc /> */
        public R Execute<T, R>(IComputeTask<T, R> task)
        {
            curFut.Value = compute.Execute(task, null);

            return default(R);
        }

        /** <inheritDoc /> */
        public R Execute<A, T, R>(Type taskType, A taskArg)
        {
            curFut.Value = compute.Execute<A, T, R>(taskType, taskArg);

            return default(R);
        }

        /** <inheritDoc /> */
        public R Execute<T, R>(Type taskType)
        {
            curFut.Value = compute.Execute<object, T, R>(taskType, null);

            return default(R);
        }

        /** <inheritDoc /> */
        public R Call<R>(IComputeFunc<R> clo)
        {
            curFut.Value = compute.Execute(clo);

            return default(R);
        }

        /** <inheritDoc /> */
        public R AffinityCall<R>(string cacheName, object affinityKey, IComputeFunc<R> clo)
        {
            compute.AffinityCall(cacheName, affinityKey, clo);

            return default(R);
        }

        /** <inheritDoc /> */
        public R Call<R>(Func<R> func)
        {
            curFut.Value = compute.Execute(func);

            return default(R);
        }

        /** <inheritDoc /> */
        public ICollection<R> Call<R>(IEnumerable<IComputeFunc<R>> clos)
        {
            curFut.Value = compute.Execute(clos);

            return null;
        }

        /** <inheritDoc /> */
        public R2 Call<R1, R2>(IEnumerable<IComputeFunc<R1>> clos, IComputeReducer<R1, R2> rdc)
        {
            curFut.Value = compute.Execute(clos, rdc);

            return default(R2);
        }

        /** <inheritDoc /> */
        public ICollection<R> Broadcast<R>(IComputeFunc<R> clo)
        {
            curFut.Value = compute.Broadcast(clo);

            return null;
        }

        /** <inheritDoc /> */
        public ICollection<R> Broadcast<T, R>(IComputeFunc<T, R> clo, T arg)
        {
            curFut.Value = compute.Broadcast(clo, arg);

            return null;
        }

        /** <inheritDoc /> */
        public void Broadcast(IComputeAction action)
        {
            curFut.Value = compute.Broadcast(action);
        }

        /** <inheritDoc /> */
        public void Run(IComputeAction action)
        {
            curFut.Value = compute.Run(action);
        }

        /** <inheritDoc /> */
        public void AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            compute.AffinityRun(cacheName, affinityKey, action);
        }

        /** <inheritDoc /> */
        public void Run(IEnumerable<IComputeAction> actions)
        {
            curFut.Value = compute.Run(actions);
        }

        /** <inheritDoc /> */
        public R Apply<T, R>(IComputeFunc<T, R> clo, T arg)
        {
            curFut.Value = compute.Apply(clo, arg);

            return default(R);
        }

        /** <inheritDoc /> */
        public ICollection<R> Apply<T, R>(IComputeFunc<T, R> clo, IEnumerable<T> args)
        {
            curFut.Value = compute.Apply(clo, args);

            return null;
        }

        /** <inheritDoc /> */
        public R2 Apply<T, R1, R2>(IComputeFunc<T, R1> clo, IEnumerable<T> args, IComputeReducer<R1, R2> rdc)
        {
            curFut.Value = compute.Apply(clo, args, rdc);

            return default(R2);
        }
    }
}
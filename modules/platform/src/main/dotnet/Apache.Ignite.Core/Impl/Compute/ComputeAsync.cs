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
        protected readonly ComputeImpl Compute;

        /** Current future. */
        private readonly ThreadLocal<IFuture> _curFut = new ThreadLocal<IFuture>();

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeAsync"/> class.
        /// </summary>
        /// <param name="computeImpl">The compute implementation.</param>
        internal ComputeAsync(ComputeImpl computeImpl)
        {
            Compute = computeImpl;
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
            var fut = _curFut.Value;

            if (fut == null)
                throw new InvalidOperationException("Asynchronous operation not started.");

            var fut0 = fut as IFuture<TResult>;

            if (fut0 == null)
                throw new InvalidOperationException(
                    string.Format("Requested future type {0} is incompatible with current future type {1}",
                        typeof(IFuture<TResult>), fut.GetType()));

            _curFut.Value = null;

            return fut0;
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return Compute.ClusterGroup; }
        }

        /** <inheritDoc /> */
        public ICompute WithNoFailover()
        {
            Compute.WithNoFailover();

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithTimeout(long timeout)
        {
            Compute.WithTimeout(timeout);

            return this;
        }

        /** <inheritDoc /> */
        public ICompute WithKeepPortable()
        {
            Compute.WithKeepPortable();

            return this;
        }
        
        /** <inheritDoc /> */
        public T ExecuteJavaTask<T>(string taskName, object taskArg)
        {
            _curFut.Value = Compute.ExecuteJavaTaskAsync<T>(taskName, taskArg);

            return default(T);
        }

        /** <inheritDoc /> */
        public TR Execute<TA, T, TR>(IComputeTask<TA, T, TR> task, TA taskArg)
        {
            _curFut.Value = Compute.Execute(task, taskArg);

            return default(TR);
        }

        /** <inheritDoc /> */
        public TR Execute<T, TR>(IComputeTask<T, TR> task)
        {
            _curFut.Value = Compute.Execute(task, null);

            return default(TR);
        }

        /** <inheritDoc /> */
        public TR Execute<TA, T, TR>(Type taskType, TA taskArg)
        {
            _curFut.Value = Compute.Execute<TA, T, TR>(taskType, taskArg);

            return default(TR);
        }

        /** <inheritDoc /> */
        public TR Execute<T, TR>(Type taskType)
        {
            _curFut.Value = Compute.Execute<object, T, TR>(taskType, null);

            return default(TR);
        }

        /** <inheritDoc /> */
        public TR Call<TR>(IComputeFunc<TR> clo)
        {
            _curFut.Value = Compute.Execute(clo);

            return default(TR);
        }

        /** <inheritDoc /> */
        public TR AffinityCall<TR>(string cacheName, object affinityKey, IComputeFunc<TR> clo)
        {
            Compute.AffinityCall(cacheName, affinityKey, clo);

            return default(TR);
        }

        /** <inheritDoc /> */
        public TR Call<TR>(Func<TR> func)
        {
            _curFut.Value = Compute.Execute(func);

            return default(TR);
        }

        /** <inheritDoc /> */
        public ICollection<TR> Call<TR>(IEnumerable<IComputeFunc<TR>> clos)
        {
            _curFut.Value = Compute.Execute(clos);

            return null;
        }

        /** <inheritDoc /> */
        public TR2 Call<TR1, TR2>(IEnumerable<IComputeFunc<TR1>> clos, IComputeReducer<TR1, TR2> rdc)
        {
            _curFut.Value = Compute.Execute(clos, rdc);

            return default(TR2);
        }

        /** <inheritDoc /> */
        public ICollection<TR> Broadcast<TR>(IComputeFunc<TR> clo)
        {
            _curFut.Value = Compute.Broadcast(clo);

            return null;
        }

        /** <inheritDoc /> */
        public ICollection<TR> Broadcast<T, TR>(IComputeFunc<T, TR> clo, T arg)
        {
            _curFut.Value = Compute.Broadcast(clo, arg);

            return null;
        }

        /** <inheritDoc /> */
        public void Broadcast(IComputeAction action)
        {
            _curFut.Value = Compute.Broadcast(action);
        }

        /** <inheritDoc /> */
        public void Run(IComputeAction action)
        {
            _curFut.Value = Compute.Run(action);
        }

        /** <inheritDoc /> */
        public void AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            Compute.AffinityRun(cacheName, affinityKey, action);
        }

        /** <inheritDoc /> */
        public void Run(IEnumerable<IComputeAction> actions)
        {
            _curFut.Value = Compute.Run(actions);
        }

        /** <inheritDoc /> */
        public TR Apply<T, TR>(IComputeFunc<T, TR> clo, T arg)
        {
            _curFut.Value = Compute.Apply(clo, arg);

            return default(TR);
        }

        /** <inheritDoc /> */
        public ICollection<TR> Apply<T, TR>(IComputeFunc<T, TR> clo, IEnumerable<T> args)
        {
            _curFut.Value = Compute.Apply(clo, args);

            return null;
        }

        /** <inheritDoc /> */
        public TR2 Apply<T, TR1, TR2>(IComputeFunc<T, TR1> clo, IEnumerable<T> args, IComputeReducer<TR1, TR2> rdc)
        {
            _curFut.Value = Compute.Apply(clo, args, rdc);

            return default(TR2);
        }
    }
}
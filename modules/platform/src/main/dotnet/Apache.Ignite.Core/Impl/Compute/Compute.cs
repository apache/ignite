/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Compute;

    using U = GridGain.Impl.GridUtils;

    /// <summary>
    /// Synchronous Compute facade.
    /// </summary>
    internal class Compute : ICompute
    {
        /** */
        private readonly ComputeImpl compute;

        /// <summary>
        /// Initializes a new instance of the <see cref="Compute"/> class.
        /// </summary>
        /// <param name="computeImpl">The compute implementation.</param>
        public Compute(ComputeImpl computeImpl)
        {
            Debug.Assert(computeImpl != null);

            compute = computeImpl;
        }

        /** <inheritDoc /> */
        public ICompute WithAsync()
        {
            return new ComputeAsync(compute);
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            throw U.GetAsyncModeDisabledException();
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            throw U.GetAsyncModeDisabledException();
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
            return compute.ExecuteJavaTask<T>(taskName, taskArg);
        }

        /** <inheritDoc /> */
        public R Execute<A, T, R>(IComputeTask<A, T, R> task, A taskArg)
        {
            return compute.Execute(task, taskArg).Get();
        }

        /** <inheritDoc /> */
        public R Execute<T, R>(IComputeTask<T, R> task)
        {
            return compute.Execute(task, null).Get();
        }

        /** <inheritDoc /> */
        public R Execute<A, T, R>(Type taskType, A taskArg)
        {
            return compute.Execute<A, T, R>(taskType, taskArg).Get();
        }

        public R Execute<T, R>(Type taskType)
        {
            return compute.Execute<object, T, R>(taskType, null).Get();
        }

        /** <inheritDoc /> */
        public R Call<R>(IComputeFunc<R> clo)
        {
            return compute.Execute(clo).Get();
        }

        /** <inheritDoc /> */
        public R AffinityCall<R>(string cacheName, object affinityKey, IComputeFunc<R> clo)
        {
            return compute.AffinityCall(cacheName, affinityKey, clo).Get();
        }

        /** <inheritDoc /> */
        public R Call<R>(Func<R> func)
        {
            return compute.Execute(func).Get();
        }

        /** <inheritDoc /> */
        public ICollection<R> Call<R>(IEnumerable<IComputeFunc<R>> clos)
        {
            return compute.Execute(clos).Get();
        }

        /** <inheritDoc /> */
        public R2 Call<R1, R2>(IEnumerable<IComputeFunc<R1>> clos, IComputeReducer<R1, R2> rdc)
        {
            return compute.Execute(clos, rdc).Get();
        }

        /** <inheritDoc /> */
        public ICollection<R> Broadcast<R>(IComputeFunc<R> clo)
        {
            return compute.Broadcast(clo).Get();
        }

        /** <inheritDoc /> */
        public ICollection<R> Broadcast<T, R>(IComputeFunc<T, R> clo, T arg)
        {
            return compute.Broadcast(clo, arg).Get();
        }

        /** <inheritDoc /> */
        public void Broadcast(IComputeAction action)
        {
            compute.Broadcast(action).Get();
        }

        /** <inheritDoc /> */
        public void Run(IComputeAction action)
        {
            compute.Run(action).Get();
        }

        /** <inheritDoc /> */
        public void AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            compute.AffinityRun(cacheName, affinityKey, action).Get();
        }

        /** <inheritDoc /> */
        public void Run(IEnumerable<IComputeAction> actions)
        {
            compute.Run(actions).Get();
        }

        /** <inheritDoc /> */
        public R Apply<T, R>(IComputeFunc<T, R> clo, T arg)
        {
            return compute.Apply(clo, arg).Get();
        }

        /** <inheritDoc /> */
        public ICollection<R> Apply<T, R>(IComputeFunc<T, R> clo, IEnumerable<T> args)
        {
            return compute.Apply(clo, args).Get();
        }

        /** <inheritDoc /> */
        public R2 Apply<T, R1, R2>(IComputeFunc<T, R1> clo, IEnumerable<T> args, IComputeReducer<R1, R2> rdc)
        {
            return compute.Apply(clo, args, rdc).Get();
        }
    }
}
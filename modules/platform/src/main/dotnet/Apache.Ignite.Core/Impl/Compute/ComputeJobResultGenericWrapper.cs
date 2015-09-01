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
    using GridGain.Compute;

    /// <summary>
    /// Wraps non-generic IComputeJobResult in generic form.
    /// </summary>
    internal class ComputeJobResultGenericWrapper<T> : IComputeJobResult<T>
    {
        /** */
        private readonly IComputeJobResult<object> wrappedRes;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobResultGenericWrapper{T}"/> class.
        /// </summary>
        /// <param name="jobRes">The job result to wrap.</param>
        public ComputeJobResultGenericWrapper(IComputeJobResult<object> jobRes)
        {
            wrappedRes = jobRes;
        }

        /** <inheritdoc /> */
        public T Data()
        {
            return (T)wrappedRes.Data();
        }

        /** <inheritdoc /> */
        public Exception Exception()
        {
            return wrappedRes.Exception();
        }

        /** <inheritdoc /> */
        public IComputeJob<T> Job()
        {
            return wrappedRes.Job().Unwrap<object, T>();
        }

        /** <inheritdoc /> */
        public Guid NodeId
        {
            get { return wrappedRes.NodeId; }
        }

        /** <inheritdoc /> */
        public bool Cancelled
        {
            get { return wrappedRes.Cancelled; }
        }
    }
}
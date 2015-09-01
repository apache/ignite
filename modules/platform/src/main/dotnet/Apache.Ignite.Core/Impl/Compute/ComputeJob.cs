/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Compute
{
    using System;
    using System.Reflection;
    using GridGain.Impl;
    using GridGain.Impl.Common;
    using GridGain.Impl.Compute;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;
    using GridGain.Portable;
    using GridGain.Resource;

    /// <summary>
    /// Non-generic version of IComputeJob{T}.
    /// </summary>
    internal interface IComputeJob : IComputeJob<object>
    {
        // No-op.
    }

    /// <summary>
    /// Wraps generic func into a non-generic for internal usage.
    /// </summary>
    internal class ComputeJobWrapper : IComputeJob, IPortableWriteAware
    {
        /** */
        private readonly Func<object, object> execute;

        /** */
        private readonly Action<object> cancel;

        /** */
        private readonly object job;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeJobWrapper(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            job = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            DelegateTypeDescriptor.GetComputeJob(job.GetType(), out execute, out cancel);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncWrapper" /> class.
        /// </summary>
        public ComputeJobWrapper(object job, Func<object, object> execute, Action<object> cancel)
        {
            this.job = job;

            this.execute = execute;

            this.cancel = cancel;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            try
            {
                return execute(job);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            try
            {
                cancel(job);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, Job);
        }

        /// <summary>
        /// Injects the grid into wrapped object.
        /// </summary>
        [InstanceResource]
        public void InjectGrid(IGrid grid)
        {
            // Propagate injection
            ResourceProcessor.Inject(Job, (GridProxy)grid);
        }

        /// <summary>
        /// Gets the inner job.
        /// </summary>
        public object Job
        {
            get { return job; }
        }
    }

    /// <summary>
    /// Extension methods for IComputeJob{T}.
    /// </summary>
    internal static class ComputeJobExtensions
    {
        /// <summary>
        /// Convert to non-generic wrapper.
        /// </summary>
        public static IComputeJob ToNonGeneric<T>(this IComputeJob<T> job)
        {
            return new ComputeJobWrapper(job, x => job.Execute(), x => job.Cancel());
        }

        /// <summary>
        /// Unwraps job of one type into job of another type.
        /// </summary>
        public static IComputeJob<R> Unwrap<T, R>(this IComputeJob<T> job)
        {
            var wrapper = job as ComputeJobWrapper;

            return wrapper != null ? (IComputeJob<R>) wrapper.Job : (IComputeJob<R>) job;
        }
        
        /// <summary>
        /// Unwraps job of one type into job of another type.
        /// </summary>
        public static object Unwrap(this IComputeJob<object> job)
        {
            var wrapper = job as ComputeJobWrapper;

            return wrapper != null ? wrapper.Job : job;
        }
    }
}

/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Compute
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Non-generic version of IComputeFunc{T}.
    /// </summary>
    internal interface IComputeOutFunc : IComputeFunc<object>
    {
        // No-op.
    }

    /// <summary>
    /// Wraps generic func into a non-generic for internal usage.
    /// </summary>
    internal class ComputeOutFuncWrapper : IComputeOutFunc, IPortableWriteAware
    {
        /** */
        private readonly object func;

        /** */
        private readonly Func<object, object> invoker;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncWrapper" /> class.
        /// </summary>
        /// <param name="func">The function to wrap.</param>
        /// <param name="invoker">The function invoker.</param>
        public ComputeOutFuncWrapper(object func, Func<object> invoker)
        {
            Debug.Assert(func != null);
            Debug.Assert(invoker != null);

            this.func = func;

            this.invoker = target => invoker();
        }

        /** <inheritDoc /> */
        public object Invoke()
        {
            try
            {
                return invoker(func);
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
            PortableUtils.WritePortableOrSerializable(writer0, func);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeOutFuncWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeOutFuncWrapper(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            func = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            invoker = DelegateTypeDescriptor.GetComputeOutFunc(func.GetType());
        }

        /// <summary>
        /// Injects the grid.
        /// </summary>
        [InstanceResource]
        public void InjectGrid(IIgnite grid)
        {
            // Propagate injection
            ResourceProcessor.Inject(func, (GridProxy)grid);
        }
    }

    /// <summary>
    /// Extension methods for IComputeOutFunc{T}.
    /// </summary>
    internal static class ComputeOutFuncExtensions
    {
        /// <summary>
        /// Convert to non-generic wrapper.
        /// </summary>
        public static IComputeOutFunc ToNonGeneric<T>(this IComputeFunc<T> func)
        {
            return new ComputeOutFuncWrapper(func, () => func.Invoke());
        }
    }
}

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
    using System.Reflection;

    using GridGain.Compute;
    using GridGain.Impl.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;
    using GridGain.Portable;
    using GridGain.Resource;

    /// <summary>
    /// Non-generic version of IComputeFunc{T}.
    /// </summary>
    internal interface IComputeFunc : IComputeFunc<object, object>
    {
        // No-op
    }

    /// <summary>
    /// Wraps generic func into a non-generic for internal usage.
    /// </summary>
    internal class ComputeFuncWrapper : IComputeFunc, IPortableWriteAware
    {
        /** */
        private readonly object func;

        /** */
        private readonly Func<object, object, object> invoker;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncWrapper" /> class.
        /// </summary>
        /// <param name="func">The function to wrap.</param>
        /// <param name="invoker">The function invoker.</param>
        public ComputeFuncWrapper(object func, Func<object, object> invoker)
        {
            this.func = func;

            this.invoker = (target, arg) => invoker(arg);
        }

        /** <inheritDoc /> */
        public object Invoke(object arg)
        {
            try
            {
                return invoker(func, arg);
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
        /// Initializes a new instance of the <see cref="ComputeFuncWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeFuncWrapper(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            func = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            invoker = DelegateTypeDescriptor.GetComputeFunc(func.GetType());
        }

        /// <summary>
        /// Injects the grid.
        /// </summary>
        [InstanceResource]
        public void InjectGrid(IIgnite grid)
        {
            // Propagate injection
            ResourceProcessor.Inject(func, (GridProxy) grid);
        }
    }    
    
    /// <summary>
    /// Extension methods for IComputeFunc{T}.
    /// </summary>
    internal static class ComputeFuncExtensions
    {
        /// <summary>
        /// Convert to non-generic wrapper.
        /// </summary>
        public static IComputeFunc ToNonGeneric<T, R>(this IComputeFunc<T, R> func)
        {
            return new ComputeFuncWrapper(func, x => func.Invoke((T) x));
        }
    }
}

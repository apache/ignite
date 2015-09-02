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

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
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Deployment;
    using Apache.Ignite.Core.Impl.Resource;
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
    internal class ComputeOutFuncWrapper : IComputeOutFunc, IBinaryWriteAware
    {
        /** */
        private readonly object _func;

        /** */
        private readonly Func<object, object> _invoker;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncWrapper" /> class.
        /// </summary>
        /// <param name="func">The function to wrap.</param>
        /// <param name="invoker">The function invoker.</param>
        public ComputeOutFuncWrapper(object func, Func<object> invoker)
        {
            Debug.Assert(func != null);
            Debug.Assert(invoker != null);

            _func = func;

            _invoker = target => invoker();
        }

        /** <inheritDoc /> */
        public object Invoke()
        {
            try
            {
                return _invoker(_func);
            }
            catch (TargetInvocationException ex)
            {
                if (ex.InnerException != null)
                    throw ex.InnerException;

                throw;
            }
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter)writer.GetRawWriter();

            writer0.WriteWithPeerDeployment(_func);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeOutFuncWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeOutFuncWrapper(IBinaryRawReader reader)
        {
            _func = reader.ReadObject<object>();

            _invoker = DelegateTypeDescriptor.GetComputeOutFunc(_func.GetType());
        }

        /// <summary>
        /// Injects the grid.
        /// </summary>
        [InstanceResource]
        public void InjectIgnite(IIgnite ignite)
        {
            // Propagate injection
            ResourceProcessor.Inject(_func, (Ignite)ignite);
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

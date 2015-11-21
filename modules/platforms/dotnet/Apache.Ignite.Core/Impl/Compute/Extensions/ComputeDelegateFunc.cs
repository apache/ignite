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

namespace Apache.Ignite.Core.Impl.Compute.Extensions
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Compute func from a delegate.
    /// </summary>
    [Serializable]
    internal class ComputeDelegateFunc<TRes> : SerializableWrapper<Func<TRes>>, IComputeFunc<TRes>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateFunc{R}"/> class.
        /// </summary>
        /// <param name="func">The function to wrap.</param>
        public ComputeDelegateFunc(Func<TRes> func) : base(func)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateFunc{R}"/> class.
        /// </summary>
        /// <param name="info">The information.</param>
        /// <param name="context">The context.</param>
        public ComputeDelegateFunc(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public TRes Invoke()
        {
            return WrappedObject();
        }
    }

    /// <summary>
    /// Compute func from a delegate.
    /// </summary>
    [Serializable]
    internal class ComputeDelegateFunc<TArg, TRes> : SerializableWrapper<Func<TArg, TRes>>, IComputeFunc<TArg, TRes>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateFunc{R}"/> class.
        /// </summary>
        /// <param name="func">The function to wrap.</param>
        public ComputeDelegateFunc(Func<TArg, TRes> func) : base(func)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateFunc{R}"/> class.
        /// </summary>
        /// <param name="info">The information.</param>
        /// <param name="context">The context.</param>
        public ComputeDelegateFunc(SerializationInfo info, StreamingContext context): base(info, context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public TRes Invoke(TArg arg)
        {
            return WrappedObject(arg);
        }
    }
}

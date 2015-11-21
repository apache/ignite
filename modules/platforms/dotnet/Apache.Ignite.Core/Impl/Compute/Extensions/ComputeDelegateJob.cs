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
    /// Compute job from delegate.
    /// </summary>
    [Serializable]
    internal class ComputeDelegateJob<TRes> : SerializableWrapper<Func<TRes>>, IComputeJob<TRes>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateJob{T}"/> class.
        /// </summary>
        /// <param name="del">The delegate.</param>
        public ComputeDelegateJob(Func<TRes> del) : base(del)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeDelegateJob{T}"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        public ComputeDelegateJob(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public TRes Execute()
        {
            return WrappedObject();
        }

        /** <inheritdoc /> */
        public void Cancel()
        {
            // No-op.
        }
    }
}
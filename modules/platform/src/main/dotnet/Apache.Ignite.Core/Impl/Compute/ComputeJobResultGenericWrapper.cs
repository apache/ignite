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
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Wraps non-generic IComputeJobResult in generic form.
    /// </summary>
    internal class ComputeJobResultGenericWrapper<T> : IComputeJobResult<T>
    {
        /** */
        private readonly IComputeJobResult<object> _wrappedRes;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobResultGenericWrapper{T}"/> class.
        /// </summary>
        /// <param name="jobRes">The job result to wrap.</param>
        public ComputeJobResultGenericWrapper(IComputeJobResult<object> jobRes)
        {
            _wrappedRes = jobRes;
        }

        /** <inheritdoc /> */
        public T Data()
        {
            return (T)_wrappedRes.Data();
        }

        /** <inheritdoc /> */
        public Exception Exception()
        {
            return _wrappedRes.Exception();
        }

        /** <inheritdoc /> */
        public IComputeJob<T> Job()
        {
            return _wrappedRes.Job().Unwrap<object, T>();
        }

        /** <inheritdoc /> */
        public Guid NodeId
        {
            get { return _wrappedRes.NodeId; }
        }

        /** <inheritdoc /> */
        public bool Cancelled
        {
            get { return _wrappedRes.Cancelled; }
        }
    }
}
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

namespace Apache.Ignite.Core.Compute
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// This runtime exception can be thrown from <see cref="IComputeJob{T}.Execute()"/>
    /// method to force job failover to another node within task topology.
    /// <see cref="IComputeFunc{T,R}"/> or <see cref="IComputeFunc{T}"/>
    /// passed into any of the <see cref="ICompute"/> methods can also throw this exception
    /// to force failover.
    /// </summary>
    [Serializable]
    public class ComputeJobFailoverException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException"/> class.
        /// </summary>
        public ComputeJobFailoverException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ComputeJobFailoverException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ComputeJobFailoverException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ComputeJobFailoverException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}

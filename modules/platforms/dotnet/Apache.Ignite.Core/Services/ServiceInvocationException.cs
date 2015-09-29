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

namespace Apache.Ignite.Core.Services
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Indicates an error during Grid Services invocation.
    /// </summary>
    [Serializable]
    public class ServiceInvocationException : IgniteException
    {
        /** Serializer key. */
        private const string KeyPortableCause = "PortableCause";

        /** Cause. */
        private readonly IPortableObject _portableCause;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        public ServiceInvocationException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ServiceInvocationException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ServiceInvocationException(string message, Exception cause) : base(message, cause)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceInvocationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="portableCause">The portable cause.</param>
        public ServiceInvocationException(string message, IPortableObject portableCause)
            :base(message)
        {
            _portableCause = portableCause;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ServiceInvocationException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            _portableCause = (IPortableObject) info.GetValue(KeyPortableCause, typeof (IPortableObject));
        }

        /// <summary>
        /// Gets the portable cause.
        /// </summary>
        public IPortableObject PortableCause
        {
            get { return _portableCause; }
        }

        /** <inheritdoc /> */
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(KeyPortableCause, _portableCause);

            base.GetObjectData(info, context);
        }
    }
}
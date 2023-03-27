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

namespace Apache.Ignite.Core.Client
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Ignite thin client exception.
    /// </summary>
    [Serializable]
    public class IgniteClientException : IgniteException
    {
        /** Error code field. */
        private const string ErrorCodeField = "StatusCode";

        /** Error code. */
        private readonly ClientStatusCode _statusCode = ClientStatusCode.Fail;

        /// <summary>
        /// Gets the status code code.
        /// </summary>
        public ClientStatusCode StatusCode
        {
            get { return _statusCode; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException"/> class.
        /// </summary>
        public IgniteClientException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public IgniteClientException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException" /> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public IgniteClientException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException" /> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        /// <param name="statusCode">The error code.</param>
        public IgniteClientException(string message, Exception cause, ClientStatusCode statusCode) 
            : base(message, cause)
        {
            _statusCode = statusCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected IgniteClientException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
            _statusCode = (ClientStatusCode) info.GetInt32(ErrorCodeField);
        }

        /// <summary>
        /// When overridden in a derived class, sets the <see cref="SerializationInfo" /> 
        /// with information about the exception.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo" /> that holds the serialized object data
        /// about the exception being thrown.</param>
        /// <param name="context">The <see cref="StreamingContext" /> that contains contextual information
        /// about the source or destination.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue(ErrorCodeField, (int) _statusCode);
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0} [StatusCode={1}]", base.ToString(), StatusCode);
        }
    }
}

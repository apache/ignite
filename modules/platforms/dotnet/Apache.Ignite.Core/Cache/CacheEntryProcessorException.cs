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

namespace Apache.Ignite.Core.Cache
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// An exception to indicate a problem occurred attempting to execute an 
    /// <see cref="ICacheEntryProcessor{K, V, A, R}"/> against an entry.
    /// </summary>
    [Serializable]
    public class CacheEntryProcessorException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        public CacheEntryProcessorException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CacheEntryProcessorException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public CacheEntryProcessorException(string message, Exception cause)
            : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="innerException">The inner exception.</param>
        public CacheEntryProcessorException(Exception innerException)
            : base("Error occurred in CacheEntryProcessor, see InnerException for details.", innerException)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected CacheEntryProcessorException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
            // No-op.
        }
    }
}
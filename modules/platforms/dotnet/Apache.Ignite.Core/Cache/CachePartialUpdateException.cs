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
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception thrown from non-transactional cache in case when update succeeded only partially.
    /// </summary>
    [Serializable]
    public class CachePartialUpdateException : CacheException
    {
        /** Serializer key. */
        private const string KeyFailedKeys = "FailedKeys";

        /** Failed keys. */
        private readonly IList<object> _failedKeys;

        /** Failed keys exception. */
        private readonly Exception _failedKeysException;

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePartialUpdateException"/> class.
        /// </summary>
        public CachePartialUpdateException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePartialUpdateException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CachePartialUpdateException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePartialUpdateException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected CachePartialUpdateException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            _failedKeys = (IList<object>) info.GetValue(KeyFailedKeys, typeof (IList<object>));
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        /// <param name="failedKeysException">Exception occurred during failed keys read/write.</param>
        public CachePartialUpdateException(string msg, Exception failedKeysException) : this(msg, null, failedKeysException)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        /// <param name="failedKeys">Failed keys.</param>
        public CachePartialUpdateException(string msg, IList<object> failedKeys) : this(msg, failedKeys, null)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        /// <param name="failedKeys">Failed keys.</param>
        /// <param name="failedKeysException">Exception occurred during failed keys read/write.</param>
        private CachePartialUpdateException(string msg, IList<object> failedKeys, Exception failedKeysException) : base(msg)
        {
            _failedKeys = failedKeys;
            _failedKeysException = failedKeysException;
        }

        /// <summary>
        /// Gets the failed keys.
        /// </summary>
        public IEnumerable<T> GetFailedKeys<T>()
        {
            if (_failedKeysException != null)
                throw _failedKeysException;
            
            return _failedKeys == null ? null : _failedKeys.Cast<T>();
        }

        /** <inheritdoc /> */
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(KeyFailedKeys, _failedKeys);

            base.GetObjectData(info, context);
        }
    }
}

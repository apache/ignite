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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Result of the session state lock processor.
    /// </summary>
    internal class SessionStateLockResult
    {
        /** Success flag. */
        private readonly bool _success;

        /** Session state data. */
        private readonly IgniteSessionStateStoreData _data;

        /** Lock time. */
        private readonly DateTime? _lockTime;

        /** Lock id. */
        private readonly long _lockId;

        /// <summary>
        /// Initializes a new instance of the <see cref="SessionStateLockResult"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public SessionStateLockResult(IBinaryRawReader reader)
        {
            _success = reader.ReadBoolean();

            if (_success)
                _data = new IgniteSessionStateStoreData(reader);

            _lockTime = reader.ReadTimestamp();
            _lockId = reader.ReadLong();

            Debug.Assert(_success ^ (_data == null));
            Debug.Assert(_success ^ (_lockTime != null));
        }

        /// <summary>
        /// Gets a value indicating whether lock succeeded.
        /// </summary>
        public bool Success
        {
            get { return _success; }
        }

        /// <summary>
        /// Gets the data. Null when <see cref="Success"/> is <c>false</c>.
        /// </summary>
        public IgniteSessionStateStoreData Data
        {
            get { return _data; }
        }

        /// <summary>
        /// Gets the lock time. Null when <see cref="Success"/> is <c>true</c>.
        /// </summary>
        public DateTime? LockTime
        {
            get { return _lockTime; }
        }

        /// <summary>
        /// Gets the lock identifier.
        /// </summary>
        public long LockId
        {
            get { return _lockId; }
        }
    }
}

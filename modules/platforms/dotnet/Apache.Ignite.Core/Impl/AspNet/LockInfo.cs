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

namespace Apache.Ignite.Core.Impl.AspNet
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    
    /// <summary>
    /// Lock info.
    /// </summary>
    public struct LockInfo : IBinaryWriteAware
    {
        /** */
        private readonly DateTime _lockTime;

        /** */
        private readonly long _lockId;
        
        /** */
        private readonly Guid _lockNodeId;

        /// <summary>
        /// Initializes a new instance of the <see cref="LockInfo"/> struct.
        /// </summary>
        /// <param name="lockId">The lock identifier.</param>
        /// <param name="lockNodeId">The lock node identifier.</param>
        /// <param name="lockTime">The lock time.</param>
        public LockInfo(long lockId, Guid lockNodeId, DateTime lockTime)
        {
            _lockId = lockId;
            _lockNodeId = lockNodeId;
            _lockTime = lockTime;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LockInfo"/> struct.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public LockInfo(IBinaryRawReader reader)
        {
            _lockId = reader.ReadLong();

            var guid = reader.ReadGuid();
            Debug.Assert(guid != null);
            _lockNodeId = guid.Value;

            var time = reader.ReadTimestamp();
            Debug.Assert(time != null);
            _lockTime = time.Value;
        }

        /// <summary>
        /// Gets or sets the lock node id. Null means not locked.
        /// </summary>
        public Guid LockNodeId
        {
            get { return _lockNodeId; }
        }

        /// <summary>
        /// Gets or sets the lock id.
        /// </summary>
        public long LockId
        {
            get { return _lockId; }
        }

        /// <summary>
        /// Gets or sets the lock time.
        /// </summary>
        public DateTime LockTime
        {
            get { return _lockTime; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = writer.GetRawWriter();

            raw.WriteLong(_lockId);
            raw.WriteGuid(_lockNodeId);
            raw.WriteTimestamp(_lockTime);
        }
    }
}

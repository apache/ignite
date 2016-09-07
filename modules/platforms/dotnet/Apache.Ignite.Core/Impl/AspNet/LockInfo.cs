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
    public struct LockInfo : IBinaryWriteAware, IEquatable<LockInfo>
    {
        /** */
        private readonly long _lockId;
        
        /** */
        private readonly Guid _lockNodeId;

        /** */
        private readonly DateTime _lockTime;

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

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public bool Equals(LockInfo other)
        {
            return _lockTime.Equals(other._lockTime) && _lockId == other._lockId 
                && _lockNodeId.Equals(other._lockNodeId);
        }

        /// <summary>
        /// Determines whether the specified <see cref="object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is LockInfo && Equals((LockInfo) obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms 
        /// and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _lockTime.GetHashCode();
                hashCode = (hashCode*397) ^ _lockId.GetHashCode();
                hashCode = (hashCode*397) ^ _lockNodeId.GetHashCode();
                return hashCode;
            }
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        public static bool operator ==(LockInfo left, LockInfo right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        public static bool operator !=(LockInfo left, LockInfo right)
        {
            return !left.Equals(right);
        }
    }
}

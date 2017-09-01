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

namespace Apache.Ignite.Core.Common
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Ignite guid with additional local ID.
    /// </summary>
    [Serializable]
    public struct IgniteGuid : IEquatable<IgniteGuid>, IBinaryWriteAware
    {
        /** Global id. */
        private readonly Guid _globalId;

        /** Local id. */
        private readonly long _localId;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteGuid"/> struct.
        /// </summary>
        /// <param name="globalId">The global id.</param>
        /// <param name="localId">The local id.</param>
        public IgniteGuid(Guid globalId, long localId)
        {
            _globalId = globalId;
            _localId = localId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteGuid"/> struct.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal IgniteGuid(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            var stream = ((BinaryReader) reader).Stream;

            _localId = stream.ReadLong();
            _globalId = BinaryUtils.ReadGuid(stream);
        }

        /// <summary>
        /// Gets the global id.
        /// </summary>
        public Guid GlobalId
        {
            get { return _globalId; }
        }

        /// <summary>
        /// Gets the local id.
        /// </summary>
        public long LocalId
        {
            get { return _localId; }
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public bool Equals(IgniteGuid other)
        {
            return _globalId.Equals(other._globalId) && _localId == other._localId;
        }

        /// <summary>
        /// Determines whether the specified <see cref="object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is IgniteGuid && Equals((IgniteGuid) obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and
        /// data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return (_globalId.GetHashCode() * 397) ^ _localId.GetHashCode();
            }
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, 
                "IgniteGuid [GlobalId={0}, LocalId={1}]", GlobalId, LocalId);
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="a">First item.</param>
        /// <param name="b">Second item.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(IgniteGuid a, IgniteGuid b)
        {
            return a.Equals(b);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="a">First item.</param>
        /// <param name="b">Second item.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(IgniteGuid a, IgniteGuid b)
        {
            return !(a == b);
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        void IBinaryWriteAware.WriteBinary(IBinaryWriter writer)
        {
            Debug.Assert(writer != null);

            var stream = ((BinaryWriter) writer.GetRawWriter()).Stream;

            stream.WriteLong(_localId);
            BinaryUtils.WriteGuid(_globalId, stream);
        }
    }
}
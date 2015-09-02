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
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Ignite guid with additional local ID.
    /// </summary>
    public struct IgniteGuid : IEquatable<IgniteGuid>
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

        /** <inheritDoc /> */
        public bool Equals(IgniteGuid other)
        {
            return _globalId.Equals(other._globalId) && _localId == other._localId;
        }

        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is IgniteGuid && Equals((IgniteGuid) obj);
        }

        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                return (_globalId.GetHashCode() * 397) ^ _localId.GetHashCode();
            }
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format("IgniteGuid [GlobalId={0}, LocalId={1}]", GlobalId, LocalId);
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary> 
        /// <param name="w">Writer.</param>
        public void WritePortable(IPortableRawWriter w)
        {
            w.WriteGuid(GlobalId);
            w.WriteLong(LocalId);
        }

        /// <summary>
        /// Reads this object from the given reader.
        /// </summary> 
        /// <param name="r">Reader.</param>
        public static IgniteGuid ReadPortable(IPortableRawReader r)
        {
            var guid = r.ReadGuid();

            return guid == null
                ? new IgniteGuid(Guid.Empty, 0)
                : new IgniteGuid(guid.Value, r.ReadLong());
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
    }
}
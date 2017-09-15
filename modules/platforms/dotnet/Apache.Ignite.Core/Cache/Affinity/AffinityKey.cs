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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Optional wrapper for cache keys to provide support for custom affinity mapping.
    /// The value returned by <see cref="Affinity"/> will be used for key-to-node affinity.
    /// </summary>
    public struct AffinityKey : IEquatable<AffinityKey>, IBinaryWriteAware
    {
        /** User key. */
        private readonly object _key;

        /** Affinity key. */
        private readonly object _affinity;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityKey"/> struct.
        /// </summary>
        /// <param name="key">The key.</param>
        public AffinityKey(object key)
        {
            _key = key;
            _affinity = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityKey"/> struct.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="affinity">The affinity key.</param>
        public AffinityKey(object key, object affinity)
        {
            _key = key;
            _affinity = affinity;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityKey"/> struct.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal AffinityKey(IBinaryReader reader)
        {
            _key = reader.ReadObject<object>("key");
            _affinity = reader.ReadObject<object>("affKey");
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            IgniteArgumentCheck.NotNull(writer, "writer");

            writer.WriteObject("key", _key);
            writer.WriteObject("affKey", _affinity);
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public object Key
        {
            get { return _key; }
        }

        /// <summary>
        /// Gets the affinity key.
        /// </summary>
        public object Affinity
        {
            get { return _affinity ?? _key; }
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        public bool Equals(AffinityKey other)
        {
            return Equals(_key, other._key) && Equals(_affinity, other._affinity);
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
            return obj is AffinityKey && Equals((AffinityKey) obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return ((_key != null ? _key.GetHashCode() : 0)*397) ^
                    (_affinity != null ? _affinity.GetHashCode() : 0);
            }
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(AffinityKey left, AffinityKey right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(AffinityKey left, AffinityKey right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("AffinityKey [Key={0}, Affinity={1}]", _key, _affinity);
        }
    }
}

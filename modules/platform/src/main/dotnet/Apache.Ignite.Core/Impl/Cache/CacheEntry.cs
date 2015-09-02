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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Represents a cache entry.
    /// </summary>
    internal struct CacheEntry<TK, TV> : ICacheEntry<TK, TV>
    {
        /** Key. */
        private readonly TK _key;

        /** Value. */
        private readonly TV _val;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntry{K,V}"/> struct.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        public CacheEntry(TK key, TV val)
        {
            _key = key;
            _val = val;
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public TK Key
        {
            get { return _key; }
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public TV Value
        {
            get { return _val; }
        }

        /// <summary>
        /// Determines whether the specified <see cref="CacheEntry{K,V}"/>, is equal to this instance.
        /// </summary>
        /// <param name="other">The <see cref="CacheEntry{K,V}"/> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="CacheEntry{K,V}"/> is equal to this instance; 
        ///   otherwise, <c>false</c>.
        /// </returns>
        public bool Equals(CacheEntry<TK, TV> other)
        {
            return EqualityComparer<TK>.Default.Equals(_key, other._key) &&
                EqualityComparer<TV>.Default.Equals(_val, other._val);
        }
        
        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) 
                return false;

            return obj is CacheEntry<TK, TV> && Equals((CacheEntry<TK, TV>) obj);
        }
        
        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<TK>.Default.GetHashCode(_key) * 397) ^
                    EqualityComparer<TV>.Default.GetHashCode(_val);
            }
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format("CacheEntry [Key={0}, Value={1}]", _key, _val);
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="a">First item.</param>
        /// <param name="b">Second item.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(CacheEntry<TK, TV> a, CacheEntry<TK, TV> b)
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
        public static bool operator !=(CacheEntry<TK, TV> a, CacheEntry<TK, TV> b)
        {
            return !(a == b);
        }
    }
}
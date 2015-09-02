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
    internal struct CacheEntry<K, V> : ICacheEntry<K, V>
    {
        /** Key. */
        private readonly K key;

        /** Value. */
        private readonly V val;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntry{K,V}"/> struct.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        public CacheEntry(K key, V val)
        {
            this.key = key;
            this.val = val;
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public K Key
        {
            get { return key; }
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public V Value
        {
            get { return val; }
        }

        /// <summary>
        /// Determines whether the specified <see cref="CacheEntry{K,V}"/>, is equal to this instance.
        /// </summary>
        /// <param name="other">The <see cref="CacheEntry{K,V}"/> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="CacheEntry{K,V}"/> is equal to this instance; 
        ///   otherwise, <c>false</c>.
        /// </returns>
        public bool Equals(CacheEntry<K, V> other)
        {
            return EqualityComparer<K>.Default.Equals(key, other.key) &&
                EqualityComparer<V>.Default.Equals(val, other.val);
        }
        
        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) 
                return false;

            return obj is CacheEntry<K, V> && Equals((CacheEntry<K, V>) obj);
        }
        
        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<K>.Default.GetHashCode(key) * 397) ^
                    EqualityComparer<V>.Default.GetHashCode(val);
            }
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format("CacheEntry [Key={0}, Value={1}]", key, val);
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="a">First item.</param>
        /// <param name="b">Second item.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(CacheEntry<K, V> a, CacheEntry<K, V> b)
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
        public static bool operator !=(CacheEntry<K, V> a, CacheEntry<K, V> b)
        {
            return !(a == b);
        }
    }
}
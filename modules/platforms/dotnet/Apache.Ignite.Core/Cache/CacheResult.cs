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

    /// <summary>
    /// Represents a cache operation result with a success flag.
    /// </summary>
    /// <typeparam name="T">Operation result value type.</typeparam>
    public struct CacheResult<T> : IEquatable<CacheResult<T>>
    {
        /** */
        private readonly T _value;

        /** */
        private readonly bool _success;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheResult{T}"/> struct with a specified value 
        /// and sets success flag to true.
        /// </summary>
        /// <param name="value">The value.</param>
        public CacheResult(T value)
        {
            _value = value;
            _success = true;
        }

        /// <summary>
        /// Gets the cache value.
        /// </summary>
        public T Value
        {
            get { return _value; }
        }

        /// <summary>
        /// Gets a value indicating whether the operation completed successfully.
        /// </summary>
        public bool Success
        {
            get { return _success; }
        }

        /** <inehritdoc /> */
        public bool Equals(CacheResult<T> other)
        {
            return EqualityComparer<T>.Default.Equals(_value, other._value) && _success == other._success;
        }

        /** <inehritdoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is CacheResult<T> && Equals((CacheResult<T>) obj);
        }

        /** <inehritdoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<T>.Default.GetHashCode(_value)*397) ^ _success.GetHashCode();
            }
        }

        /** <inehritdoc /> */
        public static bool operator ==(CacheResult<T> left, CacheResult<T> right)
        {
            return left.Equals(right);
        }

        /** <inehritdoc /> */
        public static bool operator !=(CacheResult<T> left, CacheResult<T> right)
        {
            return !left.Equals(right);
        }
    }
}

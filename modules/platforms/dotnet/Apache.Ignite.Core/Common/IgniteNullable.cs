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
    using System.Collections.Generic;

    /// <summary>
    /// Represents a nullable which works the same way with value and reference types to unify logic.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    public struct IgniteNullable<T> : IEquatable<IgniteNullable<T>>
    {
        /** */
        private readonly T _value;

        /** */
        private readonly bool _hasValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteNullable{T}"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="hasValue">Has value flag.</param>
        public IgniteNullable(T value, bool hasValue)
        {
            _value = value;
            _hasValue = hasValue;
        }

        /// <summary>
        /// Gets the cache value.
        /// </summary>
        public T Value
        {
            get
            {
                if (!HasValue)
                    throw new InvalidOperationException("Nullable object must have a value.");

                return _value;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current object has a value.
        /// </summary>
        public bool HasValue
        {
            get { return _hasValue; }
        }

        /** <inehritdoc /> */
        public bool Equals(IgniteNullable<T> other)
        {
            return EqualityComparer<T>.Default.Equals(_value, other._value) && _hasValue == other._hasValue;
        }

        /** <inehritdoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is IgniteNullable<T> && Equals((IgniteNullable<T>) obj);
        }

        /** <inehritdoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<T>.Default.GetHashCode(_value)*397) ^ _hasValue.GetHashCode();
            }
        }

        /** <inehritdoc /> */
        public static bool operator ==(IgniteNullable<T> left, IgniteNullable<T> right)
        {
            return left.Equals(right);
        }

        /** <inehritdoc /> */
        public static bool operator !=(IgniteNullable<T> left, IgniteNullable<T> right)
        {
            return !left.Equals(right);
        }
    }
}

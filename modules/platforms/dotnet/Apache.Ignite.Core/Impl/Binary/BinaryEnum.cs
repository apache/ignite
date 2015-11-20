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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents a typed enum in binary form.
    /// </summary>
    internal class BinaryEnum : IBinaryObject, IEquatable<BinaryEnum>
    {
        /** Type id. */
        private readonly int _typeId;

        /** Value. */
        private readonly int _value;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryEnum" /> class.
        /// </summary>
        /// <param name="typeId">The type identifier.</param>
        /// <param name="value">The value.</param>
        /// <param name="marsh">The marshaller.</param>
        public BinaryEnum(int typeId, int value, Marshaller marsh)
        {
            Debug.Assert(marsh != null);

            _typeId = typeId;
            _value = value;
            _marsh = marsh;
        }

        /** <inheritdoc /> */
        public int TypeId
        {
            get { return _typeId; }
        }

        /** <inheritdoc /> */
        public IBinaryType GetBinaryType()
        {
            return _marsh.GetBinaryType(_typeId);
        }

        /** <inheritdoc /> */
        public TF GetField<TF>(string fieldName)
        {
            if (fieldName != null)
                throw new NotSupportedException("Enum in binary form has no fields. " +
                                                "Use null field name to retrieve integer enum value.");

            return TypeCaster<TF>.Cast(_value);
        }

        /** <inheritdoc /> */
        public T Deserialize<T>()
        {
            // TODO: Type validation
            return TypeCaster<T>.Cast(_value);
        }

        /** <inheritdoc /> */
        public bool Equals(BinaryEnum other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return _typeId == other._typeId && _value == other._value;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            if (obj.GetType() != GetType())
                return false;

            return Equals((BinaryEnum) obj);
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        /** <inheritdoc /> */
        public static bool operator ==(BinaryEnum left, BinaryEnum right)
        {
            return Equals(left, right);
        }

        /** <inheritdoc /> */
        public static bool operator !=(BinaryEnum left, BinaryEnum right)
        {
            return !Equals(left, right);
        }
    }
}

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
    using Apache.Ignite.Core.Impl.Binary.Metadata;

    /// <summary>
    /// Represents a typed enum in binary form.
    /// </summary>
    internal class BinaryEnum : IBinaryObject, IEquatable<BinaryEnum>
    {
        /** Type id. */
        private readonly int _typeId;

        /** Value. */
        private readonly int _enumValue;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryEnum" /> class.
        /// </summary>
        /// <param name="typeId">The type identifier.</param>
        /// <param name="enumValue">The value.</param>
        /// <param name="marsh">The marshaller.</param>
        public BinaryEnum(int typeId, int enumValue, Marshaller marsh)
        {
            Debug.Assert(marsh != null);

            _typeId = typeId;
            _enumValue = enumValue;
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
            return default(TF);
        }

        /** <inheritdoc /> */
        public bool HasField(string fieldName)
        {
            return false;
        }

        /** <inheritdoc /> */
        public T Deserialize<T>()
        {
            return BinaryUtils.GetEnumValue<T>(_enumValue, _typeId, _marsh);
        }

        /** <inheritdoc /> */
        public int EnumValue
        {
            get { return _enumValue; }
        }

        /** <inheritdoc /> */
        public string EnumName
        {
            get { return _marsh.GetBinaryType(_typeId).GetEnumName(_enumValue); }
        }

        /** <inheritdoc /> */
        public IBinaryObjectBuilder ToBuilder()
        {
            throw new NotSupportedException("Builder cannot be created for enum.");
        }

        /** <inheritdoc /> */
        public bool Equals(BinaryEnum other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return _typeId == other._typeId && _enumValue == other._enumValue;
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
            return _enumValue.GetHashCode();
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            var meta = GetBinaryType();

            if (meta == null || meta == BinaryType.Empty)
            {
                return string.Format("BinaryEnum [typeId={0}, enumValue={1}]", _typeId, _enumValue);
            }

            return string.Format("{0} [typeId={1}, enumValue={2}, enumValueName={3}]",
                meta.TypeName, _typeId, _enumValue, EnumName);
        }
    }
}

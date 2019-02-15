/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

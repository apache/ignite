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

namespace Apache.Ignite.Core.Impl.Binary.Metadata
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary metadata implementation.
    /// </summary>
    internal class BinaryType : IBinaryType
    {
        /** Empty metadata. */
        public static readonly BinaryType Empty =
            new BinaryType(BinaryUtils.TypeObject, BinaryTypeNames.TypeNameObject, null, null, false, null, null);

        /** Empty dictionary. */
        private static readonly IDictionary<string, BinaryField> EmptyDict = new Dictionary<string, BinaryField>();

        /** Empty list. */
        private static readonly ICollection<string> EmptyList = new List<string>().AsReadOnly();

        /** Type name map. */
        private static readonly string[] TypeNames = new string[byte.MaxValue];

        /** Fields. */
        private readonly IDictionary<string, BinaryField> _fields;

        /** Enum values. */
        private readonly IDictionary<string, int> _enumNameToValue;

        /** Enum names. */
        private readonly IDictionary<int, string> _enumValueToName;

        /** Enum flag. */
        private readonly bool _isEnum;

        /** Type id. */
        private readonly int _typeId;

        /** Type name. */
        private readonly string _typeName;

        /** Aff key field name. */
        private readonly string _affinityKeyFieldName;

        /** Type descriptor. */
        private readonly IBinaryTypeDescriptor _descriptor;

        /** Marshaller. */
        private readonly Marshaller _marshaller;

        /// <summary>
        /// Initializes the <see cref="BinaryType"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline",
            Justification = "Readability.")]
        static BinaryType()
        {
            TypeNames[BinaryUtils.TypeBool] = BinaryTypeNames.TypeNameBool;
            TypeNames[BinaryUtils.TypeByte] = BinaryTypeNames.TypeNameByte;
            TypeNames[BinaryUtils.TypeShort] = BinaryTypeNames.TypeNameShort;
            TypeNames[BinaryUtils.TypeChar] = BinaryTypeNames.TypeNameChar;
            TypeNames[BinaryUtils.TypeInt] = BinaryTypeNames.TypeNameInt;
            TypeNames[BinaryUtils.TypeLong] = BinaryTypeNames.TypeNameLong;
            TypeNames[BinaryUtils.TypeFloat] = BinaryTypeNames.TypeNameFloat;
            TypeNames[BinaryUtils.TypeDouble] = BinaryTypeNames.TypeNameDouble;
            TypeNames[BinaryUtils.TypeDecimal] = BinaryTypeNames.TypeNameDecimal;
            TypeNames[BinaryUtils.TypeString] = BinaryTypeNames.TypeNameString;
            TypeNames[BinaryUtils.TypeGuid] = BinaryTypeNames.TypeNameGuid;
            TypeNames[BinaryUtils.TypeTimestamp] = BinaryTypeNames.TypeNameTimestamp;
            TypeNames[BinaryUtils.TypeEnum] = BinaryTypeNames.TypeNameEnum;
            TypeNames[BinaryUtils.TypeObject] = BinaryTypeNames.TypeNameObject;
            TypeNames[BinaryUtils.TypeArrayBool] = BinaryTypeNames.TypeNameArrayBool;
            TypeNames[BinaryUtils.TypeArrayByte] = BinaryTypeNames.TypeNameArrayByte;
            TypeNames[BinaryUtils.TypeArrayShort] = BinaryTypeNames.TypeNameArrayShort;
            TypeNames[BinaryUtils.TypeArrayChar] = BinaryTypeNames.TypeNameArrayChar;
            TypeNames[BinaryUtils.TypeArrayInt] = BinaryTypeNames.TypeNameArrayInt;
            TypeNames[BinaryUtils.TypeArrayLong] = BinaryTypeNames.TypeNameArrayLong;
            TypeNames[BinaryUtils.TypeArrayFloat] = BinaryTypeNames.TypeNameArrayFloat;
            TypeNames[BinaryUtils.TypeArrayDouble] = BinaryTypeNames.TypeNameArrayDouble;
            TypeNames[BinaryUtils.TypeArrayDecimal] = BinaryTypeNames.TypeNameArrayDecimal;
            TypeNames[BinaryUtils.TypeArrayString] = BinaryTypeNames.TypeNameArrayString;
            TypeNames[BinaryUtils.TypeArrayGuid] = BinaryTypeNames.TypeNameArrayGuid;
            TypeNames[BinaryUtils.TypeArrayTimestamp] = BinaryTypeNames.TypeNameArrayTimestamp;
            TypeNames[BinaryUtils.TypeArrayEnum] = BinaryTypeNames.TypeNameArrayEnum;
            TypeNames[BinaryUtils.TypeArray] = BinaryTypeNames.TypeNameArrayObject;
            TypeNames[BinaryUtils.TypeCollection] = BinaryTypeNames.TypeNameCollection;
            TypeNames[BinaryUtils.TypeDictionary] = BinaryTypeNames.TypeNameMap;
        }

        /// <summary>
        /// Get type name by type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Type name.</returns>
        private static string GetTypeName(int typeId)
        {
            var typeName = (typeId >= 0 && typeId < TypeNames.Length) ? TypeNames[typeId] : null;

            if (typeName != null)
                return typeName;

            throw new BinaryObjectException("Invalid type ID: " + typeId);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryType" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public BinaryType(BinaryReader reader)
        {
            _typeId = reader.ReadInt();
            _typeName = reader.ReadString();
            _affinityKeyFieldName = reader.ReadString();

            int fieldsNum = reader.ReadInt();

            _fields = new Dictionary<string, BinaryField>(fieldsNum);

            for (int i = 0; i < fieldsNum; ++i)
            {
                string name = reader.ReadString();
                BinaryField field = new BinaryField(reader);

                _fields[name] = field;
            }
            
            _isEnum = reader.ReadBoolean();

            if (_isEnum)
            {
                var count = reader.ReadInt();

                _enumNameToValue = new Dictionary<string, int>(count);

                for (var i = 0; i < count; i++)
                {
                    _enumNameToValue[reader.ReadString()] = reader.ReadInt();
                }

                _enumValueToName = _enumNameToValue.ToDictionary(x => x.Value, x => x.Key);
            }

            _marshaller = reader.Marshaller;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryType" /> class.
        /// </summary>
        /// <param name="desc">Descriptor.</param>
        /// <param name="marshaller">Marshaller.</param>
        /// <param name="fields">Fields.</param>
        public BinaryType(IBinaryTypeDescriptor desc, Marshaller marshaller, 
            IDictionary<string, BinaryField> fields = null) 
            : this (desc.TypeId, desc.TypeName, fields, desc.AffinityKeyFieldName, desc.IsEnum, 
                  GetEnumValues(desc), marshaller)
        {
            _descriptor = desc;
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="fields">Fields.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="isEnum">Enum flag.</param>
        /// <param name="enumValues">Enum values.</param>
        /// <param name="marshaller">Marshaller.</param>
        public BinaryType(int typeId, string typeName, IDictionary<string, BinaryField> fields,
            string affKeyFieldName, bool isEnum, IDictionary<string, int> enumValues, Marshaller marshaller)
        {
            _typeId = typeId;
            _typeName = typeName;
            _affinityKeyFieldName = affKeyFieldName;
            _fields = fields;
            _isEnum = isEnum;
            _enumNameToValue = enumValues;

            if (_enumNameToValue != null)
            {
                _enumValueToName = _enumNameToValue.ToDictionary(x => x.Value, x => x.Key);
            }

            _marshaller = marshaller;
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        /// <returns></returns>
        public int TypeId
        {
            get { return _typeId; }
        }

        /// <summary>
        /// Gets type name.
        /// </summary>
        public string TypeName
        {
            get { return _typeName; }
        }

        /// <summary>
        /// Gets field names for that type.
        /// </summary>
        public ICollection<string> Fields
        {
            get { return _fields != null ? _fields.Keys : EmptyList; }
        }

        /// <summary>
        /// Gets field type for the given field name.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>
        /// Field type.
        /// </returns>
        public string GetFieldTypeName(string fieldName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(fieldName, "fieldName");

            if (_fields != null)
            {
                BinaryField fieldMeta;

                if (!_fields.TryGetValue(fieldName, out fieldMeta))
                {
                    throw new BinaryObjectException("BinaryObject field does not exist: " + fieldName);
                }

                return GetTypeName(fieldMeta.TypeId);
            }
            
            return null;
        }

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName
        {
            get { return _affinityKeyFieldName; }
        }

        /** <inheritdoc /> */
        public bool IsEnum
        {
            get { return _isEnum; }
        }

        /** <inheritdoc /> */
        public IEnumerable<IBinaryObject> GetEnumValues()
        {
            if (!_isEnum)
            {
                throw new NotSupportedException(
                    "IBinaryObject.Value is only supported for enums. " +
                    "Check IBinaryObject.GetBinaryType().IsEnum property before accessing Value.");
            }

            if (_marshaller == null)
            {
                yield break;
            }

            foreach (var pair in _enumValueToName)
            {
                yield return new BinaryEnum(_typeId, pair.Key, _marshaller);
            }
        }

        /// <summary>
        /// Gets the descriptor.
        /// </summary>
        public IBinaryTypeDescriptor Descriptor
        {
            get { return _descriptor; }
        }

        /// <summary>
        /// Gets fields map.
        /// </summary>
        /// <returns>Fields map.</returns>
        public IDictionary<string, BinaryField> GetFieldsMap()
        {
            return _fields ?? EmptyDict;
        }

        /// <summary>
        /// Gets the enum values map.
        /// </summary>
        public IDictionary<string, int> EnumValuesMap
        {
            get { return _enumNameToValue; }
        }

        /// <summary>
        /// Updates the fields.
        /// </summary>
        public void UpdateFields(IDictionary<string, BinaryField> fields)
        {
            if (fields == null || fields.Count == 0)
                return;

            Debug.Assert(_fields != null);

            foreach (var field in fields)
                _fields[field.Key] = field.Value;
        }

        /// <summary>
        /// Gets the enum value by name.
        /// </summary>
        public int? GetEnumValue(string valueName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(valueName, "valueName");

            if (!_isEnum)
            {
                throw new NotSupportedException("Can't get enum value for a non-enum type: " + _typeName);
            }

            int res;

            return _enumNameToValue != null && _enumNameToValue.TryGetValue(valueName, out res) ? res : (int?) null;
        }

        /// <summary>
        /// Gets the name of the enum value.
        /// </summary>
        public string GetEnumName(int value)
        {
            if (!_isEnum)
            {
                throw new NotSupportedException("Can't get enum value for a non-enum type: " + _typeName);
            }

            string res;

            return _enumValueToName != null && _enumValueToName.TryGetValue(value, out res) ? res : null;
        }

        /// <summary>
        /// Gets the enum values.
        /// </summary>
        private static IDictionary<string, int> GetEnumValues(IBinaryTypeDescriptor desc)
        {
            if (desc == null || desc.Type == null || !desc.IsEnum)
            {
                return null;
            }

            var enumType = desc.Type;

            var values = Enum.GetValues(enumType);
            var res = new Dictionary<string, int>(values.Length);

            var underlyingType = Enum.GetUnderlyingType(enumType);

            foreach (var value in values)
            {
                var name = Enum.GetName(enumType, value);
                Debug.Assert(name != null);

                res[name] = GetEnumValueAsInt(underlyingType, value);
            }

            return res;
        }

        /// <summary>
        /// Gets the enum value as int.
        /// </summary>
        private static int GetEnumValueAsInt(Type underlyingType, object value)
        {
            if (underlyingType == typeof(int))
            {
                return (int) value;
            }

            if (underlyingType == typeof(byte))
            {
                return (byte) value;
            }

            if (underlyingType == typeof(sbyte))
            {
                return (sbyte) value;
            }

            if (underlyingType == typeof(short))
            {
                return (short) value;
            }

            if (underlyingType == typeof(ushort))
            {
                return (ushort) value;
            }

            if (underlyingType == typeof(uint))
            {
                return unchecked((int) (uint) value);
            }

            throw new BinaryObjectException("Unexpected enum underlying type: " + underlyingType);
        }
    }
}

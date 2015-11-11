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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Binary metadata implementation.
    /// </summary>
    internal class BinaryType : IBinaryType
    {
        /** Empty metadata. */
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly BinaryType EmptyMeta =
            new BinaryType(BinaryUtils.TypeObject, BinaryTypeNames.TypeNameObject, null, null);

        /** Empty dictionary. */
        private static readonly IDictionary<string, int> EmptyDict = new Dictionary<string, int>();

        /** Empty list. */
        private static readonly ICollection<string> EmptyList = new List<string>().AsReadOnly();

        /** Fields. */
        private readonly IDictionary<string, int> _fields;

        /// <summary>
        /// Get type name by type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Type name.</returns>
        private static string ConvertTypeName(int typeId)
        {
            switch (typeId)
            {
                case BinaryUtils.TypeBool:
                    return BinaryTypeNames.TypeNameBool;
                case BinaryUtils.TypeByte:
                    return BinaryTypeNames.TypeNameByte;
                case BinaryUtils.TypeShort:
                    return BinaryTypeNames.TypeNameShort;
                case BinaryUtils.TypeChar:
                    return BinaryTypeNames.TypeNameChar;
                case BinaryUtils.TypeInt:
                    return BinaryTypeNames.TypeNameInt;
                case BinaryUtils.TypeLong:
                    return BinaryTypeNames.TypeNameLong;
                case BinaryUtils.TypeFloat:
                    return BinaryTypeNames.TypeNameFloat;
                case BinaryUtils.TypeDouble:
                    return BinaryTypeNames.TypeNameDouble;
                case BinaryUtils.TypeDecimal:
                    return BinaryTypeNames.TypeNameDecimal;
                case BinaryUtils.TypeString:
                    return BinaryTypeNames.TypeNameString;
                case BinaryUtils.TypeGuid:
                    return BinaryTypeNames.TypeNameGuid;
                case BinaryUtils.TypeTimestamp:
                    return BinaryTypeNames.TypeNameTimestamp;
                case BinaryUtils.TypeEnum:
                    return BinaryTypeNames.TypeNameEnum;
                case BinaryUtils.TypeBinary:
                case BinaryUtils.TypeObject:
                    return BinaryTypeNames.TypeNameObject;
                case BinaryUtils.TypeArrayBool:
                    return BinaryTypeNames.TypeNameArrayBool;
                case BinaryUtils.TypeArrayByte:
                    return BinaryTypeNames.TypeNameArrayByte;
                case BinaryUtils.TypeArrayShort:
                    return BinaryTypeNames.TypeNameArrayShort;
                case BinaryUtils.TypeArrayChar:
                    return BinaryTypeNames.TypeNameArrayChar;
                case BinaryUtils.TypeArrayInt:
                    return BinaryTypeNames.TypeNameArrayInt;
                case BinaryUtils.TypeArrayLong:
                    return BinaryTypeNames.TypeNameArrayLong;
                case BinaryUtils.TypeArrayFloat:
                    return BinaryTypeNames.TypeNameArrayFloat;
                case BinaryUtils.TypeArrayDouble:
                    return BinaryTypeNames.TypeNameArrayDouble;
                case BinaryUtils.TypeArrayDecimal:
                    return BinaryTypeNames.TypeNameArrayDecimal;
                case BinaryUtils.TypeArrayString:
                    return BinaryTypeNames.TypeNameArrayString;
                case BinaryUtils.TypeArrayGuid:
                    return BinaryTypeNames.TypeNameArrayGuid;
                case BinaryUtils.TypeArrayTimestamp:
                    return BinaryTypeNames.TypeNameArrayTimestamp;
                case BinaryUtils.TypeArrayEnum:
                    return BinaryTypeNames.TypeNameArrayEnum;
                case BinaryUtils.TypeArray:
                    return BinaryTypeNames.TypeNameArrayObject;
                case BinaryUtils.TypeCollection:
                    return BinaryTypeNames.TypeNameCollection;
                case BinaryUtils.TypeDictionary:
                    return BinaryTypeNames.TypeNameMap;
                default:
                    throw new BinaryObjectException("Invalid type ID: " + typeId);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryType" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public BinaryType(IBinaryRawReader reader)
        {
            TypeId = reader.ReadInt();
            TypeName = reader.ReadString();
            AffinityKeyFieldName = reader.ReadString();
            _fields = reader.ReadDictionaryAsGeneric<string, int>();
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="fields">Fields.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        public BinaryType(int typeId, string typeName, IDictionary<string, int> fields,
            string affKeyFieldName)
        {
            TypeId = typeId;
            TypeName = typeName;
            AffinityKeyFieldName = affKeyFieldName;
            _fields = fields;
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        /// <returns></returns>
        public int TypeId { get; private set; }

        /// <summary>
        /// Gets type name.
        /// </summary>
        public string TypeName { get; private set; }

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
            if (_fields != null)
            {
                int typeId;

                _fields.TryGetValue(fieldName, out typeId);

                return ConvertTypeName(typeId);
            }
            
            return null;
        }

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName { get; private set; }

        /// <summary>
        /// Gets fields map.
        /// </summary>
        /// <returns>Fields map.</returns>
        public IDictionary<string, int> FieldsMap()
        {
            return _fields ?? EmptyDict;
        }
    }
}

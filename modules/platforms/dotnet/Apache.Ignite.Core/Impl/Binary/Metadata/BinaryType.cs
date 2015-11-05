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
            new BinaryType(BinaryUtils.TypeObject, PortableTypeNames.TypeNameObject, null, null);

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
                    return PortableTypeNames.TypeNameBool;
                case BinaryUtils.TypeByte:
                    return PortableTypeNames.TypeNameByte;
                case BinaryUtils.TypeShort:
                    return PortableTypeNames.TypeNameShort;
                case BinaryUtils.TypeChar:
                    return PortableTypeNames.TypeNameChar;
                case BinaryUtils.TypeInt:
                    return PortableTypeNames.TypeNameInt;
                case BinaryUtils.TypeLong:
                    return PortableTypeNames.TypeNameLong;
                case BinaryUtils.TypeFloat:
                    return PortableTypeNames.TypeNameFloat;
                case BinaryUtils.TypeDouble:
                    return PortableTypeNames.TypeNameDouble;
                case BinaryUtils.TypeDecimal:
                    return PortableTypeNames.TypeNameDecimal;
                case BinaryUtils.TypeString:
                    return PortableTypeNames.TypeNameString;
                case BinaryUtils.TypeGuid:
                    return PortableTypeNames.TypeNameGuid;
                case BinaryUtils.TypeTimestamp:
                    return PortableTypeNames.TypeNameTimestamp;
                case BinaryUtils.TypeEnum:
                    return PortableTypeNames.TypeNameEnum;
                case BinaryUtils.TypePortable:
                case BinaryUtils.TypeObject:
                    return PortableTypeNames.TypeNameObject;
                case BinaryUtils.TypeArrayBool:
                    return PortableTypeNames.TypeNameArrayBool;
                case BinaryUtils.TypeArrayByte:
                    return PortableTypeNames.TypeNameArrayByte;
                case BinaryUtils.TypeArrayShort:
                    return PortableTypeNames.TypeNameArrayShort;
                case BinaryUtils.TypeArrayChar:
                    return PortableTypeNames.TypeNameArrayChar;
                case BinaryUtils.TypeArrayInt:
                    return PortableTypeNames.TypeNameArrayInt;
                case BinaryUtils.TypeArrayLong:
                    return PortableTypeNames.TypeNameArrayLong;
                case BinaryUtils.TypeArrayFloat:
                    return PortableTypeNames.TypeNameArrayFloat;
                case BinaryUtils.TypeArrayDouble:
                    return PortableTypeNames.TypeNameArrayDouble;
                case BinaryUtils.TypeArrayDecimal:
                    return PortableTypeNames.TypeNameArrayDecimal;
                case BinaryUtils.TypeArrayString:
                    return PortableTypeNames.TypeNameArrayString;
                case BinaryUtils.TypeArrayGuid:
                    return PortableTypeNames.TypeNameArrayGuid;
                case BinaryUtils.TypeArrayTimestamp:
                    return PortableTypeNames.TypeNameArrayTimestamp;
                case BinaryUtils.TypeArrayEnum:
                    return PortableTypeNames.TypeNameArrayEnum;
                case BinaryUtils.TypeArray:
                    return PortableTypeNames.TypeNameArrayObject;
                case BinaryUtils.TypeCollection:
                    return PortableTypeNames.TypeNameCollection;
                case BinaryUtils.TypeDictionary:
                    return PortableTypeNames.TypeNameMap;
                default:
                    throw new BinaryObjectException("Invalid type ID: " + typeId);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryType" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public BinaryType(IPortableRawReader reader)
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

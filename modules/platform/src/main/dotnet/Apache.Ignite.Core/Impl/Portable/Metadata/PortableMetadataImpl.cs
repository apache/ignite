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

namespace Apache.Ignite.Core.Impl.Portable.Metadata
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable metadata implementation.
    /// </summary>
    internal class PortableMetadataImpl : IPortableMetadata
    {
        /** Empty metadata. */
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly PortableMetadataImpl EmptyMeta =
            new PortableMetadataImpl(PortableUtils.TypeObject, PortableTypeNames.TypeNameObject, null, null);

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
                case PortableUtils.TypeBool:
                    return PortableTypeNames.TypeNameBool;
                case PortableUtils.TypeByte:
                    return PortableTypeNames.TypeNameByte;
                case PortableUtils.TypeShort:
                    return PortableTypeNames.TypeNameShort;
                case PortableUtils.TypeChar:
                    return PortableTypeNames.TypeNameChar;
                case PortableUtils.TypeInt:
                    return PortableTypeNames.TypeNameInt;
                case PortableUtils.TypeLong:
                    return PortableTypeNames.TypeNameLong;
                case PortableUtils.TypeFloat:
                    return PortableTypeNames.TypeNameFloat;
                case PortableUtils.TypeDouble:
                    return PortableTypeNames.TypeNameDouble;
                case PortableUtils.TypeDecimal:
                    return PortableTypeNames.TypeNameDecimal;
                case PortableUtils.TypeString:
                    return PortableTypeNames.TypeNameString;
                case PortableUtils.TypeGuid:
                    return PortableTypeNames.TypeNameGuid;
                case PortableUtils.TypeDate:
                    return PortableTypeNames.TypeNameDate;
                case PortableUtils.TypeEnum:
                    return PortableTypeNames.TypeNameEnum;
                case PortableUtils.TypePortable:
                case PortableUtils.TypeObject:
                    return PortableTypeNames.TypeNameObject;
                case PortableUtils.TypeArrayBool:
                    return PortableTypeNames.TypeNameArrayBool;
                case PortableUtils.TypeArrayByte:
                    return PortableTypeNames.TypeNameArrayByte;
                case PortableUtils.TypeArrayShort:
                    return PortableTypeNames.TypeNameArrayShort;
                case PortableUtils.TypeArrayChar:
                    return PortableTypeNames.TypeNameArrayChar;
                case PortableUtils.TypeArrayInt:
                    return PortableTypeNames.TypeNameArrayInt;
                case PortableUtils.TypeArrayLong:
                    return PortableTypeNames.TypeNameArrayLong;
                case PortableUtils.TypeArrayFloat:
                    return PortableTypeNames.TypeNameArrayFloat;
                case PortableUtils.TypeArrayDouble:
                    return PortableTypeNames.TypeNameArrayDouble;
                case PortableUtils.TypeArrayDecimal:
                    return PortableTypeNames.TypeNameArrayDecimal;
                case PortableUtils.TypeArrayString:
                    return PortableTypeNames.TypeNameArrayString;
                case PortableUtils.TypeArrayGuid:
                    return PortableTypeNames.TypeNameArrayGuid;
                case PortableUtils.TypeArrayDate:
                    return PortableTypeNames.TypeNameArrayDate;
                case PortableUtils.TypeArrayEnum:
                    return PortableTypeNames.TypeNameArrayEnum;
                case PortableUtils.TypeArray:
                    return PortableTypeNames.TypeNameArrayObject;
                case PortableUtils.TypeCollection:
                    return PortableTypeNames.TypeNameCollection;
                case PortableUtils.TypeDictionary:
                    return PortableTypeNames.TypeNameMap;
                default:
                    throw new PortableException("Invalid type ID: " + typeId);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableMetadataImpl" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableMetadataImpl(IPortableRawReader reader)
        {
            TypeId = reader.ReadInt();
            TypeName = reader.ReadString();
            AffinityKeyFieldName = reader.ReadString();
            _fields = reader.ReadGenericDictionary<string, int>();
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="fields">Fields.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        public PortableMetadataImpl(int typeId, string typeName, IDictionary<string, int> fields,
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
        public string FieldTypeName(string fieldName)
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

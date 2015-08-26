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
    public class PortableMetadataImpl : IPortableMetadata, IPortableWriteAware
    {
        /** Empty metadata. */
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly PortableMetadataImpl EMPTY_META =
            new PortableMetadataImpl(PortableUtils.TYPE_OBJECT, PortableTypeNames.TypeNameObject, null, null);

        /** Empty dictionary. */
        private static readonly IDictionary<string, int> EMPTY_DICT = new Dictionary<string, int>();

        /** Empty list. */
        private static readonly ICollection<string> EMPTY_LIST = new List<string>().AsReadOnly();

        /** Fields. */
        private readonly IDictionary<string, int> fields;

        /// <summary>
        /// Get type name by type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Type name.</returns>
        private static string ConvertTypeName(int typeId)
        {
            switch (typeId)
            {
                case PortableUtils.TYPE_BOOL:
                    return PortableTypeNames.TypeNameBool;
                case PortableUtils.TYPE_BYTE:
                    return PortableTypeNames.TypeNameByte;
                case PortableUtils.TYPE_SHORT:
                    return PortableTypeNames.TypeNameShort;
                case PortableUtils.TYPE_CHAR:
                    return PortableTypeNames.TypeNameChar;
                case PortableUtils.TYPE_INT:
                    return PortableTypeNames.TypeNameInt;
                case PortableUtils.TYPE_LONG:
                    return PortableTypeNames.TypeNameLong;
                case PortableUtils.TYPE_FLOAT:
                    return PortableTypeNames.TypeNameFloat;
                case PortableUtils.TYPE_DOUBLE:
                    return PortableTypeNames.TypeNameDouble;
                case PortableUtils.TYPE_DECIMAL:
                    return PortableTypeNames.TypeNameDecimal;
                case PortableUtils.TYPE_STRING:
                    return PortableTypeNames.TypeNameString;
                case PortableUtils.TYPE_GUID:
                    return PortableTypeNames.TypeNameGuid;
                case PortableUtils.TYPE_DATE:
                    return PortableTypeNames.TypeNameDate;
                case PortableUtils.TYPE_ENUM:
                    return PortableTypeNames.TypeNameEnum;
                case PortableUtils.TYPE_PORTABLE:
                case PortableUtils.TYPE_OBJECT:
                    return PortableTypeNames.TypeNameObject;
                case PortableUtils.TYPE_ARRAY_BOOL:
                    return PortableTypeNames.TypeNameArrayBool;
                case PortableUtils.TYPE_ARRAY_BYTE:
                    return PortableTypeNames.TypeNameArrayByte;
                case PortableUtils.TYPE_ARRAY_SHORT:
                    return PortableTypeNames.TypeNameArrayShort;
                case PortableUtils.TYPE_ARRAY_CHAR:
                    return PortableTypeNames.TypeNameArrayChar;
                case PortableUtils.TYPE_ARRAY_INT:
                    return PortableTypeNames.TypeNameArrayInt;
                case PortableUtils.TYPE_ARRAY_LONG:
                    return PortableTypeNames.TypeNameArrayLong;
                case PortableUtils.TYPE_ARRAY_FLOAT:
                    return PortableTypeNames.TypeNameArrayFloat;
                case PortableUtils.TYPE_ARRAY_DOUBLE:
                    return PortableTypeNames.TypeNameArrayDouble;
                case PortableUtils.TYPE_ARRAY_DECIMAL:
                    return PortableTypeNames.TypeNameArrayDecimal;
                case PortableUtils.TYPE_ARRAY_STRING:
                    return PortableTypeNames.TypeNameArrayString;
                case PortableUtils.TYPE_ARRAY_GUID:
                    return PortableTypeNames.TypeNameArrayGuid;
                case PortableUtils.TYPE_ARRAY_DATE:
                    return PortableTypeNames.TypeNameArrayDate;
                case PortableUtils.TYPE_ARRAY_ENUM:
                    return PortableTypeNames.TypeNameArrayEnum;
                case PortableUtils.TYPE_ARRAY:
                    return PortableTypeNames.TypeNameArrayObject;
                case PortableUtils.TYPE_COLLECTION:
                    return PortableTypeNames.TypeNameCollection;
                case PortableUtils.TYPE_DICTIONARY:
                    return PortableTypeNames.TypeNameMap;
                default:
                    throw new PortableException("Invalid type ID: " + typeId);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableMetadataImpl" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableMetadataImpl(IPortableReader reader)
        {
            var rawReader = reader.RawReader();

            TypeId = rawReader.ReadInt();
            TypeName = rawReader.ReadString();
            AffinityKeyFieldName = rawReader.ReadString();
            fields = rawReader.ReadGenericDictionary<string, int>();
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
            this.fields = fields;
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
            get { return fields != null ? fields.Keys : EMPTY_LIST; }
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
            if (fields != null)
            {
                int typeId;

                fields.TryGetValue(fieldName, out typeId);

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
            return fields ?? EMPTY_DICT;
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WritePortable(IPortableWriter writer)
        {
            IPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteInt(TypeId);
            rawWriter.WriteString(TypeName);
            rawWriter.WriteString(AffinityKeyFieldName);
            rawWriter.WriteGenericDictionary(fields);
        }
    }
}

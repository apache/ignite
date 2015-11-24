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

        /** Type name map. */
        private static readonly string[] TypeNames = new string[byte.MaxValue];

        /** Fields. */
        private readonly IDictionary<string, int> _fields;

        /// <summary>
        /// Initializes the <see cref="BinaryType"/> class.
        /// </summary>
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

                return GetTypeName(typeId);
            }
            
            return null;
        }

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName { get; private set; }

        /** <inheritdoc /> */
        public bool IsEnum
        {
            get { return false; }  // TODO
        }

        /// <summary>
        /// Gets fields map.
        /// </summary>
        /// <returns>Fields map.</returns>
        public IDictionary<string, int> GetFieldsMap()
        {
            return _fields ?? EmptyDict;
        }
    }
}

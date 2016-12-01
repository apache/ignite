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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.Structure;

    /// <summary>
    /// Full type descriptor.
    /// </summary> 
    internal class BinaryFullTypeDescriptor : IBinaryTypeDescriptor
    {
        /** Type. */
        private readonly Type _type;

        /** Type ID. */
        private readonly int _typeId;

        /** Type name. */
        private readonly string _typeName;

        /** User type flag. */
        private readonly bool _userType;

        /** Name converter. */
        private readonly IBinaryNameMapper _nameMapper;

        /** Mapper. */
        private readonly IBinaryIdMapper _idMapper;

        /** Serializer. */
        private readonly IBinarySerializerInternal _serializer;

        /** Whether to cache deserialized value in IBinaryObject */
        private readonly bool _keepDeserialized;

        /** Affinity field key name. */
        private readonly string _affKeyFieldName;

        /** Type structure. */
        private volatile BinaryStructure _writerTypeStruct = BinaryStructure.CreateEmpty();

        /** Type structure. */
        private volatile BinaryStructure _readerTypeStructure = BinaryStructure.CreateEmpty();
        
        /** Type schema. */
        private readonly BinaryObjectSchema _schema = new BinaryObjectSchema();

        /** Enum flag. */
        private readonly bool _isEnum;

        /** Comparer. */
        private readonly IBinaryEqualityComparer _equalityComparer;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="userType">User type flag.</param>
        /// <param name="nameMapper">Name converter.</param>
        /// <param name="idMapper">Mapper.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="keepDeserialized">Whether to cache deserialized value in IBinaryObject</param>
        /// <param name="affKeyFieldName">Affinity field key name.</param>
        /// <param name="isEnum">Enum flag.</param>
        /// <param name="comparer">Equality comparer.</param>
        public BinaryFullTypeDescriptor(
            Type type, 
            int typeId, 
            string typeName, 
            bool userType, 
            IBinaryNameMapper nameMapper, 
            IBinaryIdMapper idMapper,
            IBinarySerializerInternal serializer, 
            bool keepDeserialized, 
            string affKeyFieldName,
            bool isEnum, 
            IEqualityComparer<IBinaryObject> comparer)
        {
            _type = type;
            _typeId = typeId;
            _typeName = typeName;
            _userType = userType;
            _nameMapper = nameMapper;
            _idMapper = idMapper;
            _serializer = serializer;
            _keepDeserialized = keepDeserialized;
            _affKeyFieldName = affKeyFieldName;
            _isEnum = isEnum;

            _equalityComparer = comparer as IBinaryEqualityComparer;

            if (comparer != null && _equalityComparer == null)
                throw new IgniteException(string.Format("Unsupported IEqualityComparer<IBinaryObject> " +
                                                        "implementation: {0}. Only predefined implementations " +
                                                        "are supported.", comparer.GetType()));
        }

        /// <summary>
        /// Type.
        /// </summary>
        public Type Type
        {
            get { return _type; }
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        public int TypeId
        {
            get { return _typeId; }
        }

        /// <summary>
        /// Type name.
        /// </summary>
        public string TypeName
        {
            get { return _typeName; }
        }

        /// <summary>
        /// User type flag.
        /// </summary>
        public bool UserType
        {
            get { return _userType; }
        }

        /// <summary>
        /// Whether to cache deserialized value in IBinaryObject
        /// </summary>
        public bool KeepDeserialized
        {
            get { return _keepDeserialized; }
        }

        /// <summary>
        /// Name converter.
        /// </summary>
        public IBinaryNameMapper NameMapper
        {
            get { return _nameMapper; }
        }

        /// <summary>
        /// Mapper.
        /// </summary>
        public IBinaryIdMapper IdMapper
        {
            get { return _idMapper; }
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        public IBinarySerializerInternal Serializer
        {
            get { return _serializer; }
        }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName
        {
            get { return _affKeyFieldName; }
        }

        /** <inheritdoc/> */
        public bool IsEnum
        {
            get { return _isEnum; }
        }

        /** <inheritdoc/> */
        public IBinaryEqualityComparer EqualityComparer
        {
            get { return _equalityComparer; }
        }

        /** <inheritDoc /> */
        public BinaryStructure WriterTypeStructure
        {
            get { return _writerTypeStruct; }
        }

        /** <inheritDoc /> */
        public BinaryStructure ReaderTypeStructure
        {
            get { return _readerTypeStructure; }
        }

        /** <inheritDoc /> */
        public void UpdateWriteStructure(BinaryStructure exp, int pathIdx, 
            IList<BinaryStructureUpdate> updates)
        {
            lock (this)
            {
                _writerTypeStruct = _writerTypeStruct.Merge(exp, pathIdx, updates);
            }
        }

        /** <inheritDoc /> */
        public void UpdateReadStructure(BinaryStructure exp, int pathIdx, 
            IList<BinaryStructureUpdate> updates)
        {
            lock (this)
            {
                _readerTypeStructure = _readerTypeStructure.Merge(exp, pathIdx, updates);
            }
        }

        /** <inheritDoc /> */
        public BinaryObjectSchema Schema
        {
            get { return _schema; }
        }
    }
}

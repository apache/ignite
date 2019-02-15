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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
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
        private volatile BinaryStructure _writerTypeStruct;

        /** Type structure. */
        private volatile BinaryStructure _readerTypeStructure = BinaryStructure.CreateEmpty();
        
        /** Type schema. */
        private readonly BinaryObjectSchema _schema;

        /** Enum flag. */
        private readonly bool _isEnum;

        /** Register flag. */
        private readonly bool _isRegistered;

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
        /// <param name="isRegistered">Registered flag.</param>
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
            bool isRegistered = true)
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

            _isRegistered = isRegistered;
            _schema = new BinaryObjectSchema();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryFullTypeDescriptor"/> class,
        /// copying values from specified descriptor.
        /// </summary>
        /// <param name="desc">The descriptor to copy from.</param>
        /// <param name="type">Type.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="isRegistered">Registered flag.</param>
        public BinaryFullTypeDescriptor(BinaryFullTypeDescriptor desc, Type type,
            IBinarySerializerInternal serializer, bool isRegistered)
        {
            _type = type;
            _typeId = desc._typeId;
            _typeName = desc._typeName;
            _userType = desc._userType;
            _nameMapper = desc._nameMapper;
            _idMapper = desc._idMapper;
            _serializer = serializer;
            _keepDeserialized = desc._keepDeserialized;
            _affKeyFieldName = desc._affKeyFieldName;
            _isEnum = desc._isEnum;
            _isRegistered = isRegistered;

            _schema = desc._schema;
            _writerTypeStruct = desc._writerTypeStruct;
            _readerTypeStructure = desc._readerTypeStructure;
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
        public void UpdateWriteStructure(int pathIdx, IList<BinaryStructureUpdate> updates)
        {
            lock (this)
            {
                if (_writerTypeStruct == null)
                {
                    // Null struct serves as an indication of a binary type that has never been sent to the cluster,
                    // which is important for types without any fields.
                    _writerTypeStruct = BinaryStructure.CreateEmpty();
                }

                _writerTypeStruct = _writerTypeStruct.Merge(pathIdx, updates);
            }
        }

        /// <summary>
        /// Resets writer structure.
        /// </summary>
        public void ResetWriteStructure()
        {
            lock (this)
            {
                _writerTypeStruct = null;
            }
        }

        /** <inheritDoc /> */
        public void UpdateReadStructure(int pathIdx, IList<BinaryStructureUpdate> updates)
        {
            lock (this)
            {
                _readerTypeStructure = _readerTypeStructure.Merge(pathIdx, updates);
            }
        }

        /** <inheritDoc /> */
        public BinaryObjectSchema Schema
        {
            get { return _schema; }
        }

        /** <inheritDoc /> */
        public bool IsRegistered
        {
            get { return _isRegistered; }
        }
    }
}

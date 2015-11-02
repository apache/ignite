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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Collections.Generic;

    using Apache.Ignite.Core.Impl.Portable.Structure;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Full type descriptor.
    /// </summary> 
    internal class PortableFullTypeDescriptor : IPortableTypeDescriptor
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
        private readonly IPortableNameMapper _nameMapper;

        /** Mapper. */
        private readonly IPortableIdMapper _idMapper;

        /** Serializer. */
        private readonly IPortableSerializer _serializer;

        /** Whether to cache deserialized value in IPortableObject */
        private readonly bool _keepDeserialized;

        /** Affinity field key name. */
        private readonly string _affKeyFieldName;

        /** Type structure. */
        private volatile PortableStructure _writerTypeStruct = PortableStructure.CreateEmpty();

        /** Type structure. */
        private volatile PortableStructure _readerTypeStructure = PortableStructure.CreateEmpty();
        
        /** Type schema. */
        private readonly PortableObjectSchema _schema = new PortableObjectSchema();

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
        /// <param name="keepDeserialized">Whether to cache deserialized value in IPortableObject</param>
        /// <param name="affKeyFieldName">Affinity field key name.</param>
        public PortableFullTypeDescriptor(
            Type type, 
            int typeId, 
            string typeName, 
            bool userType, 
            IPortableNameMapper nameMapper, 
            IPortableIdMapper idMapper, 
            IPortableSerializer serializer, 
            bool keepDeserialized, 
            string affKeyFieldName)
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
        /// Whether to cache deserialized value in IPortableObject
        /// </summary>
        public bool KeepDeserialized
        {
            get { return _keepDeserialized; }
        }

        /// <summary>
        /// Name converter.
        /// </summary>
        public IPortableNameMapper NameMapper
        {
            get { return _nameMapper; }
        }

        /// <summary>
        /// Mapper.
        /// </summary>
        public IPortableIdMapper IdMapper
        {
            get { return _idMapper; }
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        public IPortableSerializer Serializer
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

        /** <inheritDoc /> */
        public PortableStructure WriterTypeStructure
        {
            get { return _writerTypeStruct; }
        }

        /** <inheritDoc /> */
        public PortableStructure ReaderTypeStructure
        {
            get { return _readerTypeStructure; }
        }

        /** <inheritDoc /> */
        public void UpdateWriteStructure(PortableStructure exp, int pathIdx, 
            IList<PortableStructureUpdate> updates)
        {
            lock (this)
            {
                _writerTypeStruct = _writerTypeStruct.Merge(exp, pathIdx, updates);
            }
        }

        /** <inheritDoc /> */
        public void UpdateReadStructure(PortableStructure exp, int pathIdx, 
            IList<PortableStructureUpdate> updates)
        {
            lock (this)
            {
                _readerTypeStructure = _readerTypeStructure.Merge(exp, pathIdx, updates);
            }
        }

        /** <inheritDoc /> */
        public PortableObjectSchema Schema
        {
            get { return _schema; }
        }
    }
}

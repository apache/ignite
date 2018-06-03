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
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.Structure;

    /// <summary>
    /// Surrogate type descriptor. Used in cases when type if identified by name and 
    /// is not provided in configuration.
    /// </summary>
    internal class BinarySurrogateTypeDescriptor : IBinaryTypeDescriptor
    {
        /** Binary configuration. */
        private readonly BinaryConfiguration _cfg;

        /** Type ID. */
        private readonly int _id;

        /** Type name. */
        private readonly string _name;

        /** Type structure. */
        private volatile BinaryStructure _writerTypeStruct = BinaryStructure.CreateEmpty();

        /** Type structure. */
        private BinaryStructure _readerTypeStructure = BinaryStructure.CreateEmpty();
        
        /** Type schema. */
        private readonly BinaryObjectSchema _schema = new BinaryObjectSchema();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="id">Type ID.</param>
        /// <param name="typeName">Name of the type.</param>
        public BinarySurrogateTypeDescriptor(BinaryConfiguration cfg, int id, string typeName)
        {
            Debug.Assert(cfg != null);

            _cfg = cfg;
            _id = id;
            _name = typeName;
        }

        /** <inheritDoc /> */
        public Type Type
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public int TypeId
        {
            get { return _id; }
        }

        /** <inheritDoc /> */
        public string TypeName
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public bool UserType
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public bool KeepDeserialized
        {
            get { return _cfg.KeepDeserialized; }
        }

        /** <inheritDoc /> */
        public IBinaryNameMapper NameMapper
        {
            get { return _cfg.NameMapper; }
        }

        /** <inheritDoc /> */
        public IBinaryIdMapper IdMapper
        {
            get { return _cfg.IdMapper; }
        }

        /** <inheritDoc /> */
        public IBinarySerializerInternal Serializer
        {
            get { return new UserSerializerProxy(_cfg.Serializer); }
        }

        /** <inheritDoc /> */
        public string AffinityKeyFieldName
        {
            get { return null; }
        }

        /** <inheritdoc/> */
        public bool IsEnum
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public BinaryStructure WriterTypeStructure
        {
            get { return _writerTypeStruct; }
        }

        public BinaryStructure ReaderTypeStructure
        {
            get { return _readerTypeStructure; }
        }

        /** <inheritDoc /> */
        public void UpdateWriteStructure(int pathIdx, IList<BinaryStructureUpdate> updates)
        {
            lock (this)
            {
                _writerTypeStruct = _writerTypeStruct.Merge(pathIdx, updates);
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
            get { return false; }
        }
    }
}

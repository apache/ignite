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
    /// Surrogate type descriptor. Used in cases when type if identified by name and 
    /// is not provided in configuration.
    /// </summary>
    internal class PortableSurrogateTypeDescriptor : IPortableTypeDescriptor
    {
        /** Portable configuration. */
        private readonly PortableConfiguration _cfg;

        /** Type ID. */
        private readonly int _id;

        /** Type name. */
        private readonly string _name;

        /** Type structure. */
        private volatile PortableStructure _writerTypeStruct = PortableStructure.CreateEmpty();

        /** Type structure. */
        private PortableStructure _readerTypeStructure = PortableStructure.CreateEmpty();
        
        /** Type schema. */
        private readonly PortableObjectSchema _schema = new PortableObjectSchema();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Portable configuration.</param>
        /// <param name="id">Type ID.</param>
        public PortableSurrogateTypeDescriptor(PortableConfiguration cfg, int id)
        {
            _cfg = cfg;
            _id = id;
        }

        /// <summary>
        /// Constrcutor.
        /// </summary>
        /// <param name="cfg">Portable configuration.</param>
        /// <param name="name">Type name.</param>
        public PortableSurrogateTypeDescriptor(PortableConfiguration cfg, string name)
        {
            _cfg = cfg;
            _name = name;

            _id = PortableUtils.TypeId(name, cfg.DefaultNameMapper, cfg.DefaultIdMapper);
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
            get { return _cfg.DefaultKeepDeserialized; }
        }

        /** <inheritDoc /> */
        public IPortableNameMapper NameMapper
        {
            get { return _cfg.DefaultNameMapper; }
        }

        /** <inheritDoc /> */
        public IPortableIdMapper IdMapper
        {
            get { return _cfg.DefaultIdMapper; }
        }

        /** <inheritDoc /> */
        public IPortableSerializer Serializer
        {
            get { return _cfg.DefaultSerializer; }
        }

        /** <inheritDoc /> */
        public string AffinityKeyFieldName
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public PortableStructure WriterTypeStructure
        {
            get { return _writerTypeStruct; }
        }

        public PortableStructure ReaderTypeStructure
        {
            get { return _readerTypeStructure; }
        }

        /** <inheritDoc /> */
        public void UpdateWriteStructure(PortableStructure exp, int pathIdx, IList<PortableStructureUpdate> updates)
        {
            lock (this)
            {
                _writerTypeStruct = _writerTypeStruct.Merge(exp, pathIdx, updates);
            }
        }

        /** <inheritDoc /> */
        public void UpdateReadStructure(PortableStructure exp, int pathIdx, IList<PortableStructureUpdate> updates)
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

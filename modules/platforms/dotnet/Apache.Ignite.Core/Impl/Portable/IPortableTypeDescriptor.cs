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
    /// Type descriptor.
    /// </summary>
    internal interface IPortableTypeDescriptor
    {
        /// <summary>
        /// Type.
        /// </summary>
        Type Type
        {
            get;
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        int TypeId
        {
            get;
        }

        /// <summary>
        /// Type name.
        /// </summary>
        string TypeName
        {
            get;
        }

        /// <summary>
        /// User type flag.
        /// </summary>
        bool UserType
        {
            get;
        }

        /// <summary>
        /// Whether to cache deserialized value in IPortableObject
        /// </summary>
        bool KeepDeserialized
        {
            get;
        }

        /// <summary>
        /// Name converter.
        /// </summary>
        IPortableNameMapper NameMapper
        {
            get;
        }

        /// <summary>
        /// Mapper.
        /// </summary>
        IPortableIdMapper IdMapper
        {
            get;
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        IPortableSerializer Serializer
        {
            get;
        }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        string AffinityKeyFieldName
        {
            get;
        }

        /// <summary>
        /// Write type structure.
        /// </summary>
        PortableStructure WriterTypeStructure { get; }

        /// <summary>
        /// Read type structure.
        /// </summary>
        PortableStructure ReaderTypeStructure { get; }

        /// <summary>
        /// Update write type structure.
        /// </summary>
        /// <param name="exp">Expected type structure.</param>
        /// <param name="pathIdx">Path index.</param>
        /// <param name="updates">Recorded updates.</param>
        void UpdateWriteStructure(PortableStructure exp, int pathIdx, IList<PortableStructureUpdate> updates);

        /// <summary>
        /// Update read type structure.
        /// </summary>
        /// <param name="exp">Expected type structure.</param>
        /// <param name="pathIdx">Path index.</param>
        /// <param name="updates">Recorded updates.</param>
        void UpdateReadStructure(PortableStructure exp, int pathIdx, IList<PortableStructureUpdate> updates);

        /// <summary>
        /// Gets the schema.
        /// </summary>
        PortableObjectSchema Schema { get; }
    }
}

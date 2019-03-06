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
    /// Type descriptor.
    /// </summary>
    internal interface IBinaryTypeDescriptor
    {
        /// <summary>
        /// Type.
        /// </summary>
        Type Type { get; }

        /// <summary>
        /// Type ID.
        /// </summary>
        int TypeId { get; }

        /// <summary>
        /// Type name.
        /// </summary>
        string TypeName { get; }

        /// <summary>
        /// User type flag.
        /// </summary>
        bool UserType { get; }

        /// <summary>
        /// Whether to cache deserialized value in IBinaryObject
        /// </summary>
        bool KeepDeserialized { get; }

        /// <summary>
        /// Name converter.
        /// </summary>
        IBinaryNameMapper NameMapper { get; }

        /// <summary>
        /// Mapper.
        /// </summary>
        IBinaryIdMapper IdMapper { get; }

        /// <summary>
        /// Serializer.
        /// </summary>
        IBinarySerializerInternal Serializer { get; }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        string AffinityKeyFieldName { get; }

        /// <summary>
        /// Gets a value indicating whether this descriptor represents an enum type.
        /// </summary>
        bool IsEnum { get; }

        /// <summary>
        /// Write type structure.
        /// </summary>
        BinaryStructure WriterTypeStructure { get; }

        /// <summary>
        /// Read type structure.
        /// </summary>
        BinaryStructure ReaderTypeStructure { get; }

        /// <summary>
        /// Update write type structure.
        /// </summary>
        /// <param name="pathIdx">Path index.</param>
        /// <param name="updates">Recorded updates.</param>
        void UpdateWriteStructure(int pathIdx, IList<BinaryStructureUpdate> updates);

        /// <summary>
        /// Update read type structure.
        /// </summary>
        /// <param name="pathIdx">Path index.</param>
        /// <param name="updates">Recorded updates.</param>
        void UpdateReadStructure(int pathIdx, IList<BinaryStructureUpdate> updates);

        /// <summary>
        /// Gets the schema.
        /// </summary>
        BinaryObjectSchema Schema { get; }

        /// <summary>
        /// Gets a value indicating whether this descriptor is registered in the cluster.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is registered; otherwise, <c>false</c>.
        /// </value>
        bool IsRegistered { get; }
    }
}

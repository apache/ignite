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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;

    /// <summary>
    /// Binary processor.
    /// </summary>
    internal interface IBinaryProcessor
    {
        /// <summary>
        /// Gets metadata for specified type.
        /// </summary>
        BinaryType GetBinaryType(int typeId);

        /// <summary>
        /// Gets metadata for all known types.
        /// </summary>
        List<IBinaryType> GetBinaryTypes();

        /// <summary>
        /// Put binary types to Grid.
        /// </summary>
        /// <param name="types">Binary types.</param>
        void PutBinaryTypes(ICollection<BinaryType> types);

        /// <summary>
        /// Registers the type.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <param name="typeName">The type name.</param>
        /// <returns>True if registration succeeded; otherwise, false.</returns>
        bool RegisterType(int id, string typeName);

        /// <summary>
        /// Registers the enum.
        /// </summary>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="values">The values.</param>
        /// <returns>Resulting binary type.</returns>
        BinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values);

        /// <summary>
        /// Gets the type name by id.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns>Type or null.</returns>
        string GetTypeName(int id);
    }
}
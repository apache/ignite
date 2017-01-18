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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Internal comparer interface for <see cref="BinaryTypeConfiguration.EqualityComparer"/> implementations,
    /// provides more efficient API.
    /// </summary>
    internal interface IBinaryEqualityComparer
    {
        /// <summary>
        /// Returns a hash code for the binary object in specified stream at specified position.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="startPos">Data start position (right after the header).</param>
        /// <param name="length">Data length (without header and schema).</param>
        /// <param name="schema">Schema holder.</param>
        /// <param name="schemaId">Schema identifier.</param>
        /// <param name="marshaller">Marshaller.</param>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>
        /// A hash code for the object in the stream.
        /// </returns>
        int GetHashCode(IBinaryStream stream, int startPos, int length, BinaryObjectSchemaHolder schema, int schemaId,
            Marshaller marshaller, IBinaryTypeDescriptor desc);

        /// <summary>
        /// Returns a value indicating that two binary object are equal.
        /// </summary>
        /// <param name="x">First object.</param>
        /// <param name="y">Second object.</param>
        /// <returns>True when objects are equal; otherwise false.</returns>
        bool Equals(IBinaryObject x, IBinaryObject y);
    }
}

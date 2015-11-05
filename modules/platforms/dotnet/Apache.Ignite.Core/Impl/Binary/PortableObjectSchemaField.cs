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
    using System.Runtime.InteropServices;

    /// <summary>
    /// Portable schema field DTO (as it is stored in a stream).
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 0)]
    internal struct PortableObjectSchemaField
    {
        /* Field ID */
        public readonly int Id;

        /** Offset. */
        public readonly int Offset;

        /** Size, equals to sizeof(PortableObjectSchemaField) */
        public const int Size = 8;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableObjectSchemaField"/> struct.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="offset">The offset.</param>
        public PortableObjectSchemaField(int id, int offset)
        {
            Id = id;
            Offset = offset;
        }
    }
}
/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Diagnostics;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Binary schema field DTO (as it is stored in a stream).
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 0)]
    internal struct BinaryObjectSchemaField
    {
        /* Field ID */
        public readonly int Id;

        /** Offset. */
        public readonly int Offset;

        /** Size, equals to sizeof(BinaryObjectSchemaField) */
        public const int Size = 8;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryObjectSchemaField"/> struct.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="offset">The offset.</param>
        public BinaryObjectSchemaField(int id, int offset)
        {
            Debug.Assert(offset >= 0);

            Id = id;
            Offset = offset;
        }
    }
}
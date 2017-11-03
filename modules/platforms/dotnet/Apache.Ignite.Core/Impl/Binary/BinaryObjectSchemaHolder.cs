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
    using System.Threading;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Shared schema holder.
    /// </summary>
    internal class BinaryObjectSchemaHolder
    {
        /** Current schema. */
        private static readonly ThreadLocal<BinaryObjectSchemaHolder> CurrentHolder =
            new ThreadLocal<BinaryObjectSchemaHolder>(() => new BinaryObjectSchemaHolder());

        /** Fields. */
        private BinaryObjectSchemaField[] _fields = new BinaryObjectSchemaField[32];

        /** Current field index. */
        private int _idx;

        /// <summary>
        /// Gets the schema holder for the current thread.
        /// </summary>
        public static BinaryObjectSchemaHolder Current
        {
            get { return CurrentHolder.Value; }
        }

        /// <summary>
        /// Adds a field to the holder.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <param name="offset">The offset.</param>
        public void PushField(int id, int offset)
        {
            if (_idx == _fields.Length)
                Array.Resize(ref _fields, _fields.Length * 2);

            _fields[_idx] = new BinaryObjectSchemaField(id, offset);

            _idx++;
        }

        /// <summary>
        /// Gets the start of a new schema
        /// </summary>
        public int PushSchema()
        {
            return _idx;
        }

        /// <summary>
        /// Resets schema position to specified index.
        /// </summary>
        public void PopSchema(int idx)
        {
            _idx = idx;
        }

        /// <summary>
        /// Writes collected schema to the stream and pops it.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="schemaOffset">The schema offset.</param>
        /// <param name="schemaId">The schema identifier.</param>
        /// <param name="flags">Flags according to offset sizes.</param>
        /// <returns>
        /// True if current schema was non empty; false otherwise.
        /// </returns>
        public bool WriteSchema(IBinaryStream stream, int schemaOffset, out int schemaId, 
            ref BinaryObjectHeader.Flag flags)
        {
            schemaId = Fnv1Hash.Basis;

            var count = _idx - schemaOffset;

            if (count == 0) 
                return false;

            flags |= BinaryObjectSchemaSerializer.WriteSchema(_fields, stream, schemaOffset, count, 
                (flags & BinaryObjectHeader.Flag.CompactFooter) == BinaryObjectHeader.Flag.CompactFooter);

            for (var i = schemaOffset; i < _idx; i++)
                schemaId = Fnv1Hash.Update(schemaId, _fields[i].Id);

            return true;
        }

        /// <summary>
        /// Gets the schema.
        /// </summary>
        /// <param name="schemaOffset">The schema offset.</param>
        /// <returns>Current schema as an array of field ids.</returns>
        public int[] GetSchema(int schemaOffset)
        {
            int[] result = new int[_idx - schemaOffset];

            for (int i = 0; i < result.Length; i++)
                result[i] = _fields[i + schemaOffset].Id;

            return result;
        }

        /// <summary>
        /// Gets the schema.
        /// </summary>
        /// <param name="schemaOffset">The schema offset.</param>
        /// <returns>Current schema as a dictionary.</returns>
        public Dictionary<int, int> GetFullSchema(int schemaOffset)
        {
            var size = _idx - schemaOffset;

            var result = new Dictionary<int, int>(size);

            for (int i = schemaOffset; i < _idx; i++)
            {
                var fld = _fields[i];

                result[fld.Id] = fld.Offset;
            }

            return result;
        }
    }
}

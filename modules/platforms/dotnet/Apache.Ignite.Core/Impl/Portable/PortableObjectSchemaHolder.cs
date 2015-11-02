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
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Shared schema holder.
    /// </summary>
    internal class PortableObjectSchemaHolder
    {
        /** Fields. */
        private PortableObjectSchemaField[] _fields = new PortableObjectSchemaField[16];

        /** Offsets for different schemas. */
        private readonly Stack<int> _offsets = new Stack<int>();

        /** Current field index. */
        private int _idx;

        /// <summary>
        /// Adds a field to the holder.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <param name="offset">The offset.</param>
        public void Push(int id, int offset)
        {
            if (_idx == _fields.Length)
                Array.Resize(ref _fields, _fields.Length * 2);

            _fields[_idx] = new PortableObjectSchemaField(id, offset);

            _idx++;
        }

        /// <summary>
        /// Marks the start of a new schema.
        /// </summary>
        public void PushSchema()
        {
            _offsets.Push(_idx);
        }

        /// <summary>
        /// Writes collected schema to the stream and pops it.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="schemaId">The schema identifier.</param>
        /// <returns>True if current schema was non empty; false otherwise.</returns>
        public bool WriteAndPop(IPortableStream stream, out int schemaId)
        {
            var count = _offsets.Pop();

            if (count > 0)
            {
                var startIdx = _idx - count;

                PortableObjectSchemaField.WriteArray(_fields, stream, startIdx, count);

                schemaId = PortableUtils.GetSchemaId(_fields, startIdx, count);

                _idx -= count;

                return true;
            }

            schemaId = PortableUtils.GetSchemaId(null, 0, 0);

            return false;
        }
    }
}

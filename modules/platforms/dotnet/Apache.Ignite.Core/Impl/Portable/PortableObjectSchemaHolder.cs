﻿/*
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
    using System.Threading;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Shared schema holder.
    /// </summary>
    internal class PortableObjectSchemaHolder
    {
        /** Current schema. */
        private static readonly ThreadLocal<PortableObjectSchemaHolder> CurrentHolder =
            new ThreadLocal<PortableObjectSchemaHolder>(() => new PortableObjectSchemaHolder());

        /** Fields. */
        private PortableObjectSchemaField[] _fields = new PortableObjectSchemaField[32];

        /** Current field index. */
        private int _idx;

        /// <summary>
        /// Gets the schema holder for the current thread.
        /// </summary>
        public static PortableObjectSchemaHolder Current
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

            _fields[_idx] = new PortableObjectSchemaField(id, offset);

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
        /// <param name="flags">Flags according to offset sizes: <see cref="PortableObjectHeader.FlagByteOffsets" />,
        /// <see cref="PortableObjectHeader.FlagShortOffsets" />, or 0.</param>
        /// <returns>
        /// True if current schema was non empty; false otherwise.
        /// </returns>
        public bool WriteSchema(IPortableStream stream, int schemaOffset, out int schemaId, out short flags)
        {
            schemaId = Fnv1Hash.Basis;
            flags = 0;

            var count = _idx - schemaOffset;

            if (count == 0) 
                return false;

            flags = PortableObjectHeader.WriteSchema(_fields, stream, schemaOffset, count);

            for (var i = schemaOffset; i < _idx; i++)
                schemaId = Fnv1Hash.Update(schemaId, _fields[i].Id);

            return true;
        }
    }
}

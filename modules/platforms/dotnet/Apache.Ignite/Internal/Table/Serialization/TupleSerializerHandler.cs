/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Table.Serialization
{
    using Buffers;
    using Ignite.Table;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Serializer handler for <see cref="IIgniteTuple"/>.
    /// </summary>
    internal class TupleSerializerHandler : IRecordSerializerHandler<IIgniteTuple>
    {
        /// <summary>
        /// Singleton instance.
        /// </summary>
        public static readonly TupleSerializerHandler Instance = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="TupleSerializerHandler"/> class.
        /// </summary>
        private TupleSerializerHandler()
        {
            // No-op.
        }

        /// <inheritdoc/>
        public IIgniteTuple Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
        {
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;
            var tuple = new IgniteTuple(count);

            for (var index = 0; index < count; index++)
            {
                if (reader.TryReadNoValue())
                {
                    continue;
                }

                var column = columns[index];
                tuple[column.Name] = reader.ReadObject(column.Type);
            }

            return tuple;
        }

        /// <inheritdoc/>
        public IIgniteTuple ReadValuePart(PooledBuffer buf, Schema schema, IIgniteTuple key)
        {
            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var columns = schema.Columns;
            var tuple = new IgniteTuple(columns.Count);

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (i < schema.KeyColumnCount)
                {
                    tuple[column.Name] = key[column.Name];
                }
                else
                {
                    if (r.TryReadNoValue())
                    {
                        continue;
                    }

                    tuple[column.Name] = r.ReadObject(column.Type);
                }
            }

            return tuple;
        }

        /// <inheritdoc/>
        public void Write(ref MessagePackWriter writer, Schema schema, IIgniteTuple record, bool keyOnly = false)
        {
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var index = 0; index < count; index++)
            {
                var col = columns[index];
                var colIdx = record.GetOrdinal(col.Name);

                if (colIdx < 0)
                {
                    writer.WriteNoValue();
                }
                else
                {
                    writer.WriteObject(record[colIdx]);
                }
            }
        }
    }
}

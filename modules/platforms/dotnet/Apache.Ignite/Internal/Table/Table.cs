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

namespace Apache.Ignite.Internal.Table
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Table;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Table API.
    /// </summary>
    internal class Table : ITable
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** Schemas. */
        private readonly ConcurrentDictionary<int, Schema> _schemas = new();

        /** */
        private readonly object _latestSchemaLock = new();

        /** */
        private volatile int _latestSchemaVersion = -1;

        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class.
        /// </summary>
        /// <param name="name">Table name.</param>
        /// <param name="id">Table id.</param>
        /// <param name="socket">Socket.</param>
        public Table(string name, Guid id, ClientFailoverSocket socket)
        {
            _socket = socket;
            Name = name;
            Id = id;
        }

        /// <inheritdoc/>
        public string Name { get; }

        /// <summary>
        /// Gets the id.
        /// </summary>
        public Guid Id { get; }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAsync(IIgniteTuple keyRec)
        {
            IgniteArgumentCheck.NotNull(keyRec, nameof(keyRec));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGet, writer).ConfigureAwait(false);
            return Read(resBuf.GetReader());

            void Write(MessagePackWriter w)
            {
                w.Write(Id);
                w.Write(schema.Version);

                for (var i = 0; i < schema.KeyColumnCount; i++)
                {
                    var col = schema.Columns[i];

                    w.WriteObject(keyRec[col.Name]);
                }

                w.Flush();
            }

            IIgniteTuple? Read(MessagePackReader r)
            {
                if (r.NextMessagePackType == MessagePackType.Nil)
                {
                    return null;
                }

                var schemaVersion = r.ReadInt32();

                if (schemaVersion != schema.Version)
                {
                    // TODO: Load schema (IGNITE-15430).
                    throw new NotSupportedException();
                }

                var columns = schema.Columns;

                var tuple = new IgniteTuple(columns.Count);

                foreach (var column in columns)
                {
                    tuple[column.Name] = r.ReadObject(column.Type);
                }

                return tuple;
            }
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(IIgniteTuple rec)
        {
            IgniteArgumentCheck.NotNull(rec, nameof(rec));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleUpsert, writer).ConfigureAwait(false);

            void Write(MessagePackWriter w)
            {
                w.Write(Id);
                w.Write(schema.Version);

                foreach (var col in schema.Columns)
                {
                    var colIdx = rec.GetOrdinal(col.Name);

                    if (colIdx < 0)
                    {
                        w.WriteNil();
                    }
                    else
                    {
                        w.WriteObject(rec[colIdx]);
                    }
                }

                w.Flush();
            }
        }

        private async Task<Schema> GetLatestSchemaAsync()
        {
            var latestSchemaVersion = _latestSchemaVersion;

            if (latestSchemaVersion >= 0)
            {
                return _schemas[latestSchemaVersion];
            }

            return await LoadSchemaAsync(null).ConfigureAwait(false);
        }

        private async Task<Schema> LoadSchemaAsync(int? version)
        {
            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.SchemasGet, writer).ConfigureAwait(false);
            return Read(resBuf.GetReader());

            void Write(MessagePackWriter w)
            {
                w.Write(Id);

                if (version == null)
                {
                    w.WriteNil();
                }
                else
                {
                    w.WriteArrayHeader(1);
                    w.Write(version.Value);
                }

                w.Flush();
            }

            Schema Read(MessagePackReader r)
            {
                var schemaCount = r.ReadMapHeader();

                if (schemaCount == 0)
                {
                    throw new IgniteClientException("Schema not found: " + version);
                }

                Schema last = null!;

                for (var i = 0; i < schemaCount; i++)
                {
                    last = ReadSchema(ref r);
                }

                // Store all schemas in the map, and return last.
                return last;
            }
        }

        private Schema ReadSchema(ref MessagePackReader r)
        {
            var schemaVersion = r.ReadInt32();
            var columnCount = r.ReadArrayHeader();
            var keyColumnCount = 0;

            var columns = new Column[columnCount];
            var columnsMap = new Dictionary<string, Column>(columnCount);

            for (var i = 0; i < columnCount; i++)
            {
                var propertyCount = r.ReadArrayHeader();

                Debug.Assert(propertyCount >= 4, "propertyCount >= 4");

                var name = r.ReadString();
                var type = r.ReadInt32();
                var isKey = r.ReadBoolean();
                var isNullable = r.ReadBoolean();

                r.Skip(propertyCount - 4);

                var column = new Column(name, (ClientDataType)type, isNullable, isKey, i);

                columns[i] = column;
                columnsMap[column.Name] = column;

                if (isKey)
                {
                    keyColumnCount++;
                }
            }

            var schema = new Schema(schemaVersion, keyColumnCount, columns, columnsMap);

            _schemas[schemaVersion] = schema;

            lock (_latestSchemaLock)
            {
                if (schemaVersion > _latestSchemaVersion)
                {
                    _latestSchemaVersion = schemaVersion;
                }
            }

            return schema;
        }
    }
}

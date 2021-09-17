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
        public Table(string name, IgniteUuid id, ClientFailoverSocket socket)
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
        public IgniteUuid Id { get; }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAsync(IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, key, keyOnly: true);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGet, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> GetAllAsync(IEnumerable<IIgniteTuple> keys)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, schema, iterator, keyOnly: true);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGetAll, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadTuples(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, record);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleUpsert, writer).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task UpsertAllAsync(IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return;
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, schema, iterator);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleUpsertAll, writer).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndUpsertAsync(IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, record);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGetAndUpsert, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> InsertAsync(IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, record);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleInsert, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> InsertAllAsync(IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, schema, iterator);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleInsertAll, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadTuples(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, record);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleReplace, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(IIgniteTuple record, IIgniteTuple newRecord)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, schema, record, newRecord);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleReplaceExact, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndReplaceAsync(IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, record);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGetAndReplace, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteAsync(IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, key, keyOnly: true);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleDelete, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteExactAsync(IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, record);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleDeleteExact, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndDeleteAsync(IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, schema, key, keyOnly: true);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGetAndDelete, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> DeleteAllAsync(IEnumerable<IIgniteTuple> keys)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, schema, iterator, keyOnly: true);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleDeleteAll, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadTuples(resBuf, resSchema, keyOnly: true);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> DeleteAllExactAsync(IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, schema, iterator);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleDeleteAllExact, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf, schema).ConfigureAwait(false);

            return ReadTuples(resBuf, resSchema);
        }

        private static IIgniteTuple? ReadValueTuple(PooledBuffer buf, Schema? schema, IIgniteTuple key)
        {
            if (schema == null)
            {
                return null;
            }

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
                    tuple[column.Name] = r.ReadObject(column.Type);
                }
            }

            return tuple;
        }

        private static IIgniteTuple ReadTuple(ref MessagePackReader r, Schema schema, bool keyOnly = false)
        {
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;
            var tuple = new IgniteTuple(count);

            for (var index = 0; index < count; index++)
            {
                var column = columns[index];
                tuple[column.Name] = r.ReadObject(column.Type);
            }

            return tuple;
        }

        private static IList<IIgniteTuple> ReadTuples(PooledBuffer buf, Schema? schema, bool keyOnly = false)
        {
            if (schema == null)
            {
                return Array.Empty<IIgniteTuple>();
            }

            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var count = r.ReadInt32();
            var res = new List<IIgniteTuple>(count);

            for (var i = 0; i < count; i++)
            {
                res.Add(ReadTuple(ref r, schema, keyOnly));
            }

            return res;
        }

        private static int? ReadSchemaVersion(PooledBuffer buf)
        {
            var reader = buf.GetReader();

            return reader.ReadInt32Nullable();
        }

        private static void WriteTuple(
            ref MessagePackWriter w,
            Schema schema,
            IIgniteTuple tuple,
            bool keyOnly = false)
        {
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var index = 0; index < count; index++)
            {
                var col = columns[index];
                var colIdx = tuple.GetOrdinal(col.Name);

                if (colIdx < 0)
                {
                    w.WriteNil();
                }
                else
                {
                    w.WriteObject(tuple[colIdx]);
                }
            }
        }

        private async ValueTask<Schema?> ReadSchemaAsync(PooledBuffer buf, Schema currentSchema)
        {
            var ver = ReadSchemaVersion(buf);

            if (ver == null)
            {
                return null;
            }

            if (currentSchema.Version == ver.Value)
            {
                return currentSchema;
            }

            if (_schemas.TryGetValue(ver.Value, out var res))
            {
                return res;
            }

            return await LoadSchemaAsync(ver).ConfigureAwait(false);
        }

        private async ValueTask<Schema> GetLatestSchemaAsync()
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
            Write();

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.SchemasGet, writer).ConfigureAwait(false);
            return Read();

            void Write()
            {
                var w = writer.GetMessageWriter();
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

            Schema Read()
            {
                var r = resBuf.GetReader();
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

        private void WriteTuple(PooledArrayBufferWriter buf, Schema schema, IIgniteTuple tuple, bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            WriteTupleWithHeader(ref w, schema, tuple, keyOnly);

            w.Flush();
        }

        private void WriteTuples(
            PooledArrayBufferWriter buf,
            Schema schema,
            IIgniteTuple t,
            IIgniteTuple t2,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            WriteTupleWithHeader(ref w, schema, t, keyOnly);
            WriteTuple(ref w, schema, t2, keyOnly);

            w.Flush();
        }

        private void WriteTuples(
            PooledArrayBufferWriter buf,
            Schema schema,
            IEnumerator<IIgniteTuple> tuples,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            w.Write(Id);
            w.Write(schema.Version);
            w.Flush();

            var count = 0;
            var countPos = buf.ReserveInt32();

            do
            {
                var tuple = tuples.Current;

                if (tuple == null)
                {
                    throw new ArgumentException("Tuple collection can't contain null elements.");
                }

                WriteTuple(ref w, schema, tuple, keyOnly);
                count++;
            }
            while (tuples.MoveNext()); // First MoveNext is called outside to check for empty IEnumerable.

            buf.WriteInt32(countPos, count);

            w.Flush();
        }

        private void WriteTupleWithHeader(
            ref MessagePackWriter w,
            Schema schema,
            IIgniteTuple tuple,
            bool keyOnly = false)
        {
            w.Write(Id);
            w.Write(schema.Version);

            WriteTuple(ref w, schema, tuple, keyOnly);
        }
    }
}

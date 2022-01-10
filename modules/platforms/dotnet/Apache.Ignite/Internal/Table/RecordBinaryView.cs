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
    using System.Transactions;
    using Buffers;
    using Common;
    using Ignite.Table;
    using Ignite.Transactions;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Table API.
    /// </summary>
    internal class RecordBinaryView : IRecordView<IIgniteTuple>
    {
        /** Table. */
        private readonly Table _table;

        /** Schemas. */
        private readonly ConcurrentDictionary<int, Schema> _schemas = new();

        /** */
        private readonly object _latestSchemaLock = new();

        /** */
        private volatile int _latestSchemaVersion = -1;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordBinaryView"/> class.
        /// </summary>
        /// <param name="table">Table.</param>
        public RecordBinaryView(Table table)
        {
            _table = table;
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAsync(ITransaction? transaction, IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGet, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple?>> GetAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> keys)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, tx, schema, iterator, keyOnly: true);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleGetAll, tx, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return ReadTuplesNullable(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleUpsert, transaction, record).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task UpsertAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return;
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, tx, schema, iterator);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleUpsertAll, tx, writer).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndUpsertAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGetAndUpsert, transaction, record).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> InsertAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleInsert, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> InsertAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, tx, schema, iterator);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleInsertAll, tx, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return ReadTuples(resBuf, resSchema);
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleReplace, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, IIgniteTuple record, IIgniteTuple newRecord)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, tx, schema, record, newRecord);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleReplaceExact, tx, writer).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndReplaceAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGetAndReplace, transaction, record).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteAsync(ITransaction? transaction, IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteExactAsync(ITransaction? transaction, IIgniteTuple record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleDeleteExact, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAndDeleteAsync(ITransaction? transaction, IIgniteTuple key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoTupleOutOpAsync(ClientOp.TupleGetAndDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return ReadValueTuple(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> DeleteAllAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> keys)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, tx, schema, iterator, keyOnly: true);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleDeleteAll, tx, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return ReadTuples(resBuf, resSchema, keyOnly: true);
        }

        /// <inheritdoc/>
        public async Task<IList<IIgniteTuple>> DeleteAllExactAsync(ITransaction? transaction, IEnumerable<IIgniteTuple> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<IIgniteTuple>();
            }

            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuples(writer, tx, schema, iterator);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleDeleteAllExact, tx, writer).ConfigureAwait(false);
            var resSchema = await ReadSchemaAsync(resBuf).ConfigureAwait(false);

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
                    if (r.TryReadNoValue())
                    {
                        continue;
                    }

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
                if (r.TryReadNoValue())
                {
                    continue;
                }

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

        private static IList<IIgniteTuple?> ReadTuplesNullable(PooledBuffer buf, Schema? schema, bool keyOnly = false)
        {
            if (schema == null)
            {
                return Array.Empty<IIgniteTuple?>();
            }

            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var count = r.ReadInt32();
            var res = new List<IIgniteTuple?>(count);

            for (var i = 0; i < count; i++)
            {
                var hasValue = r.ReadBoolean();

                res.Add(hasValue ? ReadTuple(ref r, schema, keyOnly) : null);
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
                    w.WriteNoValue();
                }
                else
                {
                    w.WriteObject(tuple[colIdx]);
                }
            }
        }

        private static Transactions.Transaction? GetTx(ITransaction? tx)
        {
            if (tx == null)
            {
                return null;
            }

            if (tx is Transactions.Transaction t)
            {
                return t;
            }

            throw new TransactionException("Unsupported transaction implementation: " + tx.GetType());
        }

        private static void WriteTx(ref MessagePackWriter w, Transactions.Transaction? tx)
        {
            if (tx == null)
            {
                w.WriteNil();
            }
            else
            {
                w.WriteInt64(tx.Id);
            }
        }

        private async ValueTask<Schema?> ReadSchemaAsync(PooledBuffer buf)
        {
            var ver = ReadSchemaVersion(buf);

            if (ver == null)
            {
                return null;
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

            using var resBuf = await _table.Socket.DoOutInOpAsync(ClientOp.SchemasGet, writer).ConfigureAwait(false);
            return Read();

            void Write()
            {
                var w = writer.GetMessageWriter();
                w.Write(_table.Id);

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

        private void WriteTuple(
            PooledArrayBufferWriter buf,
            Transactions.Transaction? tx,
            Schema schema,
            IIgniteTuple tuple,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            WriteTupleWithHeader(ref w, tx, schema, tuple, keyOnly);

            w.Flush();
        }

        private void WriteTuples(
            PooledArrayBufferWriter buf,
            Transactions.Transaction? tx,
            Schema schema,
            IIgniteTuple t,
            IIgniteTuple t2,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            WriteTupleWithHeader(ref w, tx, schema, t, keyOnly);
            WriteTuple(ref w, schema, t2, keyOnly);

            w.Flush();
        }

        private void WriteTuples(
            PooledArrayBufferWriter buf,
            Transactions.Transaction? tx,
            Schema schema,
            IEnumerator<IIgniteTuple> tuples,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            w.Write(_table.Id);
            WriteTx(ref w, tx);
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
            Transactions.Transaction? tx,
            Schema schema,
            IIgniteTuple tuple,
            bool keyOnly = false)
        {
            w.Write(_table.Id);
            WriteTx(ref w, tx);
            w.Write(schema.Version);

            WriteTuple(ref w, schema, tuple, keyOnly);
        }

        private ValueTask<ClientSocket> GetSocket(Transactions.Transaction? tx)
        {
            if (tx == null)
            {
                return _table.Socket.GetSocketAsync();
            }

            if (tx.FailoverSocket != _table.Socket)
            {
                throw new IgniteClientException("Specified transaction belongs to a different IgniteClient instance.");
            }

            return new ValueTask<ClientSocket>(tx.Socket);
        }

        private async Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            Transactions.Transaction? tx,
            PooledArrayBufferWriter? request = null)
        {
            var socket = await GetSocket(tx).ConfigureAwait(false);

            return await socket.DoOutInOpAsync(clientOp, request).ConfigureAwait(false);
        }

        private async Task<PooledBuffer> DoTupleOutOpAsync(
            ClientOp op,
            ITransaction? transaction,
            IIgniteTuple tuple,
            bool keyOnly = false)
        {
            var schema = await GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = GetTx(transaction);

            using var writer = new PooledArrayBufferWriter();
            WriteTuple(writer, tx, schema, tuple, keyOnly);

            return await DoOutInOpAsync(op, tx, writer).ConfigureAwait(false);
        }
    }
}

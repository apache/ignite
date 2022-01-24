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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Buffers;
    using Ignite.Table;
    using MessagePack;
    using Proto;
    using Serialization;

    /// <summary>
    /// Table API.
    /// </summary>
    internal sealed class Table : ITable
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** Table id. */
        private readonly IgniteUuid _id;

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
            _id = id;

            RecordBinaryView = new RecordView<IIgniteTuple>(
                this,
                new RecordSerializer<IIgniteTuple>(this, TupleSerializerHandler.Instance));
        }

        /// <inheritdoc/>
        public string Name { get; }

        /// <inheritdoc/>
        public IRecordView<IIgniteTuple> RecordBinaryView { get; }

        /// <inheritdoc/>
        public IRecordView<T> GetRecordView<T>()
            where T : class
        {
            return new RecordView<T>(this, new RecordSerializer<T>(this, new ObjectSerializerHandler<T>()));
        }

        /// <summary>
        /// Writes the transaction id, if present.
        /// </summary>
        /// <param name="w">Writer.</param>
        /// <param name="tx">Transaction.</param>
        internal void WriteIdAndTx(ref MessagePackWriter w, Transactions.Transaction? tx)
        {
            w.Write(_id);

            if (tx == null)
            {
                w.WriteNil();
            }
            else
            {
                w.WriteInt64(tx.Id);
            }
        }

        /// <summary>
        /// Gets the socket.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Socket.</returns>
        internal ValueTask<ClientSocket> GetSocket(Transactions.Transaction? tx)
        {
            if (tx == null)
            {
                return _socket.GetSocketAsync();
            }

            if (tx.FailoverSocket != _socket)
            {
                throw new IgniteClientException("Specified transaction belongs to a different IgniteClient instance.");
            }

            return new ValueTask<ClientSocket>(tx.Socket);
        }

        /// <summary>
        /// Reads the schema.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <returns>Schema or null.</returns>
        internal async ValueTask<Schema?> ReadSchemaAsync(PooledBuffer buf)
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

            static int? ReadSchemaVersion(PooledBuffer buf)
            {
                var reader = buf.GetReader();

                return reader.ReadInt32Nullable();
            }
        }

        /// <summary>
        /// Gets the latest schema.
        /// </summary>
        /// <returns>Schema.</returns>
        internal async ValueTask<Schema> GetLatestSchemaAsync()
        {
            var latestSchemaVersion = _latestSchemaVersion;

            if (latestSchemaVersion >= 0)
            {
                return _schemas[latestSchemaVersion];
            }

            return await LoadSchemaAsync(null).ConfigureAwait(false);
        }

        /// <summary>
        /// Loads the schema.
        /// </summary>
        /// <param name="version">Version.</param>
        /// <returns>Schema.</returns>
        private async Task<Schema> LoadSchemaAsync(int? version)
        {
            using var writer = new PooledArrayBufferWriter();
            Write();

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.SchemasGet, writer).ConfigureAwait(false);
            return Read();

            void Write()
            {
                var w = writer.GetMessageWriter();
                w.Write(_id);

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

        /// <summary>
        /// Reads the schema.
        /// </summary>
        /// <param name="r">Reader.</param>
        /// <returns>Schema.</returns>
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

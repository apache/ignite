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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Client.Cache.Query;
    using Apache.Ignite.Core.Impl.Common;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Client cache implementation.
    /// </summary>
    internal sealed class CacheClient<TK, TV> : ICacheClient<TK, TV>, ICacheInternal
    {
        /** Scan query filter platform code: .NET filter. */
        private const byte FilterPlatformDotnet = 2;

        /** Cache name. */
        private readonly string _name;

        /** Cache id. */
        private readonly int _id;

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClient{TK, TV}" /> class.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="name">Cache name.</param>
        /// <param name="keepBinary">Binary mode flag.</param>
        public CacheClient(IgniteClient ignite, string name, bool keepBinary = false)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(name != null);

            _name = name;
            _ignite = ignite;
            _marsh = _ignite.Marshaller;
            _id = BinaryUtils.GetCacheId(name);
            _keepBinary = keepBinary;
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public TV this[TK key]
        {
            get { return Get(key); }
            set { Put(key, value); }
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp(ClientOp.CacheGet, w => w.WriteObject(key), UnmarshalNotNull<TV>);
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAsync(ClientOp.CacheGet, w => w.WriteObject(key), UnmarshalNotNull<TV>);
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            var res = DoOutInOp(ClientOp.CacheGet, w => w.WriteObject(key), UnmarshalCacheResult<TV>);

            value = res.Value;

            return res.Success;
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAsync(ClientOp.CacheGet, w => w.WriteObject(key), UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp(ClientOp.CacheGetAll, w => w.WriteEnumerable(keys), s => ReadCacheEntries(s));
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpAsync(ClientOp.CacheGetAll, w => w.WriteEnumerable(keys), s => ReadCacheEntries(s));
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            DoOutOp(ClientOp.CachePut, w => WriteKeyVal(w, key, val));
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOpAsync(ClientOp.CachePut, w => WriteKeyVal(w, key, val));
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp(ClientOp.CacheContainsKey, w => w.WriteObjectDetached(key), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAsync(ClientOp.CacheContainsKey, w => w.WriteObjectDetached(key), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp(ClientOp.CacheContainsKeys, w => w.WriteEnumerable(keys), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpAsync(ClientOp.CacheContainsKeys, w => w.WriteEnumerable(keys), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(ScanQuery<TK, TV> scanQuery)
        {
            IgniteArgumentCheck.NotNull(scanQuery, "scanQuery");

            // Filter is a binary object for all platforms.
            // For .NET it is a CacheEntryFilterHolder with a predefined id (BinaryTypeId.CacheEntryPredicateHolder).
            return DoOutInOp(ClientOp.QueryScan, w => WriteScanQuery(w, scanQuery),
                s => new ClientQueryCursor<TK, TV>(
                    _ignite, s.ReadLong(), _keepBinary, s, ClientOp.QueryScanCursorGetPage));
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(SqlQuery sqlQuery)
        {
            IgniteArgumentCheck.NotNull(sqlQuery, "sqlQuery");
            IgniteArgumentCheck.NotNull(sqlQuery.Sql, "sqlQuery.Sql");
            IgniteArgumentCheck.NotNull(sqlQuery.QueryType, "sqlQuery.QueryType");

            return DoOutInOp(ClientOp.QuerySql, w => WriteSqlQuery(w, sqlQuery),
                s => new ClientQueryCursor<TK, TV>(
                    _ignite, s.ReadLong(), _keepBinary, s, ClientOp.QuerySqlCursorGetPage));
        }

        /** <inheritDoc /> */
        public IFieldsQueryCursor Query(SqlFieldsQuery sqlFieldsQuery)
        {
            IgniteArgumentCheck.NotNull(sqlFieldsQuery, "sqlFieldsQuery");
            IgniteArgumentCheck.NotNull(sqlFieldsQuery.Sql, "sqlFieldsQuery.Sql");

            return DoOutInOp(ClientOp.QuerySqlFields,
                w => WriteSqlFieldsQuery(w, sqlFieldsQuery),
                s => GetFieldsCursor(s));
        }

        /** <inheritDoc /> */
        public IQueryCursor<T> Query<T>(SqlFieldsQuery sqlFieldsQuery, Func<IBinaryRawReader, int, T> readerFunc)
        {
            return DoOutInOp(ClientOp.QuerySqlFields,
                w => WriteSqlFieldsQuery(w, sqlFieldsQuery, false),
                s => GetFieldsCursorNoColumnNames(s, readerFunc));
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOp(ClientOp.CacheGetAndPut, w => WriteKeyVal(w, key, val), UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAsync(ClientOp.CacheGetAndPut, w => WriteKeyVal(w, key, val), UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOp(ClientOp.CacheGetAndReplace, w => WriteKeyVal(w, key, val), UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAsync(ClientOp.CacheGetAndReplace, w => WriteKeyVal(w, key, val), UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp(ClientOp.CacheGetAndRemove, w => w.WriteObjectDetached(key),
                UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAsync(ClientOp.CacheGetAndRemove, w => w.WriteObjectDetached(key),
                UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOp(ClientOp.CachePutIfAbsent, w => WriteKeyVal(w, key, val), s => s.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAsync(ClientOp.CachePutIfAbsent, w => WriteKeyVal(w, key, val), s => s.ReadBool());
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOp(ClientOp.CacheGetAndPutIfAbsent, w => WriteKeyVal(w, key, val),
                UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAsync(ClientOp.CacheGetAndPutIfAbsent, w => WriteKeyVal(w, key, val),
                UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOp(ClientOp.CacheReplace, w => WriteKeyVal(w, key, val), s => s.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAsync(ClientOp.CacheReplace, w => WriteKeyVal(w, key, val), s => s.ReadBool());
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            return DoOutInOp(ClientOp.CacheReplaceIfEquals, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(oldVal);
                w.WriteObjectDetached(newVal);
            }, s => s.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            return DoOutInOpAsync(ClientOp.CacheReplaceIfEquals, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(oldVal);
                w.WriteObjectDetached(newVal);
            }, s => s.ReadBool());
        }

        /** <inheritDoc /> */
        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            DoOutOp(ClientOp.CachePutAll, w => w.WriteDictionary(vals));
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            return DoOutOpAsync(ClientOp.CachePutAll, w => w.WriteDictionary(vals));
        }

        /** <inheritDoc /> */
        public void Clear()
        {
            DoOutOp(ClientOp.CacheClear);
        }

        /** <inheritDoc /> */
        public Task ClearAsync()
        {
            return DoOutOpAsync(ClientOp.CacheClear);
        }

        /** <inheritDoc /> */
        public void Clear(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            DoOutOp(ClientOp.CacheClearKey, w => w.WriteObjectDetached(key));
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOpAsync(ClientOp.CacheClearKey, w => w.WriteObjectDetached(key));
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(ClientOp.CacheClearKeys, w => w.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(ClientOp.CacheClearKeys, w => w.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public bool Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp(ClientOp.CacheRemoveKey, w => w.WriteObjectDetached(key), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAsync(ClientOp.CacheRemoveKey, w => w.WriteObjectDetached(key), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOp(ClientOp.CacheRemoveIfEquals, w => WriteKeyVal(w, key, val), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAsync(ClientOp.CacheRemoveIfEquals, w => WriteKeyVal(w, key, val), r => r.ReadBool());
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(ClientOp.CacheRemoveKeys, w => w.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(ClientOp.CacheRemoveKeys, w => w.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            DoOutOp(ClientOp.CacheRemoveAll);
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            return DoOutOpAsync(ClientOp.CacheRemoveAll);
        }

        /** <inheritDoc /> */
        public long GetSize(params CachePeekMode[] modes)
        {
            return DoOutInOp(ClientOp.CacheGetSize, w => WritePeekModes(modes, w), s => s.ReadLong());
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeAsync(params CachePeekMode[] modes)
        {
            return DoOutInOpAsync(ClientOp.CacheGetSize, w => WritePeekModes(modes, w), s => s.ReadLong());
        }

        /** <inheritDoc /> */
        public CacheClientConfiguration GetConfiguration()
        {
            return DoOutInOp(ClientOp.CacheGetConfiguration, null,
                s => new CacheClientConfiguration(s, _ignite.ServerVersion));
        }

        /** <inheritDoc /> */
        CacheConfiguration ICacheInternal.GetConfiguration()
        {
            return GetConfiguration().ToCacheConfiguration();
        }

        /** <inheritDoc /> */
        public ICacheClient<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            if (_keepBinary)
            {
                var result = this as ICacheClient<TK1, TV1>;

                if (result == null)
                {
                    throw new InvalidOperationException(
                        "Can't change type of binary cache. WithKeepBinary has been called on an instance of " +
                        "binary cache with incompatible generic arguments.");
                }

                return result;
            }

            return new CacheClient<TK1, TV1>(_ignite, _name, true);
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public T DoOutInOpExtension<T>(int extensionId, int opCode, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, T> readFunc)
        {
            // Should not be called, there are no usages for thin client.
            throw IgniteClient.GetClientNotSupportedException();
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private void DoOutOp(ClientOp opId, Action<BinaryWriter> writeAction = null)
        {
            DoOutInOp<object>(opId, writeAction, null);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private Task DoOutOpAsync(ClientOp opId, Action<BinaryWriter> writeAction = null)
        {
            return DoOutInOpAsync<object>(opId, writeAction, null);
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction,
            Func<IBinaryStream, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream => WriteRequest(writeAction, stream),
                readFunc, HandleError<T>);
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<BinaryWriter> writeAction,
            Func<IBinaryStream, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAsync(opId, stream => WriteRequest(writeAction, stream),
                readFunc, HandleError<T>);
        }

        /// <summary>
        /// Writes the request.
        /// </summary>
        private void WriteRequest(Action<BinaryWriter> writeAction, IBinaryStream stream)
        {
            stream.WriteInt(_id);
            stream.WriteByte(0); // Flags (skipStore, etc).

            if (writeAction != null)
            {
                var writer = _marsh.StartMarshal(stream);

                writeAction(writer);

                _marsh.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Unmarshals the value, throwing an exception for nulls.
        /// </summary>
        private T UnmarshalNotNull<T>(IBinaryStream stream)
        {
            var hdr = stream.ReadByte();

            if (hdr == BinaryUtils.HdrNull)
            {
                throw GetKeyNotFoundException();
            }

            stream.Seek(-1, SeekOrigin.Current);

            return _marsh.Unmarshal<T>(stream, _keepBinary);
        }

        /// <summary>
        /// Unmarshals the value, wrapping in a cache result.
        /// </summary>
        private CacheResult<T> UnmarshalCacheResult<T>(IBinaryStream stream)
        {
            var hdr = stream.ReadByte();

            if (hdr == BinaryUtils.HdrNull)
            {
                return new CacheResult<T>();
            }

            stream.Seek(-1, SeekOrigin.Current);

            return new CacheResult<T>(_marsh.Unmarshal<T>(stream, _keepBinary));
        }

        /// <summary>
        /// Writes the scan query.
        /// </summary>
        private void WriteScanQuery(BinaryWriter writer, ScanQuery<TK, TV> qry)
        {
            Debug.Assert(qry != null);

            if (qry.Filter == null)
            {
                writer.WriteByte(BinaryUtils.HdrNull);
            }
            else
            {
                var holder = new CacheEntryFilterHolder(qry.Filter, (key, val) => qry.Filter.Invoke(
                    new CacheEntry<TK, TV>((TK)key, (TV)val)), writer.Marshaller, _keepBinary);

                writer.WriteObject(holder);

                writer.WriteByte(FilterPlatformDotnet);
            }

            writer.WriteInt(qry.PageSize);

            writer.WriteInt(qry.Partition ?? -1);

            writer.WriteBoolean(qry.Local);
        }

        /// <summary>
        /// Writes the SQL query.
        /// </summary>
        private static void WriteSqlQuery(IBinaryRawWriter writer, SqlQuery qry)
        {
            Debug.Assert(qry != null);

            writer.WriteString(qry.QueryType);
            writer.WriteString(qry.Sql);
            QueryBase.WriteQueryArgs(writer, qry.Arguments);
            writer.WriteBoolean(qry.EnableDistributedJoins);
            writer.WriteBoolean(qry.Local);
            writer.WriteBoolean(qry.ReplicatedOnly);
            writer.WriteInt(qry.PageSize);
            writer.WriteTimeSpanAsLong(qry.Timeout);
        }

        /// <summary>
        /// Writes the SQL fields query.
        /// </summary>
        private static void WriteSqlFieldsQuery(IBinaryRawWriter writer, SqlFieldsQuery qry,
            bool includeColumns = true)
        {
            Debug.Assert(qry != null);

            writer.WriteString(qry.Schema);
            writer.WriteInt(qry.PageSize);
            writer.WriteInt(-1);  // maxRows: unlimited
            writer.WriteString(qry.Sql);
            QueryBase.WriteQueryArgs(writer, qry.Arguments);

            // .NET client does not discern between different statements for now.
            // We cound have ExecuteNonQuery method, which uses StatementType.Update, for example.
            writer.WriteByte((byte)StatementType.Any);

            writer.WriteBoolean(qry.EnableDistributedJoins);
            writer.WriteBoolean(qry.Local);
            writer.WriteBoolean(qry.ReplicatedOnly);
            writer.WriteBoolean(qry.EnforceJoinOrder);
            writer.WriteBoolean(qry.Colocated);
            writer.WriteBoolean(qry.Lazy);
            writer.WriteTimeSpanAsLong(qry.Timeout);
            writer.WriteBoolean(includeColumns);

        }

        /// <summary>
        /// Gets the fields cursor.
        /// </summary>
        private ClientFieldsQueryCursor GetFieldsCursor(IBinaryStream s)
        {
            var cursorId = s.ReadLong();
            var columnNames = ClientFieldsQueryCursor.ReadColumns(_marsh.StartUnmarshal(s));

            return new ClientFieldsQueryCursor(_ignite, cursorId, _keepBinary, s,
                ClientOp.QuerySqlFieldsCursorGetPage, columnNames);
        }

        /// <summary>
        /// Gets the fields cursor.
        /// </summary>
        private ClientQueryCursorBase<T> GetFieldsCursorNoColumnNames<T>(IBinaryStream s,
            Func<IBinaryRawReader, int, T> readerFunc)
        {
            var cursorId = s.ReadLong();
            var columnCount = s.ReadInt();

            return new ClientQueryCursorBase<T>(_ignite, cursorId, _keepBinary, s,
                ClientOp.QuerySqlFieldsCursorGetPage, r => readerFunc(r, columnCount));
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private T HandleError<T>(ClientStatusCode status, string msg)
        {
            switch (status)
            {
                case ClientStatusCode.CacheDoesNotExist:
                    throw new IgniteClientException("Cache doesn't exist: " + Name, null, status);

                default:
                    throw new IgniteClientException(msg, null, status);
            }
        }

        /// <summary>
        /// Gets the key not found exception.
        /// </summary>
        private static KeyNotFoundException GetKeyNotFoundException()
        {
            return new KeyNotFoundException("The given key was not present in the cache.");
        }

        /// <summary>
        /// Writes the peek modes.
        /// </summary>
        private static void WritePeekModes(ICollection<CachePeekMode> modes, IBinaryRawWriter w)
        {
            if (modes == null)
            {
                w.WriteInt(0);
            }
            else
            {
                w.WriteInt(modes.Count);

                foreach (var m in modes)
                {
                    // Convert bit flag to ordinal.
                    byte val = 0;
                    var flagVal = (int)m;

                    while ((flagVal = flagVal >> 1) > 0)
                    {
                        val++;
                    }

                    w.WriteByte(val);
                }
            }
        }

        /// <summary>
        /// Reads the cache entries.
        /// </summary>
        private ICollection<ICacheEntry<TK, TV>> ReadCacheEntries(IBinaryStream stream)
        {
            var reader = _marsh.StartUnmarshal(stream, _keepBinary);

            var cnt = reader.ReadInt();
            var res = new List<ICacheEntry<TK, TV>>(cnt);

            for (var i = 0; i < cnt; i++)
            {
                res.Add(new CacheEntry<TK, TV>(reader.ReadObject<TK>(), reader.ReadObject<TV>()));
            }

            return res;
        }

        /// <summary>
        /// Writes key and value.
        /// </summary>
        private static void WriteKeyVal(BinaryWriter w, TK key, TV val)
        {
            w.WriteObjectDetached(key);
            w.WriteObjectDetached(val);
        }
    }
}

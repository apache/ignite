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
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Client.Cache.Query;
    using Apache.Ignite.Core.Impl.Common;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Client cache implementation.
    /// </summary>
    internal sealed class CacheClient<TK, TV> : ICacheClient<TK, TV>, ICacheInternal
    {
        /// <summary>
        /// Additional flag values for cache operations.
        /// </summary>
        private enum ClientCacheRequestFlag : byte
        {
            /// <summary>
            /// No flags
            /// </summary>
            None = 0,

            /// <summary>
            /// With keep binary flag.
            /// Reserved for other thin clients.
            /// </summary>
            // ReSharper disable once ShiftExpressionRealShiftCountIsZero
            // ReSharper disable once UnusedMember.Local
            WithKeepBinary = 1 << 0,

            /// <summary>
            /// With transactional binary flag.
            /// Reserved for IEP-34 Thin client: transactions support.
            /// </summary>
            // ReSharper disable once UnusedMember.Local
            WithTransactional = 1 << 1,

            /// <summary>
            /// With expiration policy.
            /// </summary>
            WithExpiryPolicy = 1 << 2
        }

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

        /** Expiry policy. */
        private readonly IExpiryPolicy _expiryPolicy;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClient{TK, TV}" /> class.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="name">Cache name.</param>
        /// <param name="keepBinary">Binary mode flag.</param>
        /// /// <param name="expiryPolicy">Expire policy.</param>
        public CacheClient(IgniteClient ignite, string name, bool keepBinary = false, IExpiryPolicy expiryPolicy = null)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(name != null);

            _name = name;
            _ignite = ignite;
            _marsh = _ignite.Marshaller;
            _id = BinaryUtils.GetCacheId(name);
            _keepBinary = keepBinary;
            _expiryPolicy = expiryPolicy;
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

            return DoOutInOpAffinity(ClientOp.CacheGet, key, ctx => UnmarshalNotNull<TV>(ctx));
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinityAsync(ClientOp.CacheGet, key, ctx => ctx.Writer.WriteObjectDetached(key),
                ctx => UnmarshalNotNull<TV>(ctx));
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            var res = DoOutInOpAffinity(ClientOp.CacheGet, key, UnmarshalCacheResult<TV>);

            value = res.Value;

            return res.Success;
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinityAsync(ClientOp.CacheGet, key, ctx => ctx.Writer.WriteObjectDetached(key),
                UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp(ClientOp.CacheGetAll, ctx => ctx.Writer.WriteEnumerable(keys),
                s => ReadCacheEntries(s.Stream));
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpAsync(ClientOp.CacheGetAll, ctx => ctx.Writer.WriteEnumerable(keys),
                s => ReadCacheEntries(s.Stream));
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            DoOutInOpAffinity<object>(ClientOp.CachePut, key, val, null);
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOpAffinityAsync(ClientOp.CachePut, key, ctx => {
                ctx.Writer.WriteObjectDetached(key);
                ctx.Writer.WriteObjectDetached(val);
            });
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinity(ClientOp.CacheContainsKey, key, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinityAsync(ClientOp.CacheContainsKey, key, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp(ClientOp.CacheContainsKeys, ctx => ctx.Writer.WriteEnumerable(keys),
                ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpAsync(ClientOp.CacheContainsKeys, ctx => ctx.Writer.WriteEnumerable(keys),
                ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(ScanQuery<TK, TV> scanQuery)
        {
            IgniteArgumentCheck.NotNull(scanQuery, "scanQuery");

            // Filter is a binary object for all platforms.
            // For .NET it is a CacheEntryFilterHolder with a predefined id (BinaryTypeId.CacheEntryPredicateHolder).
            return DoOutInOp(ClientOp.QueryScan, w => WriteScanQuery(w.Writer, scanQuery),
                ctx => new ClientQueryCursor<TK, TV>(
                    ctx.Socket, ctx.Stream.ReadLong(), _keepBinary, ctx.Stream, ClientOp.QueryScanCursorGetPage));
        }

        /** <inheritDoc /> */
        [Obsolete]
        public IQueryCursor<ICacheEntry<TK, TV>> Query(SqlQuery sqlQuery)
        {
            IgniteArgumentCheck.NotNull(sqlQuery, "sqlQuery");
            IgniteArgumentCheck.NotNull(sqlQuery.Sql, "sqlQuery.Sql");
            IgniteArgumentCheck.NotNull(sqlQuery.QueryType, "sqlQuery.QueryType");

            return DoOutInOp(ClientOp.QuerySql, w => WriteSqlQuery(w.Writer, sqlQuery),
                ctx => new ClientQueryCursor<TK, TV>(
                    ctx.Socket, ctx.Stream.ReadLong(), _keepBinary, ctx.Stream, ClientOp.QuerySqlCursorGetPage));
        }

        /** <inheritDoc /> */
        public IFieldsQueryCursor Query(SqlFieldsQuery sqlFieldsQuery)
        {
            IgniteArgumentCheck.NotNull(sqlFieldsQuery, "sqlFieldsQuery");
            IgniteArgumentCheck.NotNull(sqlFieldsQuery.Sql, "sqlFieldsQuery.Sql");

            return DoOutInOp(ClientOp.QuerySqlFields,
                ctx => WriteSqlFieldsQuery(ctx.Writer, sqlFieldsQuery),
                ctx => GetFieldsCursor(ctx));
        }

        /** <inheritDoc /> */
        public IQueryCursor<T> Query<T>(SqlFieldsQuery sqlFieldsQuery, Func<IBinaryRawReader, int, T> readerFunc)
        {
            return DoOutInOp(ClientOp.QuerySqlFields,
                ctx => WriteSqlFieldsQuery(ctx.Writer, sqlFieldsQuery, false),
                ctx => GetFieldsCursorNoColumnNames(ctx, readerFunc));
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinity(ClientOp.CacheGetAndPut, key, val, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinityAsync(ClientOp.CacheGetAndPut, key, val, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinity(ClientOp.CacheGetAndReplace, key, val, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinityAsync(ClientOp.CacheGetAndReplace, key, val, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinity(ClientOp.CacheGetAndRemove, key, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinityAsync(ClientOp.CacheGetAndRemove, key, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinity(ClientOp.CachePutIfAbsent, key, val, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinityAsync(ClientOp.CachePutIfAbsent, key, val, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinity(ClientOp.CacheGetAndPutIfAbsent, key, val, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinityAsync(ClientOp.CacheGetAndPutIfAbsent, key, val, UnmarshalCacheResult<TV>);
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinity(ClientOp.CacheReplace, key, val, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinityAsync(ClientOp.CacheReplace, key, val, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            return DoOutInOpAffinity(ClientOp.CacheReplaceIfEquals, key, ctx =>
            {
                ctx.Writer.WriteObjectDetached(key);
                ctx.Writer.WriteObjectDetached(oldVal);
                ctx.Writer.WriteObjectDetached(newVal);
            }, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            return DoOutInOpAffinityAsync(ClientOp.CacheReplaceIfEquals, key, ctx =>
            {
                ctx.Writer.WriteObjectDetached(key);
                ctx.Writer.WriteObjectDetached(oldVal);
                ctx.Writer.WriteObjectDetached(newVal);
            }, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            DoOutOp(ClientOp.CachePutAll, ctx => ctx.Writer.WriteDictionary(vals));
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            return DoOutOpAsync(ClientOp.CachePutAll, ctx => ctx.Writer.WriteDictionary(vals));
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

            DoOutOpAffinity(ClientOp.CacheClearKey, key);
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOpAffinityAsync(ClientOp.CacheClearKey, key, ctx => ctx.Writer.WriteObjectDetached(key));
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(ClientOp.CacheClearKeys, ctx => ctx.Writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(ClientOp.CacheClearKeys, ctx => ctx.Writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public bool Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinity(ClientOp.CacheRemoveKey, key, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpAffinityAsync(ClientOp.CacheRemoveKey, key, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinity(ClientOp.CacheRemoveIfEquals, key, val, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpAffinityAsync(ClientOp.CacheRemoveIfEquals, key, val, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(ClientOp.CacheRemoveKeys, ctx => ctx.Writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(ClientOp.CacheRemoveKeys, ctx => ctx.Writer.WriteEnumerable(keys));
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
            return DoOutInOp(ClientOp.CacheGetSize, w => WritePeekModes(modes, w.Stream),
                ctx => ctx.Stream.ReadLong());
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeAsync(params CachePeekMode[] modes)
        {
            return DoOutInOpAsync(ClientOp.CacheGetSize, w => WritePeekModes(modes, w.Stream),
                ctx => ctx.Stream.ReadLong());
        }

        /** <inheritDoc /> */
        public CacheClientConfiguration GetConfiguration()
        {
            return DoOutInOp(ClientOp.CacheGetConfiguration, null,
                ctx => new CacheClientConfiguration(ctx.Stream, ctx.Features));
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

            return new CacheClient<TK1, TV1>(_ignite, _name, true, _expiryPolicy);
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            IgniteArgumentCheck.NotNull(plc, "plc");

            // WithExpiryPolicy is not supported on protocols older than 1.5.0.
            // However, we can't check that here because of partition awareness, reconnect and so on:
            // We don't know which connection is going to be used. This connection may not even exist yet.
            // See WriteRequest.
            return new CacheClient<TK, TV>(_ignite, _name, _keepBinary, plc);
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
        private void DoOutOp(ClientOp opId, Action<ClientRequestContext> writeAction = null)
        {
            DoOutInOp<object>(opId, writeAction, null);
        }

        /// <summary>
        /// Does the out op with partition awareness.
        /// </summary>
        private void DoOutOpAffinity(ClientOp opId, TK key)
        {
            DoOutInOpAffinity<object>(opId, key, null);
        }

        /// <summary>
        /// Does the out op with partition awareness.
        /// </summary>
        private Task DoOutOpAsync(ClientOp opId, Action<ClientRequestContext> writeAction = null)
        {
            return DoOutInOpAsync<object>(opId, writeAction, null);
        }

        /// <summary>
        /// Does the out op with partition awareness.
        /// </summary>
        private Task DoOutOpAffinityAsync(ClientOp opId, TK key, Action<ClientRequestContext> writeAction = null)
        {
            return DoOutInOpAffinityAsync<object>(opId, key, writeAction, null);
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, ctx => WriteRequest(writeAction, ctx),
                readFunc, HandleError<T>);
        }

        /// <summary>
        /// Does the out in op with partition awareness.
        /// </summary>
        private T DoOutInOpAffinity<T>(ClientOp opId, TK key, Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAffinity(
                opId,
                ctx => WriteRequest(c => c.Writer.WriteObjectDetached(key), ctx),
                readFunc,
                _id,
                key,
                HandleError<T>);
        }

        /// <summary>
        /// Does the out in op with partition awareness.
        /// </summary>
        private T DoOutInOpAffinity<T>(ClientOp opId, TK key, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAffinity(
                opId,
                ctx => WriteRequest(writeAction, ctx),
                readFunc,
                _id,
                key,
                HandleError<T>);
        }

        /// <summary>
        /// Does the out in op with partition awareness.
        /// </summary>
        private T DoOutInOpAffinity<T>(ClientOp opId, TK key, TV val, Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAffinity(
                opId,
                ctx => WriteRequest(c =>
                {
                    c.Writer.WriteObjectDetached(key);
                    c.Writer.WriteObjectDetached(val);
                }, ctx),
                readFunc,
                _id,
                key,
                HandleError<T>);
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAsync(opId, ctx => WriteRequest(writeAction, ctx),
                readFunc, HandleError<T>);
        }

        /// <summary>
        /// Does the out in op with partition awareness.
        /// </summary>
        private Task<T> DoOutInOpAffinityAsync<T>(ClientOp opId, TK key, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAffinityAsync(opId, ctx => WriteRequest(writeAction, ctx),
                readFunc, _id, key, HandleError<T>);
        }

        /// <summary>
        /// Does the out in op with partition awareness.
        /// </summary>
        private Task<T> DoOutInOpAffinityAsync<T>(ClientOp opId, TK key, TV val, Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAffinityAsync(
                opId,
                ctx => WriteRequest(c =>
                {
                    c.Writer.WriteObjectDetached(key);
                    c.Writer.WriteObjectDetached(val);
                }, ctx),
                readFunc,
                _id,
                key,
                HandleError<T>);
        }

        /// <summary>
        /// Does the out in op with partition awareness.
        /// </summary>
        private Task<T> DoOutInOpAffinityAsync<T>(ClientOp opId, TK key, Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAffinityAsync(opId,
                stream => WriteRequest(w => w.Writer.WriteObjectDetached(key), stream),
                readFunc, _id, key, HandleError<T>);
        }

        /// <summary>
        /// Writes the request.
        /// </summary>
        private void WriteRequest(Action<ClientRequestContext> writeAction, ClientRequestContext ctx)
        {
            ctx.Stream.WriteInt(_id);

            if (_expiryPolicy != null)
            {
                ctx.Features.ValidateWithExpiryPolicyFlag();
                ctx.Stream.WriteByte((byte) ClientCacheRequestFlag.WithExpiryPolicy);
                ExpiryPolicySerializer.WritePolicy(ctx.Writer, _expiryPolicy);
            }
            else
                ctx.Stream.WriteByte((byte) ClientCacheRequestFlag.None); // Flags (skipStore, etc).

            if (writeAction != null)
            {
                writeAction(ctx);
            }
        }

        /// <summary>
        /// Unmarshals the value, throwing an exception for nulls.
        /// </summary>
        private T UnmarshalNotNull<T>(ClientResponseContext ctx)
        {
            var stream = ctx.Stream;
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
        private CacheResult<T> UnmarshalCacheResult<T>(ClientResponseContext ctx)
        {
            var stream = ctx.Stream;
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
        [Obsolete]
        private static void WriteSqlQuery(IBinaryRawWriter writer, SqlQuery qry)
        {
            Debug.Assert(qry != null);

            writer.WriteString(qry.QueryType);
            writer.WriteString(qry.Sql);
            QueryBase.WriteQueryArgs(writer, qry.Arguments);
            writer.WriteBoolean(qry.EnableDistributedJoins);
            writer.WriteBoolean(qry.Local);
#pragma warning disable 618
            writer.WriteBoolean(qry.ReplicatedOnly);
#pragma warning restore 618
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
            // We could have ExecuteNonQuery method, which uses StatementType.Update, for example.
            writer.WriteByte((byte)StatementType.Any);

            writer.WriteBoolean(qry.EnableDistributedJoins);
            writer.WriteBoolean(qry.Local);
#pragma warning disable 618
            writer.WriteBoolean(qry.ReplicatedOnly);
#pragma warning restore 618
            writer.WriteBoolean(qry.EnforceJoinOrder);
            writer.WriteBoolean(qry.Colocated);
            writer.WriteBoolean(qry.Lazy);
            writer.WriteTimeSpanAsLong(qry.Timeout);
            writer.WriteBoolean(includeColumns);

        }

        /// <summary>
        /// Gets the fields cursor.
        /// </summary>
        private ClientFieldsQueryCursor GetFieldsCursor(ClientResponseContext ctx)
        {
            var cursorId = ctx.Stream.ReadLong();
            var columnNames = ClientFieldsQueryCursor.ReadColumns(ctx.Reader);

            return new ClientFieldsQueryCursor(ctx.Socket, cursorId, _keepBinary, ctx.Stream,
                ClientOp.QuerySqlFieldsCursorGetPage, columnNames);
        }

        /// <summary>
        /// Gets the fields cursor.
        /// </summary>
        private ClientQueryCursorBase<T> GetFieldsCursorNoColumnNames<T>(ClientResponseContext ctx,
            Func<IBinaryRawReader, int, T> readerFunc)
        {
            var cursorId = ctx.Stream.ReadLong();
            var columnCount = ctx.Stream.ReadInt();

            return new ClientQueryCursorBase<T>(ctx.Socket, cursorId, _keepBinary, ctx.Stream,
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
        private static void WritePeekModes(ICollection<CachePeekMode> modes, IBinaryStream w)
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
    }
}

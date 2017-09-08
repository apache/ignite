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

namespace Apache.Ignite.Core.Impl.Cache.Client
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Client.Query;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Common;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Client cache implementation.
    /// </summary>
    internal class CacheClient<TK, TV> : ICache<TK, TV>
    {
        /** Cache name. */
        private readonly string _name;

        /** Cache id. */
        private readonly int _id;

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClient{TK, TV}" /> class.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="name">Cache name.</param>
        public CacheClient(IgniteClient ignite, string name)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(name != null);

            _name = name;
            _ignite = ignite;
            _marsh = _ignite.Marshaller;
            _id = BinaryUtils.GetCacheId(name);
        }

        /** <inheritDoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { throw IgniteClient.GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public CacheConfiguration GetConfiguration()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool IsEmpty()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool IsKeepBinary
        {
            get { throw IgniteClient.GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithSkipStore()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task LoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task LocalLoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void LoadAll(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task LoadAllAsync(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool TryLocalPeek(TK key, out TV value, params CachePeekMode[] modes)
        {
            throw IgniteClient.GetClientNotSupportedException();
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
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            DoOutOp(ClientOp.CachePut, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(val);
            });
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void Clear()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task ClearAsync()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void Clear(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void LocalClear(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void LocalClearAll(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool Remove(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public int GetLocalSize(params CachePeekMode[] modes)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public int GetSize(params CachePeekMode[] modes)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<int> GetSizeAsync(params CachePeekMode[] modes)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            var opId = ClientQueryType.GetClientOp(qry.OpId);

            if (opId == null)
            {
                throw IgniteClient.GetClientNotSupportedException();
            }

            // Filter is a binary object for all platforms.
            // For .NET it is a CacheEntryFilterHolder with a predefined id (BinaryTypeId.CacheEntryPredicateHolder).
            var cursorId = DoOutInOp(opId.Value, w => qry.Write(w, false), s => s.ReadLong());

            return new ClientQueryCursor<TK, TV>(_ignite, cursorId, false);
        }

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle QueryContinuous(ContinuousQuery<TK, TV> qry)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuous(ContinuousQuery<TK, TV> qry, QueryBase initialQry)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IEnumerable<ICacheEntry<TK, TV>> GetLocalEntries(params CachePeekMode[] peekModes)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public TRes Invoke<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<TRes> InvokeAsync<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntryProcessorResult<TK, TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntryProcessorResult<TK, TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TK> keys, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICacheLock Lock(TK key)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICacheLock LockAll(IEnumerable<TK> keys)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool IsLocalLocked(TK key, bool byCurrentThread)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics(IClusterGroup clusterGroup)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetLocalMetrics()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Task Rebalance()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithPartitionRecover()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICollection<int> GetLostPartitions()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction,
            Func<IBinaryStream, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream =>
            {
                stream.WriteInt(_id);
                stream.WriteByte(0);  // Flags (skipStore, etc).

                if (writeAction != null)
                {
                    var writer = _marsh.StartMarshal(stream);

                    writeAction(writer);

                    _marsh.FinishMarshal(writer);
                }
            }, readFunc);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private void DoOutOp(ClientOp opId, Action<BinaryWriter> writeAction)
        {
            DoOutInOp<object>(opId, writeAction, null);
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

            return _marsh.Unmarshal<T>(stream);
        }

        /// <summary>
        /// Gets the key not found exception.
        /// </summary>
        private static KeyNotFoundException GetKeyNotFoundException()
        {
            return new KeyNotFoundException("The given key was not present in the cache.");
        }
    }
}

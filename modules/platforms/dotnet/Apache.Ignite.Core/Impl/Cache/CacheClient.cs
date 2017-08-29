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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Client cache implementation.
    /// </summary>
    internal class CacheClient<TK, TV> : ICache<TK, TV>
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /** Cache name. */
        private readonly string _name;

        /** Cache id. */
        private readonly int _id;

        /** Marshaller */
        private readonly Marshaller _marsh = BinaryUtils.Marshaller;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClient{TK, TV}" /> class.
        /// </summary>
        /// <param name="socket">The socket.</param>
        /// <param name="name">Cache name.</param>
        public CacheClient(ClientSocket socket, string name)
        {
            Debug.Assert(socket != null);
            Debug.Assert(name != null);

            _socket = socket;
            _name = name;
            _id = BinaryUtils.GetCacheId(name);
        }

        /** <inheritDoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            throw new NotImplementedException();
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
            get { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public CacheConfiguration GetConfiguration()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool IsEmpty()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool IsKeepBinary
        {
            get { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithSkipStore()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task LoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task LocalLoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void LoadAll(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task LoadAllAsync(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool TryLocalPeek(TK key, out TV value, params CachePeekMode[] modes)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public TV this[TK key]
        {
            get
            {
                return Get(key);
            }
            set
            {
                Put(key, value);
            }
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            return DoOutInOp(ClientOp.CacheGet, w => w.WriteObject(key), UnmarshalNotNull<TV>);
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void Clear()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task ClearAsync()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void Clear(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void LocalClear(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void LocalClearAll(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool Remove(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public int GetLocalSize(params CachePeekMode[] modes)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public int GetSize(params CachePeekMode[] modes)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<int> GetSizeAsync(params CachePeekMode[] modes)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle QueryContinuous(ContinuousQuery<TK, TV> qry)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuous(ContinuousQuery<TK, TV> qry, QueryBase initialQry)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IEnumerable<ICacheEntry<TK, TV>> GetLocalEntries(params CachePeekMode[] peekModes)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public TRes Invoke<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<TRes> InvokeAsync<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntryProcessorResult<TK, TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntryProcessorResult<TK, TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TK> keys, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICacheLock Lock(TK key)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICacheLock LockAll(IEnumerable<TK> keys)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool IsLocalLocked(TK key, bool byCurrentThread)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics(IClusterGroup clusterGroup)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetLocalMetrics()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task Rebalance()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithPartitionRecover()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICollection<int> GetLostPartitions()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryStream, T> readFunc)
        {
            return _socket.DoOutInOp(opId, stream =>
            {
                stream.WriteInt(_id);
                stream.WriteByte(0);  // Flags (skipStore, etc).

                if (writeAction != null)
                {
                    writeAction(_marsh.StartMarshal(stream));
                }
            }, readFunc);
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

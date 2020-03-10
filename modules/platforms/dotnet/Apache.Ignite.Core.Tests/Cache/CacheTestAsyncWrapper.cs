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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Wraps IGridCache implementation to simplify async mode testing.
    /// </summary>
    internal class CacheTestAsyncWrapper<TK, TV> : ICache<TK, TV>
    {
        private readonly ICache<TK, TV> _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheTestAsyncWrapper{K, V}"/> class.
        /// </summary>
        /// <param name="cache">The cache to be wrapped.</param>
        public CacheTestAsyncWrapper(ICache<TK, TV> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _cache.Name; }
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _cache.Ignite; }
        }

        /** <inheritDoc /> */
        public CacheConfiguration GetConfiguration()
        {
            return _cache.GetConfiguration();
        }

        /** <inheritDoc /> */
        public bool IsEmpty()
        {
            return _cache.IsEmpty();
        }

        /** <inheritDoc /> */
        public bool IsKeepBinary
        {
            get { return _cache.IsKeepBinary; }
        }

        /** <inheritDoc /> */
        public bool IsAllowAtomicOpsInTx
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithSkipStore()
        {
            return _cache.WithSkipStore().WrapAsync();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            return _cache.WithExpiryPolicy(plc).WrapAsync();
        }

        /** <inheritDoc /> */
        public ICache<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            return _cache.WithKeepBinary<TK1, TV1>().WrapAsync();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithAllowAtomicOpsInTx()
        {
            return this;
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            _cache.LoadCacheAsync(p, args).WaitResult();
        }

        /** <inheritDoc /> */
        public Task LoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            return _cache.LoadCacheAsync(p, args);
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            _cache.LocalLoadCacheAsync(p, args).WaitResult();
        }

        /** <inheritDoc /> */
        public Task LocalLoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            return _cache.LocalLoadCacheAsync(p, args);
        }

        /** <inheritDoc /> */
        public void LoadAll(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            _cache.LoadAll(keys, replaceExistingValues);
        }

        /** <inheritDoc /> */
        public Task LoadAllAsync(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            return _cache.LoadAllAsync(keys, replaceExistingValues);
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            return _cache.ContainsKeyAsync(key).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            return _cache.ContainsKeyAsync(key);
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            return _cache.ContainsKeysAsync(keys).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            return _cache.ContainsKeysAsync(keys);
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            return _cache.LocalPeek(key, modes);
        }

        /** <inheritDoc /> */
        public bool TryLocalPeek(TK key, out TV value, params CachePeekMode[] modes)
        {
            return _cache.TryLocalPeek(key, out value, modes);
        }

        /** <inheritDoc /> */
        public TV this[TK key]
        {
            get { return _cache[key]; }
            set { _cache[key] = value; }
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            return _cache.GetAsync(key).GetResult();
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            return _cache.GetAsync(key);
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            return _cache.TryGet(key, out value);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            return _cache.TryGetAsync(key);
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            return _cache.GetAllAsync(keys).GetResult();
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            return _cache.GetAllAsync(keys);
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            _cache.PutAsync(key, val).WaitResult();
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            return _cache.PutAsync(key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            return _cache.GetAndPutAsync(key, val).GetResult();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            return _cache.GetAndPutAsync(key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            return _cache.GetAndReplaceAsync(key, val).GetResult();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            return _cache.GetAndReplaceAsync(key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            return _cache.GetAndRemoveAsync(key).GetResult();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            return _cache.GetAndRemoveAsync(key);
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            return _cache.PutIfAbsentAsync(key, val).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            return _cache.PutIfAbsentAsync(key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            return _cache.GetAndPutIfAbsentAsync(key, val).GetResult();
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            return _cache.GetAndPutIfAbsentAsync(key, val);
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            return _cache.ReplaceAsync(key, val).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            return _cache.ReplaceAsync(key, val);
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            return _cache.ReplaceAsync(key, oldVal, newVal).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            return _cache.ReplaceAsync(key, oldVal, newVal);
        }

        /** <inheritDoc /> */
        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            _cache.PutAllAsync(vals).WaitResult();
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            return _cache.PutAllAsync(vals);
        }

        /** <inheritDoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            _cache.LocalEvict(keys);
        }

        /** <inheritDoc /> */
        public void Clear()
        {
            _cache.ClearAsync().WaitResult();
        }

        /** <inheritDoc /> */
        public Task ClearAsync()
        {
            return _cache.ClearAsync();
        }

        /** <inheritDoc /> */
        public void Clear(TK key)
        {
            _cache.ClearAsync(key).WaitResult();
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            return _cache.ClearAsync(key);
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            _cache.ClearAllAsync(keys).WaitResult();
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            return _cache.ClearAllAsync(keys);
        }

        /** <inheritDoc /> */
        public void LocalClear(TK key)
        {
            _cache.LocalClear(key);
        }

        /** <inheritDoc /> */
        public void LocalClearAll(IEnumerable<TK> keys)
        {
            _cache.LocalClearAll(keys);
        }

        /** <inheritDoc /> */
        public bool Remove(TK key)
        {
            return _cache.RemoveAsync(key).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            return _cache.RemoveAsync(key);
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            return _cache.RemoveAsync(key, val).GetResult();
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            return _cache.RemoveAsync(key, val);
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            Task task = _cache.RemoveAllAsync(keys);
            task.WaitResult();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            return _cache.RemoveAllAsync(keys);
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            Task task = _cache.RemoveAllAsync();
            task.WaitResult();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            return _cache.RemoveAllAsync();
        }

        /** <inheritDoc /> */
        public int GetLocalSize(params CachePeekMode[] modes)
        {
            return _cache.GetLocalSize(modes);
        }

        /** <inheritDoc /> */
        public int GetSize(params CachePeekMode[] modes)
        {
            return _cache.GetSizeAsync(modes).GetResult();
        }

        /** <inheritDoc /> */
        public Task<int> GetSizeAsync(params CachePeekMode[] modes)
        {
            return _cache.GetSizeAsync(modes);
        }

        /** <inheritDoc /> */
        public long GetSizeLong(params CachePeekMode[] modes)
        {
            return _cache.GetSizeLongAsync(modes).GetResult();
        }

        /** <inheritDoc /> */
        public long GetSizeLong(int partition, params CachePeekMode[] modes)
        {
            return _cache.GetSizeLongAsync(partition, modes).GetResult();
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeLongAsync(params CachePeekMode[] modes)
        {
            return _cache.GetSizeLongAsync(modes);
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeLongAsync(int partition, params CachePeekMode[] modes)
        {
            return _cache.GetSizeLongAsync(partition, modes);
        }

        /** <inheritDoc /> */
        public long GetLocalSizeLong(params CachePeekMode[] modes)
        {
            return _cache.GetLocalSizeLong(modes);
        }

        /** <inheritDoc /> */
        public long GetLocalSizeLong(int partition, params CachePeekMode[] modes)
        {
            return _cache.GetLocalSizeLong(partition, modes);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            return _cache.Query(qry);
        }

        /** <inheritDoc /> */
        public IFieldsQueryCursor Query(SqlFieldsQuery qry)
        {
            return _cache.Query(qry);
        }

        /** <inheritDoc /> */
        [Obsolete]
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            return _cache.QueryFields(qry);
        }

        /** <inheritDoc /> */
        IContinuousQueryHandle ICache<TK, TV>.QueryContinuous(ContinuousQuery<TK, TV> qry)
        {
            return _cache.QueryContinuous(qry);
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuous(ContinuousQuery<TK, TV> qry, QueryBase initialQry)
        {
            return _cache.QueryContinuous(qry, initialQry);
        }

        /** <inheritDoc /> */
        public IEnumerable<ICacheEntry<TK, TV>> GetLocalEntries(params CachePeekMode[] peekModes)
        {
            return _cache.GetLocalEntries(peekModes);
        }

        /** <inheritDoc /> */
        public TRes Invoke<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            return _cache.InvokeAsync(key, processor, arg).GetResult();
        }

        /** <inheritDoc /> */
        public Task<TRes> InvokeAsync<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            return _cache.InvokeAsync(key, processor, arg);
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntryProcessorResult<TK, TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys, 
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            return _cache.InvokeAllAsync(keys, processor, arg).GetResult();
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntryProcessorResult<TK, TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TK> keys, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            return _cache.InvokeAllAsync(keys, processor, arg);
        }

        /** <inheritDoc /> */
        public ICacheLock Lock(TK key)
        {
            return _cache.Lock(key);
        }

        /** <inheritDoc /> */
        public ICacheLock LockAll(IEnumerable<TK> keys)
        {
            return _cache.LockAll(keys);
        }

        /** <inheritDoc /> */
        public bool IsLocalLocked(TK key, bool byCurrentThread)
        {
            return _cache.IsLocalLocked(key, byCurrentThread);
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            return _cache.GetMetrics();
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics(IClusterGroup clusterGroup)
        {
            return _cache.GetMetrics(clusterGroup);
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetLocalMetrics()
        {
            return _cache.GetLocalMetrics();
        }

        /** <inheritDoc /> */
        public void EnableStatistics(bool enabled)
        {
            _cache.EnableStatistics(enabled);
        }

        /** <inheritDoc /> */
        public Task Rebalance()
        {
            return _cache.Rebalance();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            return _cache.WithNoRetries();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithPartitionRecover()
        {
            return _cache.WithPartitionRecover();
        }

        /** <inheritDoc /> */
        public ICollection<int> GetLostPartitions()
        {
            return _cache.GetLostPartitions();
        }

        /** <inheritDoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            return _cache.GetEnumerator();
        }

        /** <inheritDoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IQueryMetrics GetQueryMetrics()
        {
            return _cache.GetQueryMetrics();
        }

        public void ResetQueryMetrics()
        {
            _cache.ResetQueryMetrics();
        }

        public void PreloadPartition(int partition)
        {
            _cache.PreloadPartitionAsync(partition).WaitResult();
        }

        public Task PreloadPartitionAsync(int partition)
        {
            return _cache.PreloadPartitionAsync(partition);
        }

        public bool LocalPreloadPartition(int partition)
        {
            return _cache.LocalPreloadPartition(partition);
        }
    }

    /// <summary>
    /// Extension methods for IGridCache.
    /// </summary>
    public static class CacheExtensions
    {
        /// <summary>
        /// Wraps specified instance into GridCacheTestAsyncWrapper.
        /// </summary>
        public static ICache<TK, TV> WrapAsync<TK, TV>(this ICache<TK, TV> cache)
        {
            return new CacheTestAsyncWrapper<TK, TV>(cache);
        }
    }
}
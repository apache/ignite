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

#pragma warning disable 618
namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client.Cache;

    /// <summary>
    /// Adapts <see cref="ICache{TK,TV}"/> to <see cref="ICacheClient{TK,TV}"/>
    /// </summary>
    public class ClientServerCacheAdapter<TK, TV> : ICacheClient<TK, TV>
    {
        private readonly ICache<TK, TV> _cache;

        public ClientServerCacheAdapter(ICache<TK, TV> cache)
        {
            _cache = cache;
        }

        public string Name
        {
            get { return _cache.Name; }
        }

        public void Put(TK key, TV val)
        {
            _cache.Put(key, val);
        }

        public Task PutAsync(TK key, TV val)
        {
            return _cache.PutAsync(key, val);
        }

        public TV Get(TK key)
        {
            return _cache.Get(key);
        }

        public Task<TV> GetAsync(TK key)
        {
            return _cache.GetAsync(key);
        }

        public bool TryGet(TK key, out TV value)
        {
            return _cache.TryGet(key, out value);
        }

        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            return _cache.TryGetAsync(key);
        }

        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            return _cache.GetAll(keys);
        }

        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            return _cache.GetAllAsync(keys);
        }

        public TV this[TK key]
        {
            get { return _cache[key]; }
            set { _cache[key] = value; }
        }

        public bool ContainsKey(TK key)
        {
            return _cache.ContainsKey(key);
        }

        public Task<bool> ContainsKeyAsync(TK key)
        {
            return _cache.ContainsKeyAsync(key);
        }

        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            return _cache.ContainsKeys(keys);
        }

        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            return _cache.ContainsKeysAsync(keys);
        }

        public IQueryCursor<ICacheEntry<TK, TV>> Query(ScanQuery<TK, TV> scanQuery)
        {
            return _cache.Query(scanQuery);
        }

        public IQueryCursor<ICacheEntry<TK, TV>> Query(SqlQuery sqlQuery)
        {
            return _cache.Query(sqlQuery);
        }

        public IFieldsQueryCursor Query(SqlFieldsQuery sqlFieldsQuery)
        {
            return _cache.Query(sqlFieldsQuery);
        }

        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            return _cache.GetAndPut(key, val);
        }

        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            return _cache.GetAndPutAsync(key, val);
        }

        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            return _cache.GetAndReplace(key, val);
        }

        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            return _cache.GetAndReplaceAsync(key, val);
        }

        public CacheResult<TV> GetAndRemove(TK key)
        {
            return _cache.GetAndRemove(key);
        }

        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            return _cache.GetAndRemoveAsync(key);
        }

        public bool PutIfAbsent(TK key, TV val)
        {
            return _cache.PutIfAbsent(key, val);
        }

        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            return _cache.PutIfAbsentAsync(key, val);
        }

        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            return _cache.GetAndPutIfAbsent(key, val);
        }

        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            return _cache.GetAndPutIfAbsentAsync(key, val);
        }

        public bool Replace(TK key, TV val)
        {
            return _cache.Replace(key, val);
        }

        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            return _cache.ReplaceAsync(key, val);
        }

        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            return _cache.Replace(key, oldVal, newVal);
        }

        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            return _cache.ReplaceAsync(key, oldVal, newVal);
        }

        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            _cache.PutAll(vals);
        }

        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            return _cache.PutAllAsync(vals);
        }

        public void Clear()
        {
            _cache.Clear();
        }

        public Task ClearAsync()
        {
            return _cache.ClearAsync();
        }

        public void Clear(TK key)
        {
            _cache.Clear(key);
        }

        public Task ClearAsync(TK key)
        {
            return _cache.ClearAsync(key);
        }

        public void ClearAll(IEnumerable<TK> keys)
        {
            _cache.ClearAll(keys);
        }

        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            return _cache.ClearAllAsync(keys);
        }

        public bool Remove(TK key)
        {
            return _cache.Remove(key);
        }

        public Task<bool> RemoveAsync(TK key)
        {
            return _cache.RemoveAsync(key);
        }

        public bool Remove(TK key, TV val)
        {
            return _cache.Remove(key, val);
        }

        public Task<bool> RemoveAsync(TK key, TV val)
        {
            return _cache.RemoveAsync(key, val);
        }

        public void RemoveAll(IEnumerable<TK> keys)
        {
            _cache.RemoveAll(keys);
        }

        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            return _cache.RemoveAllAsync(keys);
        }

        public void RemoveAll()
        {
            _cache.RemoveAll();
        }

        public Task RemoveAllAsync()
        {
            return _cache.RemoveAllAsync();
        }

        public long GetSize(params CachePeekMode[] modes)
        {
            return _cache.GetSize(modes);
        }

        public Task<long> GetSizeAsync(params CachePeekMode[] modes)
        {
            throw new NotSupportedException();
        }

        public CacheClientConfiguration GetConfiguration()
        {
            throw new NotSupportedException();
        }

        public ICacheClient<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            throw new NotSupportedException();
        }

        public ICacheClient<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            throw new NotSupportedException();
        }
    }
}
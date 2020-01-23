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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client.Cache;

    /// <summary>
    /// Cache client async wrapper.
    /// </summary>
    public class CacheClientAsyncWrapper<TK, TV> : ICacheClient<TK, TV>
    {
        /** */
        private readonly ICacheClient<TK, TV> _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClientAsyncWrapper{TK, TV}"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public CacheClientAsyncWrapper(ICacheClient<TK, TV> cache)
        {
            _cache = cache;
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _cache.Name; }
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
            var res = _cache.TryGetAsync(key).GetResult();
            value = res.Value;
            return res.Success;
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
        public TV this[TK key]
        {
            get { return _cache[key]; }
            set { _cache[key] = value; }
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
        public IQueryCursor<ICacheEntry<TK, TV>> Query(ScanQuery<TK, TV> scanQuery)
        {
            return _cache.Query(scanQuery);
        }

        /** <inheritDoc /> */
#pragma warning disable 618
        public IQueryCursor<ICacheEntry<TK, TV>> Query(SqlQuery sqlQuery)
        {
            return _cache.Query(sqlQuery);
        }
#pragma warning restore 618

        /** <inheritDoc /> */
        public IFieldsQueryCursor Query(SqlFieldsQuery sqlFieldsQuery)
        {
            return _cache.Query(sqlFieldsQuery);
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
            _cache.RemoveAllAsync(keys).WaitResult();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            return _cache.RemoveAllAsync(keys);
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            _cache.RemoveAllAsync().WaitResult();
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            return _cache.RemoveAllAsync();
        }

        /** <inheritDoc /> */
        public long GetSize(params CachePeekMode[] modes)
        {
            return _cache.GetSizeAsync(modes).GetResult();
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeAsync(params CachePeekMode[] modes)
        {
            return _cache.GetSizeAsync(modes);
        }

        /** <inheritDoc /> */
        public CacheClientConfiguration GetConfiguration()
        {
            return _cache.GetConfiguration();
        }

        /** <inheritDoc /> */
        public ICacheClient<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            return _cache.WithKeepBinary<TK1, TV1>();
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            return _cache.WithExpiryPolicy(plc);
        }
    }
}

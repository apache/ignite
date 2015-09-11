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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;

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
            Debug.Assert(cache.IsAsync, "GridCacheTestAsyncWrapper only works with async caches.");

            _cache = cache;
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithAsync()
        {
            return this;
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            Debug.Fail("GridCacheTestAsyncWrapper.Future() should not be called. It always returns null.");
            return null;
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            Debug.Fail("GridCacheTestAsyncWrapper.Future() should not be called. It always returns null.");
            return null;
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
        public bool IsEmpty
        {
            get { return _cache.IsEmpty; }
        }

        /** <inheritDoc /> */
        public bool KeepPortable
        {
            get { return _cache.KeepPortable; }
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
        public ICache<TK1, TV1> WithKeepPortable<TK1, TV1>()
        {
            return _cache.WithKeepPortable<TK1, TV1>().WrapAsync();
        }
        
        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            _cache.LoadCache(p, args);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            _cache.LocalLoadCache(p, args);
            WaitResult();
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            _cache.ContainsKey(key);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            _cache.ContainsKeys(keys);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            _cache.LocalPeek(key, modes);
            return GetResult<TV>();
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            _cache.Get(key);
            return GetResult<TV>();
        }

        /** <inheritDoc /> */
        public IDictionary<TK, TV> GetAll(IEnumerable<TK> keys)
        {
            _cache.GetAll(keys);
            return GetResult<IDictionary<TK, TV>>();
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            _cache.Put(key, val);
            WaitResult();
        }

        /** <inheritDoc /> */
        public TV GetAndPut(TK key, TV val)
        {
            _cache.GetAndPut(key, val);
            return GetResult<TV>();
        }

        /** <inheritDoc /> */
        public TV GetAndReplace(TK key, TV val)
        {
            _cache.GetAndReplace(key, val);
            return GetResult<TV>();
        }

        /** <inheritDoc /> */
        public TV GetAndRemove(TK key)
        {
            _cache.GetAndRemove(key);
            return GetResult<TV>();
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            _cache.PutIfAbsent(key, val);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public TV GetAndPutIfAbsent(TK key, TV val)
        {
            _cache.GetAndPutIfAbsent(key, val);
            return GetResult<TV>();
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            _cache.Replace(key, val);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            _cache.Replace(key, oldVal, newVal);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public void PutAll(IDictionary<TK, TV> vals)
        {
            _cache.PutAll(vals);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            _cache.LocalEvict(keys);
        }

        /** <inheritDoc /> */
        public void Clear()
        {
            _cache.Clear();
            WaitResult();
        }

        /** <inheritDoc /> */
        public void Clear(TK key)
        {
            _cache.Clear(key);
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            _cache.ClearAll(keys);
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
            _cache.Remove(key);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            _cache.Remove(key, val);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            _cache.RemoveAll(keys);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            _cache.RemoveAll();
            WaitResult();
        }

        /** <inheritDoc /> */
        public int LocalSize(params CachePeekMode[] modes)
        {
            return _cache.LocalSize(modes);
        }

        /** <inheritDoc /> */
        public int Size(params CachePeekMode[] modes)
        {
            _cache.Size(modes);
            return GetResult<int>();
        }

        /** <inheritDoc /> */
        public void LocalPromote(IEnumerable<TK> keys)
        {
            _cache.LocalPromote(keys);
        }
        
        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            return _cache.Query(qry);
        }

        /** <inheritDoc /> */
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
        public TR Invoke<TR, TA>(TK key, ICacheEntryProcessor<TK, TV, TA, TR> processor, TA arg)
        {
            _cache.Invoke(key, processor, arg);
            
            return GetResult<TR>();
        }

        /** <inheritDoc /> */
        public IDictionary<TK, ICacheEntryProcessorResult<TR>> InvokeAll<TR, TA>(IEnumerable<TK> keys, 
            ICacheEntryProcessor<TK, TV, TA, TR> processor, TA arg)
        {
            _cache.InvokeAll(keys, processor, arg);

            return GetResult<IDictionary<TK, ICacheEntryProcessorResult<TR>>>();
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
        public IFuture Rebalance()
        {
            return _cache.Rebalance();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            return _cache.WithNoRetries();
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

        /// <summary>
        /// Waits for the async result.
        /// </summary>
        private void WaitResult()
        {
            GetResult<object>();
        }

        /// <summary>
        /// Gets the async result.
        /// </summary>
        private T GetResult<T>()
        {
            return _cache.GetFuture<T>().Get();
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
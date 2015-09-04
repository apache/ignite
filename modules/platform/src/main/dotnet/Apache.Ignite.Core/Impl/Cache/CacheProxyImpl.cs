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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Cache proxy.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheProxyImpl<TK, TV> : ICache<TK, TV>
    {
        /** wrapped cache instance */
        private readonly CacheImpl<TK, TV> _cache;

        /** */
        private readonly ThreadLocal<int> _lastAsyncOp = new ThreadLocal<int>(() => PlatformTarget.OpNone);

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheProxyImpl{K, V}"/> class.
        /// </summary>
        /// <param name="cache">The cache to wrap.</param>
        public CacheProxyImpl(CacheImpl<TK, TV> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithSkipStore()
        {
            return _cache.IsSkipStore ? this : new CacheProxyImpl<TK, TV>((CacheImpl<TK, TV>)_cache.WithSkipStore());
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            return new CacheProxyImpl<TK, TV>((CacheImpl<TK, TV>)_cache.WithExpiryPolicy(plc));
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithAsync()
        {
            return IsAsync ? this : new CacheProxyImpl<TK, TV>((CacheImpl<TK, TV>) _cache.WithAsync());
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return _cache.IsAsync; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            var fut = _cache.GetFuture<TResult>(_lastAsyncOp.Value);

            ClearLastAsyncOp();

            return fut;
        }

        /** <inheritDoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            return _cache.GetEnumerator();
        }

        /** <inheritDoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) _cache).GetEnumerator();
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

        /// <summary>
        /// Skip store flag.
        /// </summary>
        internal bool SkipStore
        {
            get { return _cache.IsSkipStore; }
        }

        /** <inheritDoc /> */
        public ICache<TK1, TV1> WithKeepPortable<TK1, TV1>()
        {
            return new CacheProxyImpl<TK1, TV1>((CacheImpl<TK1, TV1>) _cache.WithKeepPortable<TK1, TV1>());
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            _cache.LoadCache(p, args);

            SetLastAsyncOp(CacheOp.LoadCache);
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            _cache.LocalLoadCache(p, args);

            SetLastAsyncOp(CacheOp.LocLoadCache);
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            var result = _cache.ContainsKey(key);
            
            SetLastAsyncOp(CacheOp.ContainsKey);

            return result;
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            var result = _cache.ContainsKeys(keys);

            SetLastAsyncOp(CacheOp.ContainsKeys);

            return result;
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            return _cache.LocalPeek(key, modes);
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            var result = _cache.Get(key);
            
            SetLastAsyncOp(CacheOp.Get);

            return result;
        }

        /** <inheritDoc /> */
        public IDictionary<TK, TV> GetAll(IEnumerable<TK> keys)
        {
            var result = _cache.GetAll(keys);

            SetLastAsyncOp(CacheOp.GetAll);

            return result;
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            _cache.Put(key, val);

            SetLastAsyncOp(CacheOp.Put);
        }

        /** <inheritDoc /> */
        public TV GetAndPut(TK key, TV val)
        {
            var result = _cache.GetAndPut(key, val);

            SetLastAsyncOp(CacheOp.GetAndPut);

            return result;
        }

        /** <inheritDoc /> */
        public TV GetAndReplace(TK key, TV val)
        {
            var result = _cache.GetAndReplace(key, val);

            SetLastAsyncOp(CacheOp.GetAndReplace);

            return result;
        }

        /** <inheritDoc /> */
        public TV GetAndRemove(TK key)
        {
            var result = _cache.GetAndRemove(key);

            SetLastAsyncOp(CacheOp.GetAndRemove);

            return result;
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            var result = _cache.PutIfAbsent(key, val);

            SetLastAsyncOp(CacheOp.PutIfAbsent);

            return result;
        }

        /** <inheritDoc /> */
        public TV GetAndPutIfAbsent(TK key, TV val)
        {
            var result = _cache.GetAndPutIfAbsent(key, val);

            SetLastAsyncOp(CacheOp.GetAndPutIfAbsent);

            return result;
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV val)
        {
            var result = _cache.Replace(key, val);

            SetLastAsyncOp(CacheOp.Replace2);

            return result;
        }

        /** <inheritDoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            var result = _cache.Replace(key, oldVal, newVal);

            SetLastAsyncOp(CacheOp.Replace3);

            return result;
        }

        /** <inheritDoc /> */
        public void PutAll(IDictionary<TK, TV> vals)
        {
            _cache.PutAll(vals);

            SetLastAsyncOp(CacheOp.PutAll);
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

            ClearLastAsyncOp();
        }

        /** <inheritDoc /> */
        public void Clear(TK key)
        {
            _cache.Clear(key);

            SetLastAsyncOp(CacheOp.Clear);
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            _cache.ClearAll(keys);
            
            SetLastAsyncOp(CacheOp.ClearAll);
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
            var result = _cache.Remove(key);

            SetLastAsyncOp(CacheOp.RemoveObj);

            return result;
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            var result = _cache.Remove(key, val);

            SetLastAsyncOp(CacheOp.RemoveBool);

            return result;
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            _cache.RemoveAll(keys);

            SetLastAsyncOp(CacheOp.RemoveAll);
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            _cache.RemoveAll();

            ClearLastAsyncOp();
        }

        /** <inheritDoc /> */
        public int LocalSize(params CachePeekMode[] modes)
        {
            return _cache.LocalSize(modes);
        }

        /** <inheritDoc /> */
        public int Size(params CachePeekMode[] modes)
        {
            var result = _cache.Size(modes);

            ClearLastAsyncOp();

            return result;
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
        public IContinuousQueryHandle QueryContinuous(ContinuousQuery<TK, TV> qry)
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
            var result = _cache.Invoke(key, processor, arg);

            SetLastAsyncOp(CacheOp.Invoke);

            return result;
        }

        /** <inheritDoc /> */
        public IDictionary<TK, ICacheEntryProcessorResult<TR>> InvokeAll<TR, TA>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TA, TR> processor, TA arg)
        {
            var result = _cache.InvokeAll(keys, processor, arg);

            SetLastAsyncOp(CacheOp.InvokeAll);

            return result;
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
            return _cache.IsNoRetries ? this : new CacheProxyImpl<TK, TV>((CacheImpl<TK, TV>) _cache.WithNoRetries());
        }

        /// <summary>
        /// Sets the last asynchronous op id.
        /// </summary>
        /// <param name="opId">The op identifier.</param>
        private void SetLastAsyncOp(CacheOp opId)
        {
            if (IsAsync)
                _lastAsyncOp.Value = (int) opId;
        }

        /// <summary>
        /// Clears the last asynchronous op id.
        /// This should be called in the end of each method that supports async and does not call SetLastAsyncOp.
        /// </summary>
        private void ClearLastAsyncOp()
        {
            if (IsAsync)
                _lastAsyncOp.Value = PlatformTarget.OpNone;
        }
    }
}
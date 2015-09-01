﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
    internal class CacheProxyImpl<K, V> : ICache<K, V>
    {
        /** wrapped cache instance */
        private readonly CacheImpl<K, V> cache;

        /** */
        private readonly ThreadLocal<int> lastAsyncOp = new ThreadLocal<int>(() => GridTarget.OP_NONE);

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheProxyImpl{K, V}"/> class.
        /// </summary>
        /// <param name="cache">The cache to wrap.</param>
        public CacheProxyImpl(CacheImpl<K, V> cache)
        {
            Debug.Assert(cache != null);

            this.cache = cache;
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithSkipStore()
        {
            return cache.IsSkipStore ? this : new CacheProxyImpl<K, V>((CacheImpl<K, V>)cache.WithSkipStore());
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithExpiryPolicy(IExpiryPolicy plc)
        {
            return new CacheProxyImpl<K, V>((CacheImpl<K, V>)cache.WithExpiryPolicy(plc));
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithAsync()
        {
            return IsAsync ? this : new CacheProxyImpl<K, V>((CacheImpl<K, V>) cache.WithAsync());
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return cache.IsAsync; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            var fut = cache.GetFuture<TResult>(lastAsyncOp.Value);

            ClearLastAsyncOp();

            return fut;
        }

        /** <inheritDoc /> */
        public IEnumerator<ICacheEntry<K, V>> GetEnumerator()
        {
            return cache.GetEnumerator();
        }

        /** <inheritDoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) cache).GetEnumerator();
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return cache.Name; }
        }

        /** <inheritDoc /> */
        public IIgnite Grid
        {
            get { return cache.Grid; }
        }

        /** <inheritDoc /> */
        public bool IsEmpty
        {
            get { return cache.IsEmpty; }
        }

        /** <inheritDoc /> */
        public bool KeepPortable
        {
            get { return cache.KeepPortable; }
        }

        /// <summary>
        /// Skip store flag.
        /// </summary>
        internal bool SkipStore
        {
            get { return cache.IsSkipStore; }
        }

        /** <inheritDoc /> */
        public ICache<K1, V1> WithKeepPortable<K1, V1>()
        {
            return new CacheProxyImpl<K1, V1>((CacheImpl<K1, V1>) cache.WithKeepPortable<K1, V1>());
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<K, V> p, params object[] args)
        {
            cache.LoadCache(p, args);

            SetLastAsyncOp(CacheOp.LOAD_CACHE);
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<K, V> p, params object[] args)
        {
            cache.LocalLoadCache(p, args);

            SetLastAsyncOp(CacheOp.LOC_LOAD_CACHE);
        }

        /** <inheritDoc /> */
        public bool ContainsKey(K key)
        {
            var result = cache.ContainsKey(key);
            
            SetLastAsyncOp(CacheOp.CONTAINS_KEY);

            return result;
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<K> keys)
        {
            var result = cache.ContainsKeys(keys);

            SetLastAsyncOp(CacheOp.CONTAINS_KEYS);

            return result;
        }

        /** <inheritDoc /> */
        public V LocalPeek(K key, params CachePeekMode[] modes)
        {
            return cache.LocalPeek(key, modes);
        }

        /** <inheritDoc /> */
        public V Get(K key)
        {
            var result = cache.Get(key);
            
            SetLastAsyncOp(CacheOp.GET);

            return result;
        }

        /** <inheritDoc /> */
        public IDictionary<K, V> GetAll(IEnumerable<K> keys)
        {
            var result = cache.GetAll(keys);

            SetLastAsyncOp(CacheOp.GET_ALL);

            return result;
        }

        /** <inheritDoc /> */
        public void Put(K key, V val)
        {
            cache.Put(key, val);

            SetLastAsyncOp(CacheOp.PUT);
        }

        /** <inheritDoc /> */
        public V GetAndPut(K key, V val)
        {
            var result = cache.GetAndPut(key, val);

            SetLastAsyncOp(CacheOp.GET_AND_PUT);

            return result;
        }

        /** <inheritDoc /> */
        public V GetAndReplace(K key, V val)
        {
            var result = cache.GetAndReplace(key, val);

            SetLastAsyncOp(CacheOp.GET_AND_REPLACE);

            return result;
        }

        /** <inheritDoc /> */
        public V GetAndRemove(K key)
        {
            var result = cache.GetAndRemove(key);

            SetLastAsyncOp(CacheOp.GET_AND_REMOVE);

            return result;
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(K key, V val)
        {
            var result = cache.PutIfAbsent(key, val);

            SetLastAsyncOp(CacheOp.PUT_IF_ABSENT);

            return result;
        }

        /** <inheritDoc /> */
        public V GetAndPutIfAbsent(K key, V val)
        {
            var result = cache.GetAndPutIfAbsent(key, val);

            SetLastAsyncOp(CacheOp.GET_AND_PUT_IF_ABSENT);

            return result;
        }

        /** <inheritDoc /> */
        public bool Replace(K key, V val)
        {
            var result = cache.Replace(key, val);

            SetLastAsyncOp(CacheOp.REPLACE_2);

            return result;
        }

        /** <inheritDoc /> */
        public bool Replace(K key, V oldVal, V newVal)
        {
            var result = cache.Replace(key, oldVal, newVal);

            SetLastAsyncOp(CacheOp.REPLACE_3);

            return result;
        }

        /** <inheritDoc /> */
        public void PutAll(IDictionary<K, V> vals)
        {
            cache.PutAll(vals);

            SetLastAsyncOp(CacheOp.PUT_ALL);
        }

        /** <inheritDoc /> */
        public void LocalEvict(IEnumerable<K> keys)
        {
            cache.LocalEvict(keys);
        }

        /** <inheritDoc /> */
        public void Clear()
        {
            cache.Clear();

            ClearLastAsyncOp();
        }

        /** <inheritDoc /> */
        public void Clear(K key)
        {
            cache.Clear(key);

            SetLastAsyncOp(CacheOp.CLEAR);
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<K> keys)
        {
            cache.ClearAll(keys);
            
            SetLastAsyncOp(CacheOp.CLEAR_ALL);
        }

        /** <inheritDoc /> */
        public void LocalClear(K key)
        {
            cache.LocalClear(key);
        }

        /** <inheritDoc /> */
        public void LocalClearAll(IEnumerable<K> keys)
        {
            cache.LocalClearAll(keys);
        }

        /** <inheritDoc /> */
        public bool Remove(K key)
        {
            var result = cache.Remove(key);

            SetLastAsyncOp(CacheOp.REMOVE_OBJ);

            return result;
        }

        /** <inheritDoc /> */
        public bool Remove(K key, V val)
        {
            var result = cache.Remove(key, val);

            SetLastAsyncOp(CacheOp.REMOVE_BOOL);

            return result;
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<K> keys)
        {
            cache.RemoveAll(keys);

            SetLastAsyncOp(CacheOp.REMOVE_ALL);
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            cache.RemoveAll();

            ClearLastAsyncOp();
        }

        /** <inheritDoc /> */
        public int LocalSize(params CachePeekMode[] modes)
        {
            return cache.LocalSize(modes);
        }

        /** <inheritDoc /> */
        public int Size(params CachePeekMode[] modes)
        {
            var result = cache.Size(modes);

            ClearLastAsyncOp();

            return result;
        }

        /** <inheritDoc /> */
        public void LocalPromote(IEnumerable<K> keys)
        {
            cache.LocalPromote(keys);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<K, V>> Query(QueryBase qry)
        {
            return cache.Query(qry);
        }

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            return cache.QueryFields(qry);
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle QueryContinuous(ContinuousQuery<K, V> qry)
        {
            return cache.QueryContinuous(qry);
        }

        /** <inheritDoc /> */
        public IContinuousQueryHandle<ICacheEntry<K, V>> QueryContinuous(ContinuousQuery<K, V> qry, QueryBase initialQry)
        {
            return cache.QueryContinuous(qry, initialQry);
        }

        /** <inheritDoc /> */
        public IEnumerable<ICacheEntry<K, V>> GetLocalEntries(params CachePeekMode[] peekModes)
        {
            return cache.GetLocalEntries(peekModes);
        }

        /** <inheritDoc /> */
        public R Invoke<R, A>(K key, ICacheEntryProcessor<K, V, A, R> processor, A arg)
        {
            var result = cache.Invoke(key, processor, arg);

            SetLastAsyncOp(CacheOp.INVOKE);

            return result;
        }

        /** <inheritDoc /> */
        public IDictionary<K, ICacheEntryProcessorResult<R>> InvokeAll<R, A>(IEnumerable<K> keys,
            ICacheEntryProcessor<K, V, A, R> processor, A arg)
        {
            var result = cache.InvokeAll(keys, processor, arg);

            SetLastAsyncOp(CacheOp.INVOKE_ALL);

            return result;
        }

        /** <inheritDoc /> */
        public ICacheLock Lock(K key)
        {
            return cache.Lock(key);
        }

        /** <inheritDoc /> */
        public ICacheLock LockAll(IEnumerable<K> keys)
        {
            return cache.LockAll(keys);
        }

        /** <inheritDoc /> */
        public bool IsLocalLocked(K key, bool byCurrentThread)
        {
            return cache.IsLocalLocked(key, byCurrentThread);
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            return cache.GetMetrics();
        }

        /** <inheritDoc /> */
        public IFuture Rebalance()
        {
            return cache.Rebalance();
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithNoRetries()
        {
            return cache.IsNoRetries ? this : new CacheProxyImpl<K, V>((CacheImpl<K, V>) cache.WithNoRetries());
        }

        /// <summary>
        /// Sets the last asynchronous op id.
        /// </summary>
        /// <param name="opId">The op identifier.</param>
        private void SetLastAsyncOp(CacheOp opId)
        {
            if (IsAsync)
                lastAsyncOp.Value = (int) opId;
        }

        /// <summary>
        /// Clears the last asynchronous op id.
        /// This should be called in the end of each method that supports async and does not call SetLastAsyncOp.
        /// </summary>
        private void ClearLastAsyncOp()
        {
            if (IsAsync)
                lastAsyncOp.Value = GridTarget.OP_NONE;
        }
    }
}
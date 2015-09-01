/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
    internal class CacheTestAsyncWrapper<K, V> : ICache<K, V>
    {
        private readonly ICache<K, V> cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheTestAsyncWrapper{K, V}"/> class.
        /// </summary>
        /// <param name="cache">The cache to be wrapped.</param>
        public CacheTestAsyncWrapper(ICache<K, V> cache)
        {
            Debug.Assert(cache.IsAsync, "GridCacheTestAsyncWrapper only works with async caches.");

            this.cache = cache;
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithAsync()
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

        /** <inheritDoc /> */
        public ICache<K, V> WithSkipStore()
        {
            return cache.WithSkipStore().WrapAsync();
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithExpiryPolicy(IExpiryPolicy plc)
        {
            return cache.WithExpiryPolicy(plc).WrapAsync();
        }

        /** <inheritDoc /> */
        public ICache<K1, V1> WithKeepPortable<K1, V1>()
        {
            return cache.WithKeepPortable<K1, V1>().WrapAsync();
        }
        
        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<K, V> p, params object[] args)
        {
            cache.LoadCache(p, args);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<K, V> p, params object[] args)
        {
            cache.LocalLoadCache(p, args);
            WaitResult();
        }

        /** <inheritDoc /> */
        public bool ContainsKey(K key)
        {
            cache.ContainsKey(key);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<K> keys)
        {
            cache.ContainsKeys(keys);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public V LocalPeek(K key, params CachePeekMode[] modes)
        {
            cache.LocalPeek(key, modes);
            return GetResult<V>();
        }

        /** <inheritDoc /> */
        public V Get(K key)
        {
            cache.Get(key);
            return GetResult<V>();
        }

        /** <inheritDoc /> */
        public IDictionary<K, V> GetAll(IEnumerable<K> keys)
        {
            cache.GetAll(keys);
            return GetResult<IDictionary<K, V>>();
        }

        /** <inheritDoc /> */
        public void Put(K key, V val)
        {
            cache.Put(key, val);
            WaitResult();
        }

        /** <inheritDoc /> */
        public V GetAndPut(K key, V val)
        {
            cache.GetAndPut(key, val);
            return GetResult<V>();
        }

        /** <inheritDoc /> */
        public V GetAndReplace(K key, V val)
        {
            cache.GetAndReplace(key, val);
            return GetResult<V>();
        }

        /** <inheritDoc /> */
        public V GetAndRemove(K key)
        {
            cache.GetAndRemove(key);
            return GetResult<V>();
        }

        /** <inheritDoc /> */
        public bool PutIfAbsent(K key, V val)
        {
            cache.PutIfAbsent(key, val);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public V GetAndPutIfAbsent(K key, V val)
        {
            cache.GetAndPutIfAbsent(key, val);
            return GetResult<V>();
        }

        /** <inheritDoc /> */
        public bool Replace(K key, V val)
        {
            cache.Replace(key, val);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public bool Replace(K key, V oldVal, V newVal)
        {
            cache.Replace(key, oldVal, newVal);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public void PutAll(IDictionary<K, V> vals)
        {
            cache.PutAll(vals);
            WaitResult();
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
            WaitResult();
        }

        /** <inheritDoc /> */
        public void Clear(K key)
        {
            cache.Clear(key);
        }

        /** <inheritDoc /> */
        public void ClearAll(IEnumerable<K> keys)
        {
            cache.ClearAll(keys);
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
            cache.Remove(key);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public bool Remove(K key, V val)
        {
            cache.Remove(key, val);
            return GetResult<bool>();
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<K> keys)
        {
            cache.RemoveAll(keys);
            WaitResult();
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            cache.RemoveAll();
            WaitResult();
        }

        /** <inheritDoc /> */
        public int LocalSize(params CachePeekMode[] modes)
        {
            return cache.LocalSize(modes);
        }

        /** <inheritDoc /> */
        public int Size(params CachePeekMode[] modes)
        {
            cache.Size(modes);
            return GetResult<int>();
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
        IContinuousQueryHandle ICache<K, V>.QueryContinuous(ContinuousQuery<K, V> qry)
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
            cache.Invoke(key, processor, arg);
            
            return GetResult<R>();
        }

        /** <inheritDoc /> */
        public IDictionary<K, ICacheEntryProcessorResult<R>> InvokeAll<R, A>(IEnumerable<K> keys, 
            ICacheEntryProcessor<K, V, A, R> processor, A arg)
        {
            cache.InvokeAll(keys, processor, arg);

            return GetResult<IDictionary<K, ICacheEntryProcessorResult<R>>>();
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
            return cache.WithNoRetries();
        }

        /** <inheritDoc /> */
        public IEnumerator<ICacheEntry<K, V>> GetEnumerator()
        {
            return cache.GetEnumerator();
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
            return cache.GetFuture<T>().Get();
        }
    }

    /// <summary>
    /// Extension methods for IGridCache.
    /// </summary>
    public static class GridCacheExtensions
    {
        /// <summary>
        /// Wraps specified instance into GridCacheTestAsyncWrapper.
        /// </summary>
        public static ICache<K, V> WrapAsync<K, V>(this ICache<K, V> cache)
        {
            return new CacheTestAsyncWrapper<K, V>(cache);
        }
    }
}
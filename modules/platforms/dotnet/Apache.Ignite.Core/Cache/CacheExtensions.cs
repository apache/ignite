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

namespace Apache.Ignite.Core.Cache
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Impl.Cache.Extensions;
    using AC = Apache.Ignite.Core.Impl.Common.IgniteArgumentCheck;

    /// <summary>
    /// <see cref="ICache{TK,TV}"/> extension methods
    /// </summary>
    public static class CacheExtensions
    {
        /// <summary> 
        /// Invokes cache entry processor func against a set of keys. 
        /// If an entry does not exist for the specified key, an attempt is made to load it (if a loader is configured) 
        /// or a surrogate entry, consisting of the key with a null value is used instead.
        ///  
        /// The order that the entries for the keys are processed is undefined.  
        /// Implementations may choose to process the entries in any order, including concurrently. 
        /// Furthermore there is no guarantee implementations will use the same processor instance  
        /// to process each entry, as the case may be in a non-local cache topology. 
        /// </summary>
        /// <param name="cache">Cache instance.</param> 
        /// <typeparam name="K">Key type.</typeparam>
        /// <typeparam name="V">Value type.</typeparam>
        /// <typeparam name="R">The type of the result.</typeparam> 
        /// <typeparam name="A">The type of the argument.</typeparam> 
        /// <param name="keys">The keys.</param> 
        /// <param name="processor">The processor.</param> 
        /// <param name="arg">The argument.</param> 
        /// <returns> 
        /// Map of <see cref="ICacheEntryProcessorResult{R}" /> of the processing per key, if any,  
        /// defined by the <see cref="ICacheEntryProcessor{K,V,A,R}"/> implementation.   
        /// No mappings will be returned for processors that return a null value for a key. 
        /// </returns> 
        /// <exception cref="CacheEntryProcessorException">If an exception has occured during processing.</exception> 
        public static IDictionary<K, ICacheEntryProcessorResult<R>> InvokeAll<K, V, R, A>(this ICache<K, V> cache,
            IEnumerable<K> keys, Func<IMutableCacheEntry<K, V>, A, R> processor, A arg)
        {
            AC.NotNull(cache, "cache");
            AC.NotNull(processor, "processor");

            return cache.InvokeAll(keys, new CacheEntryDelegateProcessor<K, V, A, R>(processor), arg);
        }

        /// <summary> 
        /// Invokes cache entry processor func against the 
        /// <see cref="IMutableCacheEntry{K, V}"/> specified by the provided key. 
        /// If an entry does not exist for the specified key, an attempt is made to load it (if a loader is configured) 
        /// or a surrogate entry, consisting of the key with a null value is used instead.
        /// </summary>
        /// <param name="cache">Cache instance.</param> 
        /// <typeparam name="K">Key type.</typeparam>
        /// <typeparam name="V">Value type.</typeparam>
        /// <typeparam name="R">The type of the result.</typeparam> 
        /// <typeparam name="A">The type of the argument.</typeparam> 
        /// <param name="key">The key.</param> 
        /// <param name="processor">The processor.</param> 
        /// <param name="arg">The argument.</param> 
        /// <returns>Result of the processing.</returns> 
        /// <exception cref="CacheEntryProcessorException">If an exception has occured during processing.</exception> 
        public static R Invoke<K, V, R, A>(this ICache<K, V> cache, K key, 
            Func<IMutableCacheEntry<K, V>, A, R> processor, A arg)
        {
            AC.NotNull(cache, "cache");
            AC.NotNull(processor, "processor");

            return cache.Invoke(key, new CacheEntryDelegateProcessor<K, V, A, R>(processor), arg);
        }

        /// <summary> 
        /// Start continuous query execution. 
        /// </summary>
        /// <param name="cache">Cache instance.</param> 
        /// <param name="localListener">Listener.</param>
        /// <param name="remoteFilter">Optional filter.</param>
        /// <param name="local">Whether query should be executed locally.</param>
        /// <param name="initialQry"> 
        /// The initial query. This query will be executed before continuous listener is registered which allows  
        /// to iterate through entries which have already existed at the time continuous query is executed. 
        /// </param> 
        /// <returns> 
        /// Handle to get initial query cursor or stop query execution. 
        /// </returns> 
        public static IContinuousQueryHandle<ICacheEntry<K, V>> QueryContinuous<K, V>(this ICache<K, V> cache, 
            Action<IEnumerable<ICacheEntryEvent<K, V>>> localListener, Func<ICacheEntryEvent<K, V>, bool> remoteFilter,
            bool local, QueryBase initialQry)  
        {
            AC.NotNull(cache, "cache");
            AC.NotNull(localListener, "localListener");

            var lsnr = new CacheEntryDelegateEventListener<K, V>(localListener);
            var filter = remoteFilter == null ? null : new CacheEntryDelegateEventFilter<K, V>(remoteFilter);

            var qry = new ContinuousQuery<K, V>(lsnr, filter, local);

            return cache.QueryContinuous(qry, initialQry);
        }

        /// <summary> 
        /// Start continuous query execution. 
        /// </summary>
        /// <param name="cache">Cache instance.</param> 
        /// <param name="localListener">Listener.</param>
        /// <param name="remoteFilter">Optional filter.</param>
        /// <param name="local">Whether query should be executed locally.</param>
        /// <returns>Handle to stop query execution.</returns> 
        public static IContinuousQueryHandle QueryContinuous<K, V>(this ICache<K, V> cache,
            Action<IEnumerable<ICacheEntryEvent<K, V>>> localListener, Func<ICacheEntryEvent<K, V>, bool> remoteFilter,
            bool local)
        {
            AC.NotNull(cache, "cache");
            AC.NotNull(localListener, "localListener");

            var lsnr = new CacheEntryDelegateEventListener<K, V>(localListener);
            var filter = remoteFilter == null ? null : new CacheEntryDelegateEventFilter<K, V>(remoteFilter);

            var qry = new ContinuousQuery<K, V>(lsnr, filter, local);

            return cache.QueryContinuous(qry);
        }

        /// <summary> 
        /// Delegates to <see cref="ICacheStore.LoadCache" /> method to load state  
        /// from the underlying persistent storage. The loaded values will then be given  
        /// to the optionally passed in predicate, and, if the predicate returns true,  
        /// will be stored in cache. If predicate is null, then all loaded values will be stored in cache. 
        /// </summary>
        /// <param name="cache">Cache instance.</param> 
        /// <param name="filter"> 
        /// Optional predicate. If provided, will be used to filter values to be put into cache. 
        /// </param> 
        /// <param name="args"> 
        /// Optional user arguments to be passed into <see cref="ICacheStore.LoadCache" />. 
        /// </param> 
        public static void LocalLoadCache<K, V>(this ICache<K, V> cache, Func<ICacheEntry<K, V>, bool> filter, 
            params object[] args)
        {
            AC.NotNull(cache, "cache");

            var filter0 = filter == null ? null : new CacheEntryDelegateFilter<K, V>(filter);

            cache.LocalLoadCache(filter0, args);
        }

        /// <summary> 
        /// Executes LocalLoadCache on all cache nodes. 
        /// </summary>
        /// <param name="cache">Cache instance.</param> 
        /// <param name="filter"> 
        /// Optional predicate. If provided, will be used to filter values to be put into cache. 
        /// </param> 
        /// <param name="args"> 
        /// Optional user arguments to be passed into <see cref="ICacheStore.LoadCache" />. 
        /// </param> 
        public static void LoadCache<K, V>(this ICache<K, V> cache, Func<ICacheEntry<K, V>, bool> filter, 
            params object[] args)
        {
            AC.NotNull(cache, "cache");

            var filter0 = filter == null ? null : new CacheEntryDelegateFilter<K, V>(filter);

            cache.LoadCache(filter0, args);
        }

        /// <summary>
        /// Executes scan query over all cache entries.
        /// </summary>
        /// <typeparam name="K">Key type.</typeparam>
        /// <typeparam name="V">Value type.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <returns>
        /// Query cursor.
        /// </returns>
        public static IQueryCursor<ICacheEntry<K, V>> ScanQuery<K, V>(this ICache<K, V> cache)
        {
            return ScanQuery(cache, null);
        }

        /// <summary>
        /// Executes scan query over cache entries. Will accept all the entries if no predicate was set.
        /// </summary>
        /// <typeparam name="K">Key type.</typeparam>
        /// <typeparam name="V">Value type.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <param name="filter">Optional filter.</param>
        /// <returns>
        /// Query cursor.
        /// </returns>
        public static IQueryCursor<ICacheEntry<K, V>> ScanQuery<K, V>(this ICache<K, V> cache, 
            Func<ICacheEntry<K, V>, bool> filter)
        {
            AC.NotNull(cache, "cache");

            var filter0 = filter == null ? null : new CacheEntryDelegateFilter<K, V>(filter);

            return cache.Query(new ScanQuery<K, V>(filter0));
        }
    }
}

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
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Main entry point for Ignite cache APIs. You can get a named cache by calling
    /// <see cref="IIgnite.Cache{K, V}(string)"/> method.
    /// <para />
    /// Cache API supports distributed transactions. All <c>Get(...)</c>, <c>Put(...)</c>, <c>Replace(...)</c>,
    /// and <c>Remove(...)</c> operations are transactional and will participate in an ongoing transaction,
    /// if any. Other methods like <c>Peek(...)</c> or various <c>Contains(...)</c> methods may
    /// be transaction-aware, i.e. check in-transaction entries first, but will not affect the current
    /// state of transaction. See <see cref="ITransaction"/> documentation for more information
    /// about transactions.
    /// <para />
    /// Neither <c>null</c> keys or values are allowed to be stored in cache. If a <c>null</c> value
    /// happens to be in cache (e.g. after invalidation or remove), then cache will treat this case
    /// as there is no value at all.
    /// <para />
    /// Note that cache is generic and you can only work with provided key and value types. If cache also
    /// contains keys or values of other types, any attempt to retrieve them will result in
    /// <see cref="InvalidCastException"/>. Use <see cref="ICache{Object, Object}"/> in order to work with entries
    /// of arbitrary types.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    public interface ICache<TK, TV> : IAsyncSupport<ICache<TK, TV>>, IEnumerable<ICacheEntry<TK, TV>>
    {
        /// <summary>
        /// Name of this cache (<c>null</c> for default cache).
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Ignite hosting this cache.
        /// </summary>
        IIgnite Ignite { get; }

        /// <summary>
        /// Checks whether this cache contains no key-value mappings.
        /// <para />
        /// Semantically equals to <c>ICache.Size(CachePeekMode.PRIMARY) == 0</c>.
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// Gets a value indicating whether to keep values in portable form.
        /// </summary>
        bool KeepPortable { get; }

        /// <summary>
        /// Get another cache instance with read-through and write-through behavior disabled.
        /// </summary>
        /// <returns>Cache with read-through and write-through behavior disabled.</returns>
        ICache<TK, TV> WithSkipStore();

        /// <summary>
        /// Returns cache with the specified expired policy set. This policy will be used for each operation
        /// invoked on the returned cache.
        /// <para />
        /// Expiry durations for each operation are calculated only once and then used as constants. Please
        /// consider this when implementing customg expiry policy implementations.
        /// </summary>
        /// <param name="plc">Expiry policy to use.</param>
        /// <returns>Cache instance with the specified expiry policy set.</returns>
        ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc);

        /// <summary>
        /// Gets cache with KeepPortable mode enabled, changing key and/or value types if necessary.
        /// You can only change key/value types when transitioning from non-portable to portable cache;
        /// Changing type of portable cache is not allowed and will throw an <see cref="InvalidOperationException"/>
        /// </summary>
        /// <typeparam name="TK1">Key type in portable mode.</typeparam>
        /// <typeparam name="TV1">Value type in protable mode.</typeparam>
        /// <returns>Cache instance with portable mode enabled.</returns>
        ICache<TK1, TV1> WithKeepPortable<TK1, TV1>();

        /// <summary>
        /// Executes <see cref="LocalLoadCache"/> on all cache nodes.
        /// </summary>
        /// <param name="p">
        /// Optional predicate. If provided, will be used to filter values to be put into cache.
        /// </param>
        /// <param name="args">
        /// Optional user arguments to be passed into <see cref="ICacheStore.LoadCache" />.
        /// </param>
        [AsyncSupported]
        void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args);

        /// <summary>
        /// Delegates to <see cref="ICacheStore.LoadCache" /> method to load state 
        /// from the underlying persistent storage. The loaded values will then be given 
        /// to the optionally passed in predicate, and, if the predicate returns true, 
        /// will be stored in cache. If predicate is null, then all loaded values will be stored in cache.
        /// </summary>
        /// <param name="p">
        /// Optional predicate. If provided, will be used to filter values to be put into cache.
        /// </param>
        /// <param name="args">
        /// Optional user arguments to be passed into <see cref="ICacheStore.LoadCache" />.
        /// </param>
        [AsyncSupported]
        void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args);

        /// <summary>
        /// Check if cache contains mapping for this key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>True if cache contains mapping for this key.</returns>
        [AsyncSupported]
        bool ContainsKey(TK key);

        /// <summary>
        /// Check if cache contains mapping for these keys.
        /// </summary>
        /// <param name="keys">Keys.</param>
        /// <returns>True if cache contains mapping for all these keys.</returns>
        [AsyncSupported]
        bool ContainsKeys(IEnumerable<TK> keys);

        /// <summary>
        /// Peeks at cached value using optional set of peek modes. This method will sequentially
        /// iterate over given peek modes, and try to peek at value using each peek mode. Once a
        /// non-null value is found, it will be immediately returned.
        /// This method does not participate in any transactions, however, it may peek at transactional
        /// value depending on the peek modes used.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="modes">Peek modes.</param>
        /// <returns>Peeked value.</returns>
        TV LocalPeek(TK key, params CachePeekMode[] modes);

        /// <summary>
        /// Retrieves value mapped to the specified key from cache.
        /// If the value is not present in cache, then it will be looked up from swap storage. If
        /// it's not present in swap, or if swap is disable, and if read-through is allowed, value
        /// will be loaded from persistent store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Value.</returns>
        [AsyncSupported]
        TV Get(TK key);

        /// <summary>
        /// Retrieves values mapped to the specified keys from cache.
        /// If some value is not present in cache, then it will be looked up from swap storage. If
        /// it's not present in swap, or if swap is disabled, and if read-through is allowed, value
        /// will be loaded from persistent store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="keys">Keys.</param>
        /// <returns>Map of key-value pairs.</returns>
        [AsyncSupported]
        IDictionary<TK, TV> GetAll(IEnumerable<TK> keys);

        /// <summary>
        /// Associates the specified value with the specified key in the cache.
        /// <para />
        /// If the cache previously contained a mapping for the key, 
        /// the old value is replaced by the specified value.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        [AsyncSupported]
        void Put(TK key, TV val);

        /// <summary>
        /// Associates the specified value with the specified key in this cache,
        /// returning an existing value if one existed.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        /// <returns>
        /// The value associated with the key at the start of the operation or null if none was associated.
        /// </returns>
        [AsyncSupported]
        TV GetAndPut(TK key, TV val);
        
        /// <summary>
        /// Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        /// <returns>
        /// The previous value associated with the specified key, or null if there was no mapping for the key.
        /// </returns>
        [AsyncSupported]
        TV GetAndReplace(TK key, TV val);

        /// <summary>
        /// Atomically removes the entry for a key only if currently mapped to some value.
        /// </summary>
        /// <param name="key">Key with which the specified value is associated.</param>
        /// <returns>The value if one existed or null if no mapping existed for this key.</returns>
        [AsyncSupported]
        TV GetAndRemove(TK key);

        /// <summary>
        /// Atomically associates the specified key with the given value if it is not already associated with a value.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        /// <returns>True if a value was set.</returns>
        [AsyncSupported]
        bool PutIfAbsent(TK key, TV val);

        /// <summary>
        /// Stores given key-value pair in cache only if cache had no previous mapping for it.
        /// If cache previously contained value for the given key, then this value is returned.
        /// In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node,
        /// which in its turn may load the value from the swap storage, and consecutively, if it's not
        /// in swap, from the underlying persistent storage.
        /// If the returned value is not needed, method putxIfAbsent() should be used instead of this one to
        /// avoid the overhead associated with returning of the previous value.
        /// If write-through is enabled, the stored value will be persisted to store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="key">Key to store in cache.</param>
        /// <param name="val">Value to be associated with the given key.</param>
        /// <returns>
        /// Previously contained value regardless of whether put happened or not (null if there was no previous value).
        /// </returns>
        [AsyncSupported]
        TV GetAndPutIfAbsent(TK key, TV val);

        /// <summary>
        /// Stores given key-value pair in cache only if there is a previous mapping for it.
        /// If cache previously contained value for the given key, then this value is returned.
        /// In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node,
        /// which in its turn may load the value from the swap storage, and consecutively, if it's not
        /// in swap, rom the underlying persistent storage.
        /// If write-through is enabled, the stored value will be persisted to store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="key">Key to store in cache.</param>
        /// <param name="val">Value to be associated with the given key.</param>
        /// <returns>True if the value was replaced.</returns>
        [AsyncSupported]
        bool Replace(TK key, TV val);

        /// <summary>
        /// Stores given key-value pair in cache only if only if the previous value is equal to the
        /// old value passed as argument.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="key">Key to store in cache.</param>
        /// <param name="oldVal">Old value to match.</param>
        /// <param name="newVal">Value to be associated with the given key.</param>
        /// <returns>True if replace happened, false otherwise.</returns>
        [AsyncSupported]
        bool Replace(TK key, TV oldVal, TV newVal);

        /// <summary>
        /// Stores given key-value pairs in cache.
        /// If write-through is enabled, the stored values will be persisted to store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="vals">Key-value pairs to store in cache.</param>
        [AsyncSupported]
        void PutAll(IDictionary<TK, TV> vals);

        /// <summary>
        /// Attempts to evict all entries associated with keys. Note, that entry will be evicted only 
        /// if it's not used (not participating in any locks or transactions).
        /// </summary>
        /// <param name="keys">Keys to evict from cache.</param>
        void LocalEvict(IEnumerable<TK> keys);

        /// <summary>
        /// Clears the contents of the cache, without notifying listeners or CacheWriters.
        /// </summary>
        [AsyncSupported]
        void Clear();

        /// <summary>
        /// Clear entry from the cache and swap storage, without notifying listeners or CacheWriters.
        /// Entry is cleared only if it is not currently locked, and is not participating in a transaction.
        /// </summary>
        /// <param name="key">Key to clear.</param>
        [AsyncSupported]
        void Clear(TK key);

        /// <summary>
        /// Clear entries from the cache and swap storage, without notifying listeners or CacheWriters.
        /// Entry is cleared only if it is not currently locked, and is not participating in a transaction.
        /// </summary>
        /// <param name="keys">Keys to clear.</param>
        [AsyncSupported]
        void ClearAll(IEnumerable<TK> keys);

        /// <summary>
        /// Clear entry from the cache and swap storage, without notifying listeners or CacheWriters.
        /// Entry is cleared only if it is not currently locked, and is not participating in a transaction.
        /// <para />
        /// Note that this operation is local as it merely clears
        /// an entry from local cache, it does not remove entries from remote caches.
        /// </summary>
        /// <param name="key">Key to clear.</param>
        void LocalClear(TK key);

        /// <summary>
        /// Clear entries from the cache and swap storage, without notifying listeners or CacheWriters.
        /// Entry is cleared only if it is not currently locked, and is not participating in a transaction.
        /// <para />
        /// Note that this operation is local as it merely clears
        /// entries from local cache, it does not remove entries from remote caches.
        /// </summary>
        /// <param name="keys">Keys to clear.</param>
        void LocalClearAll(IEnumerable<TK> keys);

        /// <summary>
        /// Removes given key mapping from cache. If cache previously contained value for the given key,
        /// then this value is returned. In case of PARTITIONED or REPLICATED caches, the value will be
        /// loaded from the primary node, which in its turn may load the value from the disk-based swap
        /// storage, and consecutively, if it's not in swap, from the underlying persistent storage.
        /// If the returned value is not needed, method removex() should always be used instead of this
        /// one to avoid the overhead associated with returning of the previous value.
        /// If write-through is enabled, the value will be removed from store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="key">Key whose mapping is to be removed from cache.</param>
        /// <returns>False if there was no matching key.</returns>
        [AsyncSupported]
        bool Remove(TK key);

        /// <summary>
        /// Removes given key mapping from cache if one exists and value is equal to the passed in value.
        /// If write-through is enabled, the value will be removed from store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="key">Key whose mapping is to be removed from cache.</param>
        /// <param name="val">Value to match against currently cached value.</param>
        /// <returns>True if entry was removed, false otherwise.</returns>
        [AsyncSupported]
        bool Remove(TK key, TV val);

        /// <summary>
        /// Removes given key mappings from cache.
        /// If write-through is enabled, the value will be removed from store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        /// <param name="keys">Keys whose mappings are to be removed from cache.</param>
        [AsyncSupported]
        void RemoveAll(IEnumerable<TK> keys);

        /// <summary>
        /// Removes all mappings from cache.
        /// If write-through is enabled, the value will be removed from store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// </summary>
        [AsyncSupported]
        void RemoveAll();

        /// <summary>
        /// Gets the number of all entries cached on this node.
        /// </summary>
        /// <param name="modes">Optional peek modes. If not provided, then total cache size is returned.</param>
        /// <returns>Cache size on this node.</returns>
        int LocalSize(params CachePeekMode[] modes);

        /// <summary>
        /// Gets the number of all entries cached across all nodes.
        /// <para />
        /// NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
        /// </summary>
        /// <param name="modes">Optional peek modes. If not provided, then total cache size is returned.</param>
        /// <returns>Cache size across all nodes.</returns>
        [AsyncSupported]
        int Size(params CachePeekMode[] modes);

        /// <summary>
        /// This method unswaps cache entries by given keys, if any, from swap storage into memory.
        /// </summary>
        /// <param name="keys">Keys to promote entries for.</param>
        void LocalPromote(IEnumerable<TK> keys);
        
        /// <summary>
        /// Queries cache.
        /// </summary>
        /// <param name="qry">Query.</param>
        /// <returns>Cursor.</returns>
        IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry);

        /// <summary>
        /// Queries separate entry fields.
        /// </summary>
        /// <param name="qry">SQL fields query.</param>
        /// <returns>Cursor.</returns>
        IQueryCursor<IList> QueryFields(SqlFieldsQuery qry);

        /// <summary>
        /// Start continuous query execution.
        /// </summary>
        /// <param name="qry">Continuous query.</param>
        /// <returns>Handle to stop query execution.</returns>
        IContinuousQueryHandle QueryContinuous(ContinuousQuery<TK, TV> qry);

        /// <summary>
        /// Start continuous query execution.
        /// </summary>
        /// <param name="qry">Continuous query.</param>
        /// <param name="initialQry">
        /// The initial query. This query will be executed before continuous listener is registered which allows 
        /// to iterate through entries which have already existed at the time continuous query is executed.
        /// </param>
        /// <returns>
        /// Handle to get initial query cursor or stop query execution.
        /// </returns>
        IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuous(ContinuousQuery<TK, TV> qry, QueryBase initialQry);
        
        /// <summary>
        /// Get local cache entries.
        /// </summary>
        /// <param name="peekModes">Peek modes.</param>
        /// <returns>Enumerable instance.</returns>
        IEnumerable<ICacheEntry<TK, TV>> GetLocalEntries(params CachePeekMode[] peekModes);

        /// <summary>
        /// Invokes an <see cref="ICacheEntryProcessor{K, V, A, R}"/> against the 
        /// <see cref="IMutableCacheEntry{K, V}"/> specified by the provided key. 
        /// If an entry does not exist for the specified key, an attempt is made to load it (if a loader is configured) 
        /// or a surrogate entry, consisting of the key with a null value is used instead.
        /// </summary>
        /// <typeparam name="TR">The type of the result.</typeparam>
        /// <typeparam name="TA">The type of the argument.</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="processor">The processor.</param>
        /// <param name="arg">The argument.</param>
        /// <returns>Result of the processing.</returns>
        /// <exception cref="CacheEntryProcessorException">If an exception has occured during processing.</exception>
        [AsyncSupported]
        TR Invoke<TR, TA>(TK key, ICacheEntryProcessor<TK, TV, TA, TR> processor, TA arg);

        /// <summary>
        /// Invokes an <see cref="ICacheEntryProcessor{K, V, A, R}"/> against a set of keys.
        /// If an entry does not exist for the specified key, an attempt is made to load it (if a loader is configured) 
        /// or a surrogate entry, consisting of the key with a null value is used instead.
        /// 
        /// The order that the entries for the keys are processed is undefined. 
        /// Implementations may choose to process the entries in any order, including concurrently.
        /// Furthermore there is no guarantee implementations will use the same processor instance 
        /// to process each entry, as the case may be in a non-local cache topology.
        /// </summary>
        /// <typeparam name="TR">The type of the result.</typeparam>
        /// <typeparam name="TA">The type of the argument.</typeparam>
        /// <param name="keys">The keys.</param>
        /// <param name="processor">The processor.</param>
        /// <param name="arg">The argument.</param>
        /// <returns>
        /// Map of <see cref="ICacheEntryProcessorResult{R}" /> of the processing per key, if any, 
        /// defined by the <see cref="ICacheEntryProcessor{K,V,A,R}"/> implementation.  
        /// No mappings will be returned for processors that return a null value for a key.
        /// </returns>
        /// <exception cref="CacheEntryProcessorException">If an exception has occured during processing.</exception>
        [AsyncSupported]
        IDictionary<TK, ICacheEntryProcessorResult<TR>> InvokeAll<TR, TA>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TA, TR> processor, TA arg);

        /// <summary>
        /// Creates an <see cref="ICacheLock"/> instance associated with passed key.
        /// This method does not acquire lock immediately, you have to call appropriate method on returned instance.
        /// </summary>
        /// <param name="key">Key for lock.</param>
        /// <returns>New <see cref="ICacheLock"/> instance associated with passed key.</returns>
        ICacheLock Lock(TK key);

        /// <summary>
        /// Creates an <see cref="ICacheLock"/> instance associated with passed keys.
        /// This method does not acquire lock immediately, you have to call appropriate method on returned instance.
        /// </summary>
        /// <param name="keys">Keys for lock.</param>
        /// <returns>New <see cref="ICacheLock"/> instance associated with passed keys.</returns>
        ICacheLock LockAll(IEnumerable<TK> keys);

        /// <summary>
        /// Checks if specified key is locked.
        /// <para />
        /// This is a local operation and does not involve any network trips
        /// or access to persistent storage in any way.
        /// </summary>
        /// <param name="key">Key to check.</param>
        /// <param name="byCurrentThread">
        /// If true, checks that current thread owns a lock on this key; 
        /// otherwise, checks that any thread on any node owns a lock on this key.
        /// </param>
        /// <returns>True if specified key is locked; otherwise, false.</returns>
        bool IsLocalLocked(TK key, bool byCurrentThread);

        /// <summary>
        /// Gets snapshot metrics (statistics) for this cache.
        /// </summary>
        /// <returns>Cache metrics.</returns>
        ICacheMetrics GetMetrics();

        /// <summary>
        /// Rebalances cache partitions. This method is usually used when rebalanceDelay configuration parameter 
        /// has non-zero value. When many nodes are started or stopped almost concurrently, 
        /// it is more efficient to delay rebalancing until the node topology is stable to make sure that no redundant 
        /// re-partitioning happens.
        /// <para />
        /// In case of partitioned caches, for better efficiency user should usually make sure that new nodes get 
        /// placed on the same place of consistent hash ring as the left nodes, and that nodes are restarted before
        /// rebalanceDelay expires.
        /// </summary>
        /// <returns>Future that will be completed when rebalancing is finished.</returns>
        IFuture Rebalance();

        /// <summary>
        /// Get another cache instance with no-retries behavior enabled.
        /// </summary>
        /// <returns>Cache with no-retries behavior enabled.</returns>
        ICache<TK, TV> WithNoRetries();
    }
}

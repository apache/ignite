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

namespace Apache.Ignite.Core.Configuration
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Defines grid cache configuration.
    /// </summary>
    public class CacheConfiguration
    {
        /// <summary>
        /// Gets or sets the cache name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        public CacheConfiguration(string name = null)
        {
            Name = name;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal CacheConfiguration(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
        }

        ///
        /// Gets cache eviction policy. By default, returns { null}        which means that evictions are disabled for cache.
        ///
        //public EvictionPolicy EvictionPolicy { get; set; }

        ///
        /// 
        ///
        //public NearCacheConfiguration NearConfiguration { get; set; }

        ///
        /// Gets write synchronization mode. This mode controls whether the main        caller should wait for update on other nodes to complete or not.
        ///
        public CacheWriteSynchronizationMode WriteSynchronizationMode { get; set; }

        ///
        /// Gets filter which determines on what nodes the cache should be started.
        ///
        //public IgnitePredicate<ClusterNode> NodeFilter { get; set; }

        ///
        /// Gets flag indicating whether eviction is synchronized between primary, backup and near nodes.        If this parameter is { true} and swap is disabled then { IgniteCache#localEvict(Collection)}        will involve all nodes where an entry is kept.  If this property is set to { false} then        eviction is done independently on different cache nodes.        <p>        Default value is defined by { #DFLT_EVICT_SYNCHRONIZED}.        <p>        Note that it's not recommended to set this value to { true} if cache        store is configured since it will allow to significantly improve cache        performance.
        ///
        public bool EvictSynchronized { get; set; }

        ///
        /// Gets size of the key buffer for synchronized evictions.        <p>        Default value is defined by { #DFLT_EVICT_KEY_BUFFER_SIZE}.
        ///
        public int EvictSynchronizedKeyBufferSize { get; set; }

        ///
        /// Gets concurrency level for synchronized evictions. This flag only makes sense        with { #isEvictSynchronized()} set        to { true}. When synchronized evictions are enabled, it is possible that        local eviction policy will try to evict entries faster than evictions can be        synchronized with backup or near nodes. This value specifies how many concurrent        synchronous eviction sessions should be allowed before the system is forced to        wait and let synchronous evictions catch up with the eviction policy.        <p>        Note that if synchronous evictions start lagging, it is possible that you have either        too big or too small eviction key buffer size or small eviction timeout. In that case        you will need to adjust { #getEvictSynchronizedKeyBufferSize} or        { #getEvictSynchronizedTimeout()} values as well.        <p>        Default value is defined by { #DFLT_EVICT_SYNCHRONIZED_CONCURRENCY_LEVEL}.
        ///
        public int EvictSynchronizedConcurrencyLevel { get; set; }

        ///
        /// Gets timeout for synchronized evictions.        <p>        Node that initiates eviction waits for responses        from remote nodes within this timeout.        <p>        Default value is defined by { #DFLT_EVICT_SYNCHRONIZED_TIMEOUT}.
        ///
        public long EvictSynchronizedTimeout { get; set; }

        ///
        /// This value denotes the maximum size of eviction queue in percents of cache        size in case of distributed cache (replicated and partitioned) and using        synchronized eviction (that is if { #isEvictSynchronized()} returns        { true}).        <p>        That queue is used internally as a buffer to decrease network costs for        synchronized eviction. Once queue size reaches specified value all required        requests for all entries in the queue are sent to remote nodes and the queue        is cleared.        <p>        Default value is defined by { #DFLT_MAX_EVICTION_OVERFLOW_RATIO} and        equals to { 10%}.
        ///
        public float EvictMaxOverflowRatio { get; set; }

        ///
        /// Gets eviction filter to specify which entries should not be evicted        (except explicit evict by calling { IgniteCache#localEvict(Collection)}).        If { EvictionFilter#evictAllowed(Cache.Entry)} method        returns { false} then eviction policy will not be notified and entry will        never be evicted.        <p>        If not provided, any entry may be evicted depending on        { #getEvictionPolicy() eviction policy} configuration.
        ///
        //public EvictionFilter<K, EvictionFilter { get; set; }

        ///
        /// Gets flag indicating whether expired cache entries will be eagerly removed from cache. When        set to { false}, expired entries will be removed on next entry access.        <p>        When not set, default value is { #DFLT_EAGER_TTL}.        <p>        <b>Note</b> that this flag only matters for entries expiring based on        { ExpiryPolicy} and should not be confused with entry        evictions based on configured { EvictionPolicy}.
        ///
        public bool EagerTtl { get; set; }

        ///
        /// Gets initial cache size which will be used to pre-create internal        hash table after start. Default value is defined by { #DFLT_START_SIZE}.
        ///
        public int StartSize { get; set; }

        ///
        /// Gets flag indicating whether value should be loaded from store if it is not in the cache        for following cache operations:        <ul>            <li>{ IgniteCache#putIfAbsent(Object, Object)}</li>            <li>{ IgniteCache#replace(Object, Object)}</li>            <li>{ IgniteCache#replace(Object, Object, Object)}</li>            <li>{ IgniteCache#remove(Object, Object)}</li>            <li>{ IgniteCache#getAndPut(Object, Object)}</li>            <li>{ IgniteCache#getAndRemove(Object)}</li>            <li>{ IgniteCache#getAndReplace(Object, Object)}</li>            <li>{ IgniteCache#getAndPutIfAbsent(Object, Object)}</li>       </ul>
        ///
        public bool LoadPreviousValue { get; set; }

        ///
        /// Gets factory for underlying persistent storage for read-through and write-through operations.
        ///
        public bool KeepPortableInStore { get; set; }

        ///
        /// Gets key topology resolver to provide mapping from keys to nodes.
        ///
//public AffinityFunction Affinity { get; set; }

        ///
        /// Gets caching mode to use. You can configure cache either to be local-only,        fully replicated, partitioned, or near. If not provided, { CacheMode#PARTITIONED}        mode will be used by default (defined by { #DFLT_CACHE_MODE} constant).
        ///
        public CacheMode CacheMode { get; set; }

        ///
        /// Gets cache atomicity mode.        <p>        Default value is defined by { #DFLT_CACHE_ATOMICITY_MODE}.
        ///
        public CacheAtomicityMode AtomicityMode { get; set; }

        ///
        /// Gets cache write ordering mode. This property can be enabled only for { CacheAtomicityMode#ATOMIC}        cache (for other atomicity modes it will be ignored).
        ///
        public CacheAtomicWriteOrderMode AtomicWriteOrderMode { get; set; }

        ///
        /// Gets number of nodes used to back up single partition for { CacheMode#PARTITIONED} cache.        <p>        If not set, default value is { #DFLT_BACKUPS}.
        ///
        public int Backups { get; set; }

        ///
        /// Gets default lock acquisition timeout. Default value is defined by { #DFLT_LOCK_TIMEOUT}        which is { 0} and means that lock acquisition will never timeout.
        ///
        public long DefaultLockTimeout { get; set; }

        ///
        /// Invalidation flag. If { true}, values will be invalidated (nullified) upon commit in near cache.
        ///
        public bool Invalidate { get; set; }

        ///
        /// Gets class name of transaction manager finder for integration for JEE app servers.
        ///
        public CacheRebalanceMode RebalanceMode { get; set; }

        ///
        /// Gets rebalance mode for distributed cache.        <p>        Default is defined by { #DFLT_REBALANCE_MODE}.
        ///
        public int RebalanceOrder { get; set; }

        ///
        /// Gets size (in number bytes) to be loaded within a single rebalance message.        Rebalancing algorithm will split total data set on every node into multiple        batches prior to sending data. Default value is defined by        { #DFLT_REBALANCE_BATCH_SIZE}.
        ///
        public int RebalanceBatchSize { get; set; }

        ///
        /// Flag indicating whether Ignite should use swap storage by default. By default        swap is disabled which is defined via { #DFLT_SWAP_ENABLED} constant.
        ///
        public bool SwapEnabled { get; set; }

        ///
        /// Gets maximum number of allowed concurrent asynchronous operations. If 0 returned then number        of concurrent asynchronous operations is unlimited.        <p>        If not set, default value is { #DFLT_MAX_CONCURRENT_ASYNC_OPS}.        <p>        If user threads do not wait for asynchronous operations to complete, it is possible to overload        a system. This property enables back-pressure control by limiting number of scheduled asynchronous        cache operations.
        ///
        public int MaxConcurrentAsyncOperations { get; set; }

        ///
        /// Flag indicating whether Ignite should use write-behind behaviour for the cache store.        By default write-behind is disabled which is defined via { #DFLT_WRITE_BEHIND_ENABLED}        constant.
        ///
        public bool WriteBehindEnabled { get; set; }

        ///
        /// Maximum size of the write-behind cache. If cache size exceeds this value,        all cached items are flushed to the cache store and write cache is cleared.        <p/>        If not provided, default value is { #DFLT_WRITE_BEHIND_FLUSH_SIZE}.        If this value is { 0}, then flush is performed according to the flush frequency interval.        <p/>        Note that you cannot set both, { flush} size and { flush frequency}, to { 0}.
        ///
        public int WriteBehindFlushSize { get; set; }

        ///
        /// Frequency with which write-behind cache is flushed to the cache store in milliseconds.        This value defines the maximum time interval between object insertion/deletion from the cache        ant the moment when corresponding operation is applied to the cache store.        <p>        If not provided, default value is { #DFLT_WRITE_BEHIND_FLUSH_FREQUENCY}.        If this value is { 0}, then flush is performed according to the flush size.        <p>        Note that you cannot set both, { flush} size and { flush frequency}, to { 0}.
        ///
        public long WriteBehindFlushFrequency { get; set; }

        ///
        /// Number of threads that will perform cache flushing. Cache flushing is performed        when cache size exceeds value defined by        { #getWriteBehindFlushSize}, or flush interval defined by        { #getWriteBehindFlushFrequency} is elapsed.        <p/>        If not provided, default value is { #DFLT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT}.
        ///
        public int WriteBehindFlushThreadCount { get; set; }

        ///
        /// Maximum batch size for write-behind cache store operations. Store operations (get or remove)        are combined in a batch of this size to be passed to        { CacheStore#writeAll(Collection)} or        { CacheStore#deleteAll(Collection)} methods.        <p/>        If not provided, default value is { #DFLT_WRITE_BEHIND_BATCH_SIZE}.
        ///
        public int WriteBehindBatchSize { get; set; }

        ///
        /// Gets size of rebalancing thread pool. Note that size serves as a hint and implementation        may create more threads for rebalancing than specified here (but never less threads).        <p>        Default value is { #DFLT_REBALANCE_THREAD_POOL_SIZE}.
        ///
        public int RebalanceThreadPoolSize { get; set; }

        ///
        /// Gets rebalance timeout (ms).        <p>        Default value is { #DFLT_REBALANCE_TIMEOUT}.
        ///
        public long RebalanceTimeout { get; set; }

        ///
        /// Gets delay in milliseconds upon a node joining or leaving topology (or crash) after which rebalancing        should be started automatically. Rebalancing should be delayed if you plan to restart nodes        after they leave topology, or if you plan to start multiple nodes at once or one after another        and don't want to repartition and rebalance until all nodes are started.        <p>        For better efficiency user should usually make sure that new nodes get placed on        the same place of consistent hash ring as the left nodes, and that nodes are        restarted before this delay expires. To place nodes on the same place in consistent hash ring,        use { IgniteConfiguration#setConsistentId(Serializable)}        to make sure that a node maps to the same hash ID event if restarted. As an example,        node IP address and port combination may be used in this case.        <p>        Default value is { 0} which means that repartitioning and rebalancing will start        immediately upon node leaving topology. If { -1} is returned, then rebalancing        will only be started manually by calling { IgniteCache#rebalance()} method or        from management console.
        ///
        public long RebalanceDelay { get; set; }

        ///
        /// Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.        When rebalancing large data sets, the CPU or network can get over-consumed with rebalancing messages,        which consecutively may slow down the application performance. This parameter helps tune        the amount of time to wait between rebalance messages to make sure that rebalancing process        does not have any negative performance impact. Note that application will continue to work        properly while rebalancing is still in progress.        <p>        Value of { 0} means that throttling is disabled. By default throttling is disabled -        the default is defined by { #DFLT_REBALANCE_THROTTLE} constant.
        ///
        public long RebalanceThrottle { get; set; }

        ///
        /// Affinity key mapper used to provide custom affinity key for any given key.        Affinity mapper is particularly useful when several objects need to be collocated        on the same node (they will also be backed up on the same nodes as well).        <p>        If not provided, then default implementation will be used. The default behavior        is described in { AffinityKeyMapper} documentation.
        ///
        //public AffinityKeyMapper AffinityMapper { get; set; }

        ///
        /// Gets maximum amount of memory available to off-heap storage. Possible values are        <ul>        <li>{ -1} - Means that off-heap storage is disabled.</li>        <li>            { 0} - Ignite will not limit off-heap storage (it's up to user to properly            add and remove entries from cache to ensure that off-heap storage does not grow            indefinitely.        </li>        <li>Any positive value specifies the limit of off-heap storage in bytes.</li>        </ul>        Default value is { -1}, specified by { #DFLT_OFFHEAP_MEMORY} constant        which means that off-heap storage is disabled by default.        <p>        Use off-heap storage to load gigabytes of data in memory without slowing down        Garbage Collection. Essentially in this case you should allocate very small amount        of memory to JVM and Ignite will cache most of the data in off-heap space        without affecting JVM performance at all.        <p>        Note that Ignite will throw an exception if max memory is set to { -1} and        { offHeapValuesOnly} flag is set to { true}.
        ///
        public long OffHeapMaxMemory { get; set; }

        ///
        /// Gets memory mode for cache. Memory mode helps control whether value is stored in on-heap memory,        off-heap memory, or swap space. Refer to { CacheMemoryMode} for more info.        <p>        Default value is { #DFLT_MEMORY_MODE}.
        ///
        public CacheMemoryMode MemoryMode { get; set; }

        ///
        /// Gets cache interceptor.
        ///
        //public CacheInterceptor Interceptor { get; set; }

        ///
        /// Gets collection of type metadata objects.
        ///
        //public ICollection<CacheTypeMetadata> TypeMetadata { get; set; }

        ///
        /// Gets flag indicating whether data can be read from backup.        If { false} always get data from primary node (never from backup).        <p>        Default value is defined by { #DFLT_READ_FROM_BACKUP}.
        ///
        public bool ReadFromBackup { get; set; }

        ///
        /// Gets flag indicating whether copy of of the value stored in cache should be created        for cache operation implying return value. Also if this flag is set copies are created for values        passed to { CacheInterceptor} and to { CacheEntryProcessor}.
        ///
        public bool CopyOnRead { get; set; }

        ///
        /// Gets classes with methods annotated by { QuerySqlFunction}        to be used as user-defined functions from SQL queries.
        ///
        public long LongQueryWarningTimeout { get; set; }

        ///
        /// If { true} all the SQL table and field names will be escaped with double quotes like        ({ "tableName"."fieldsName"}). This enforces case sensitivity for field names and        also allows having special characters in table and field names.
        ///
        public bool SqlEscapeAll { get; set; }

        ///
        /// Array of key and value type pairs to be indexed (thus array length must be always even).        It means each even (0,2,4...) class in the array will be considered as key type for cache entry,        each odd (1,3,5...) class will be considered as value type for cache entry.        <p>        The same key class can occur multiple times for different value classes, but each value class must be unique        because SQL table will be named as value class simple name.        <p>        To expose fields of these types onto SQL level and to index them you have to use annotations        from package { org.apache.ignite.cache.query.annotations}.
        ///
//public Class<?>...IndexedTypes { get; set; }

        ///
        /// Number of SQL rows which will be cached onheap to avoid deserialization on each SQL index access.        This setting only makes sense when offheap is enabled for this cache.
        ///
        public int SqlOnheapRowCacheSize { get; set; }

        ///
        /// Gets a collection of configured  query entities.
        ///
//public Collection<QueryEntity> QueryEntities { get; set; }



    }
}

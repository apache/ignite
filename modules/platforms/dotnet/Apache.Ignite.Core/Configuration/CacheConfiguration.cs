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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Defines grid cache configuration.
    /// </summary>
    public class CacheConfiguration
    {
        /** Default size of rebalance thread pool. */
        public static int DefaultRebalanceThreadPoolSize = 2;

        /** Default rebalance timeout (ms).*/
        public static long DefaultRebalanceTimeout = 10000;

        /** Time in milliseconds to wait between rebalance messages to avoid overloading CPU. */
        public static long DefaultRebalanceThrottle = 0;

        /** Default number of backups. */
        public static int DefaultBackups = 0;

        /** Default caching mode. */
        public static CacheMode DefaultCacheMode = CacheMode.Partitioned;

        /** Default atomicity mode. */
        public static CacheAtomicityMode DefaultCacheAtomicityMode = CacheAtomicityMode.Atomic;

        /** Default lock timeout. */
        public static long DefaultLockTimeout = 0;

        /** Initial default cache size. */
        public static int DefaultStartSize = 1500000;

        /** Default cache size to use with eviction policy. */
        public static int DefaultCacheSize = 100000;

        /** Initial default near cache size. */
        public static int DefaultNearStartSize = DefaultStartSize/4;

        /** Default value for 'invalidate' flag that indicates if this is invalidation-based cache. */
        public static bool DefaultInvalidate = false;

        /** Default rebalance mode for distributed cache. */
        public static CacheRebalanceMode DefaultRebalanceMode = CacheRebalanceMode.Async;

        /** Default rebalance batch size in bytes. */
        public static int DefaultRebalanceBatchSize = 512*1024; // 512K

        /** Default maximum eviction queue ratio. */
        public static float DefaultMaxEvictionOverflowRatio = 10;

        /** Default eviction synchronized flag. */
        public static bool DefaultEvictSynchronized = false;

        /** Default eviction key buffer size for batching synchronized evicts. */
        public static int DefaultEvictKeyBufferSize = 1024;

        /** Default synchronous eviction timeout in milliseconds. */
        public static long DefaultEvictSynchronizedTimeout = 10000;

        /** Default synchronous eviction concurrency level. */
        public static int DefaultEvictSynchronizedConcurrencyLevel = 4;

        /** Default value for eager ttl flag. */
        public static bool DefaultEagerTtl = true;

        /** Default off-heap storage size is {@code -1} which means that off-heap storage is disabled. */
        public static long DefaultOffheapMemory = -1;

        /** Default value for 'swapEnabled' flag. */
        public static bool DefaultSwapEnabled = false;

        /** Default value for 'maxConcurrentAsyncOps'. */
        public static int DefaultMaxConcurrentAsyncOps = 500;

        /** Default value for 'writeBehindEnabled' flag. */
        public static bool DefaultWriteBehindEnabled = false;

        /** Default flush size for write-behind cache store. */
        public static int DefaultWriteBehindFlushSize = 10240; // 10K

        /** Default critical size used when flush size is not specified. */
        public static int DefaultWriteBehindCriticalSize = 16384; // 16K

        /** Default flush frequency for write-behind cache store in milliseconds. */
        public static long DefaultWriteBehindFlushFrequency = 5000;

        /** Default count of flush threads for write-behind cache store. */
        public static int DefaultWriteFromBehindFlushThreadCnt = 1;

        /** Default batch size for write-behind cache store. */
        public static int DefaultWriteBehindBatchSize = 512;

        /** Default value for load previous value flag. */
        public static bool DefaultLoadPrevVal = false;

        /** Default memory mode. */
        public static CacheMemoryMode DefaultMemoryMode = CacheMemoryMode.OnheapTiered;

        /** Default value for 'readFromBackup' flag. */
        public static bool DefaultReadFromBackup = true;

        /** Default timeout after which long query warning will be printed. */
        public static long DefaultLongQryWarnTimeout = 3000;

        /** Default size for onheap SQL row cache size. */
        public static int DefaultSqlOnheapRowCacheSize = 10*1024;

        /** Default value for keep portable in store behavior .*/
        public static bool DefaultKeepPortableInStore = true;

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
            // TODO: Default values!!

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

        /// <summary>
        /// Gets cache eviction policy. By default, returns null which means that evictions are disabled for cache.
        /// </summary>
        //public EvictionPolicy EvictionPolicy { get; set; }

        /// <summary>
        /// 
        /// </summary>
        //public NearCacheConfiguration NearConfiguration { get; set; }

        /// <summary>
        /// Gets write synchronization mode. This mode controls whether the main        
        /// caller should wait for update on other nodes to complete or not.
        /// </summary>
        public CacheWriteSynchronizationMode WriteSynchronizationMode { get; set; }

        /// <summary>
        /// Gets filter which determines on what nodes the cache should be started.
        /// </summary>
        //public IgnitePredicate<ClusterNode> NodeFilter { get; set; }

        /// <summary>
        /// Gets flag indicating whether eviction is synchronized between primary, backup and near nodes.        
        /// If this parameter is true and swap is disabled then <see cref="ICache{TK,TV}.LocalEvict"/>
        /// will involve all nodes where an entry is kept.  
        /// If this property is set to false then eviction is done independently on different cache nodes.        
        /// Note that it's not recommended to set this value to true if cache store is configured since it will allow 
        /// to significantly improve cache performance.
        /// </summary>
        public bool EvictSynchronized { get; set; }

        /// <summary>
        /// Gets size of the key buffer for synchronized evictions.
        /// </summary>
        public int EvictSynchronizedKeyBufferSize { get; set; }

        /// <summary>
        /// Gets concurrency level for synchronized evictions. 
        /// This flag only makes sense with <see cref="EvictSynchronized"/> set to true. 
        /// When synchronized evictions are enabled, it is possible that local eviction policy will try 
        /// to evict entries faster than evictions can be synchronized with backup or near nodes. 
        /// This value specifies how many concurrent synchronous eviction sessions should be allowed 
        /// before the system is forced to wait and let synchronous evictions catch up with the eviction policy.       
        /// </summary>
        public int EvictSynchronizedConcurrencyLevel { get; set; }

        /// <summary>
        /// Gets timeout for synchronized evictions.        <p>        Node that initiates eviction waits for responses        from remote nodes within this timeout.        <p>        Default value is defined by { #DEFAULT_EVICT_SYNCHRONIZED_TIMEOUT}.
        /// </summary>
        public long EvictSynchronizedTimeout { get; set; }

        /// <summary>
        /// This value denotes the maximum size of eviction queue in percents of cache        size in case of distributed cache (replicated and partitioned) and using        synchronized eviction (that is if { #isEvictSynchronized()} returns        true).        <p>        That queue is used internally as a buffer to decrease network costs for        synchronized eviction. Once queue size reaches specified value all required        requests for all entries in the queue are sent to remote nodes and the queue        is cleared.        <p>        Default value is defined by { #DEFAULT_MAX_EVICTION_OVERFLOW_RATIO} and        equals to { 10%}.
        /// </summary>
        public float EvictMaxOverflowRatio { get; set; }

        /// <summary>
        /// Gets eviction filter to specify which entries should not be evicted        (except explicit evict by calling { IgniteCache#localEvict(Collection)}).        If { EvictionFilter#evictAllowed(Cache.Entry)} method        returns { false} then eviction policy will not be notified and entry will        never be evicted.        <p>        If not provided, any entry may be evicted depending on        { #getEvictionPolicy() eviction policy} configuration.
        /// </summary>
        //public EvictionFilter<K, EvictionFilter { get; set; }

        /// <summary>
        /// Gets flag indicating whether expired cache entries will be eagerly removed from cache. When        set to { false}, expired entries will be removed on next entry access.        <p>        When not set, default value is { #DEFAULT_EAGER_TTL}.        <p>        <b>Note</b> that this flag only matters for entries expiring based on        { ExpiryPolicy} and should not be confused with entry        evictions based on configured { EvictionPolicy}.
        /// </summary>
        public bool EagerTtl { get; set; }

        /// <summary>
        /// Gets initial cache size which will be used to pre-create internal        hash table after start. Default value is defined by { #DEFAULT_START_SIZE}.
        /// </summary>
        public int StartSize { get; set; }

        /// <summary>
        /// Gets flag indicating whether value should be loaded from store if it is not in the cache        for following cache operations:        <ul>            <li>{ IgniteCache#putIfAbsent(Object, Object)}</li>            <li>{ IgniteCache#replace(Object, Object)}</li>            <li>{ IgniteCache#replace(Object, Object, Object)}</li>            <li>{ IgniteCache#remove(Object, Object)}</li>            <li>{ IgniteCache#getAndPut(Object, Object)}</li>            <li>{ IgniteCache#getAndRemove(Object)}</li>            <li>{ IgniteCache#getAndReplace(Object, Object)}</li>            <li>{ IgniteCache#getAndPutIfAbsent(Object, Object)}</li>       </ul>
        /// </summary>
        public bool LoadPreviousValue { get; set; }

        /// <summary>
        /// Gets factory for underlying persistent storage for read-through and write-through operations.
        /// </summary>
        public bool KeepPortableInStore { get; set; }

        /// <summary>
        /// Gets key topology resolver to provide mapping from keys to nodes.
        /// </summary>
        //public AffinityFunction Affinity { get; set; }

        /// <summary>
        /// Gets caching mode to use. You can configure cache either to be local-only,        fully replicated, partitioned, or near. If not provided, { CacheMode#PARTITIONED}        mode will be used by default (defined by { #DEFAULT_CACHE_MODE} constant).
        /// </summary>
        public CacheMode CacheMode { get; set; }

        /// <summary>
        /// Gets cache atomicity mode.        <p>        Default value is defined by { #DEFAULT_CACHE_ATOMICITY_MODE}.
        /// </summary>
        public CacheAtomicityMode AtomicityMode { get; set; }

        /// <summary>
        /// Gets cache write ordering mode. This property can be enabled only for { CacheAtomicityMode#ATOMIC}        cache (for other atomicity modes it will be ignored).
        /// </summary>
        public CacheAtomicWriteOrderMode AtomicWriteOrderMode { get; set; }

        /// <summary>
        /// Gets number of nodes used to back up single partition for { CacheMode#PARTITIONED} cache.        <p>        If not set, default value is { #DEFAULT_BACKUPS}.
        /// </summary>
        public int Backups { get; set; }

        /// <summary>
        /// Gets default lock acquisition timeout. Default value is defined by { #DEFAULT_LOCK_TIMEOUT}        which is { 0} and means that lock acquisition will never timeout.
        /// </summary>
        public long LockTimeout { get; set; }

        /// <summary>
        /// Invalidation flag. If true, values will be invalidated (nullified) upon commit in near cache.
        /// </summary>
        public bool Invalidate { get; set; }

        /// <summary>
        /// Gets class name of transaction manager finder for integration for JEE app servers.
        /// </summary>
        public CacheRebalanceMode RebalanceMode { get; set; }

        /// <summary>
        /// Gets rebalance mode for distributed cache.        <p>        Default is defined by { #DEFAULT_REBALANCE_MODE}.
        /// </summary>
        public int RebalanceOrder { get; set; }

        /// <summary>
        /// Gets size (in number bytes) to be loaded within a single rebalance message.        Rebalancing algorithm will split total data set on every node into multiple        batches prior to sending data. Default value is defined by        { #DEFAULT_REBALANCE_BATCH_SIZE}.
        /// </summary>
        public int RebalanceBatchSize { get; set; }

        /// <summary>
        /// Flag indicating whether Ignite should use swap storage by default. By default        swap is disabled which is defined via { #DEFAULT_SWAP_ENABLED} constant.
        /// </summary>
        public bool SwapEnabled { get; set; }

        /// <summary>
        /// Gets maximum number of allowed concurrent asynchronous operations. If 0 returned then number        of concurrent asynchronous operations is unlimited.        <p>        If not set, default value is { #DEFAULT_MAX_CONCURRENT_ASYNC_OPS}.        <p>        If user threads do not wait for asynchronous operations to complete, it is possible to overload        a system. This property enables back-pressure control by limiting number of scheduled asynchronous        cache operations.
        /// </summary>
        public int MaxConcurrentAsyncOperations { get; set; }

        /// <summary>
        /// Flag indicating whether Ignite should use write-behind behaviour for the cache store.        By default write-behind is disabled which is defined via { #DEFAULT_WRITE_BEHIND_ENABLED}        constant.
        /// </summary>
        public bool WriteBehindEnabled { get; set; }

        /// <summary>
        /// Maximum size of the write-behind cache. If cache size exceeds this value,        all cached items are flushed to the cache store and write cache is cleared.        <p/>        If not provided, default value is { #DEFAULT_WRITE_BEHIND_FLUSH_SIZE}.        If this value is { 0}, then flush is performed according to the flush frequency interval.        <p/>        Note that you cannot set both, { flush} size and { flush frequency}, to { 0}.
        /// </summary>
        public int WriteBehindFlushSize { get; set; }

        /// <summary>
        /// Frequency with which write-behind cache is flushed to the cache store in milliseconds.        This value defines the maximum time interval between object insertion/deletion from the cache        ant the moment when corresponding operation is applied to the cache store.        <p>        If not provided, default value is { #DEFAULT_WRITE_BEHIND_FLUSH_FREQUENCY}.        If this value is { 0}, then flush is performed according to the flush size.        <p>        Note that you cannot set both, { flush} size and { flush frequency}, to { 0}.
        /// </summary>
        public long WriteBehindFlushFrequency { get; set; }

        /// <summary>
        /// Number of threads that will perform cache flushing. Cache flushing is performed        when cache size exceeds value defined by        { #getWriteBehindFlushSize}, or flush interval defined by        { #getWriteBehindFlushFrequency} is elapsed.        <p/>        If not provided, default value is { #DEFAULT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT}.
        /// </summary>
        public int WriteBehindFlushThreadCount { get; set; }

        /// <summary>
        /// Maximum batch size for write-behind cache store operations. Store operations (get or remove)        are combined in a batch of this size to be passed to        { CacheStore#writeAll(Collection)} or        { CacheStore#deleteAll(Collection)} methods.        <p/>        If not provided, default value is { #DEFAULT_WRITE_BEHIND_BATCH_SIZE}.
        /// </summary>
        public int WriteBehindBatchSize { get; set; }

        /// <summary>
        /// Gets size of rebalancing thread pool. Note that size serves as a hint and implementation        may create more threads for rebalancing than specified here (but never less threads).        <p>        Default value is { #DEFAULT_REBALANCE_THREAD_POOL_SIZE}.
        /// </summary>
        public int RebalanceThreadPoolSize { get; set; }

        /// <summary>
        /// Gets rebalance timeout (ms).        <p>        Default value is { #DEFAULT_REBALANCE_TIMEOUT}.
        /// </summary>
        public long RebalanceTimeout { get; set; }

        /// <summary>
        /// Gets delay in milliseconds upon a node joining or leaving topology (or crash) after which rebalancing        should be started automatically. Rebalancing should be delayed if you plan to restart nodes        after they leave topology, or if you plan to start multiple nodes at once or one after another        and don't want to repartition and rebalance until all nodes are started.        <p>        For better efficiency user should usually make sure that new nodes get placed on        the same place of consistent hash ring as the left nodes, and that nodes are        restarted before this delay expires. To place nodes on the same place in consistent hash ring,        use { IgniteConfiguration#setConsistentId(Serializable)}        to make sure that a node maps to the same hash ID event if restarted. As an example,        node IP address and port combination may be used in this case.        <p>        Default value is { 0} which means that repartitioning and rebalancing will start        immediately upon node leaving topology. If { -1} is returned, then rebalancing        will only be started manually by calling { IgniteCache#rebalance()} method or        from management console.
        /// </summary>
        public long RebalanceDelay { get; set; }

        /// <summary>
        /// Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.        When rebalancing large data sets, the CPU or network can get over-consumed with rebalancing messages,        which consecutively may slow down the application performance. This parameter helps tune        the amount of time to wait between rebalance messages to make sure that rebalancing process        does not have any negative performance impact. Note that application will continue to work        properly while rebalancing is still in progress.        <p>        Value of { 0} means that throttling is disabled. By default throttling is disabled -        the default is defined by { #DEFAULT_REBALANCE_THROTTLE} constant.
        /// </summary>
        public long RebalanceThrottle { get; set; }

        /// <summary>
        /// Affinity key mapper used to provide custom affinity key for any given key.        Affinity mapper is particularly useful when several objects need to be collocated        on the same node (they will also be backed up on the same nodes as well).        <p>        If not provided, then default implementation will be used. The default behavior        is described in { AffinityKeyMapper} documentation.
        /// </summary>
        //public AffinityKeyMapper AffinityMapper { get; set; }

        /// <summary>
        /// Gets maximum amount of memory available to off-heap storage. Possible values are        <ul>        <li>{ -1} - Means that off-heap storage is disabled.</li>        <li>            { 0} - Ignite will not limit off-heap storage (it's up to user to properly            add and remove entries from cache to ensure that off-heap storage does not grow            indefinitely.        </li>        <li>Any positive value specifies the limit of off-heap storage in bytes.</li>        </ul>        Default value is { -1}, specified by { #DEFAULT_OFFHEAP_MEMORY} constant        which means that off-heap storage is disabled by default.        <p>        Use off-heap storage to load gigabytes of data in memory without slowing down        Garbage Collection. Essentially in this case you should allocate very small amount        of memory to JVM and Ignite will cache most of the data in off-heap space        without affecting JVM performance at all.        <p>        Note that Ignite will throw an exception if max memory is set to { -1} and        { offHeapValuesOnly} flag is set to true.
        /// </summary>
        public long OffHeapMaxMemory { get; set; }

        /// <summary>
        /// Gets memory mode for cache. Memory mode helps control whether value is stored in on-heap memory,        off-heap memory, or swap space. Refer to { CacheMemoryMode} for more info.        <p>        Default value is { #DEFAULT_MEMORY_MODE}.
        /// </summary>
        public CacheMemoryMode MemoryMode { get; set; }

        /// <summary>
        /// Gets cache interceptor.
        /// </summary>
        //public CacheInterceptor Interceptor { get; set; }

        /// <summary>
        /// Gets collection of type metadata objects.
        /// </summary>
        //public ICollection<CacheTypeMetadata> TypeMetadata { get; set; }

        /// <summary>
        /// Gets flag indicating whether data can be read from backup.        If { false} always get data from primary node (never from backup).        <p>        Default value is defined by { #DEFAULT_READ_FROM_BACKUP}.
        /// </summary>
        public bool ReadFromBackup { get; set; }

        /// <summary>
        /// Gets flag indicating whether copy of of the value stored in cache should be created        for cache operation implying return value. Also if this flag is set copies are created for values        passed to { CacheInterceptor} and to { CacheEntryProcessor}.
        /// </summary>
        public bool CopyOnRead { get; set; }

        /// <summary>
        /// Gets classes with methods annotated by { QuerySqlFunction}        to be used as user-defined functions from SQL queries.
        /// </summary>
        public long LongQueryWarningTimeout { get; set; }

        /// <summary>
        /// If true all the SQL table and field names will be escaped with double quotes like        ({ "tableName"."fieldsName"}). This enforces case sensitivity for field names and        also allows having special characters in table and field names.
        /// </summary>
        public bool SqlEscapeAll { get; set; }

        /// <summary>
        /// Array of key and value type pairs to be indexed (thus array length must be always even).        It means each even (0,2,4...) class in the array will be considered as key type for cache entry,        each odd (1,3,5...) class will be considered as value type for cache entry.        <p>        The same key class can occur multiple times for different value classes, but each value class must be unique        because SQL table will be named as value class simple name.        <p>        To expose fields of these types onto SQL level and to index them you have to use annotations        from package { org.apache.ignite.cache.query.annotations}.
        /// </summary>
        //public Class<?>...IndexedTypes { get; set; }

        /// <summary>
        /// Number of SQL rows which will be cached onheap to avoid deserialization on each SQL index access.        This setting only makes sense when offheap is enabled for this cache.
        /// </summary>
        public int SqlOnheapRowCacheSize { get; set; }

        /// <summary>
        /// Gets a collection of configured  query entities.
        /// </summary>
        //public Collection<QueryEntity> QueryEntities { get; set; }
    }
}

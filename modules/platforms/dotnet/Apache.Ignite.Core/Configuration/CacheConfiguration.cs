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

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Apache.Ignite.Core.Configuration
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Store;

    /// <summary>
    /// Defines grid cache configuration.
    /// </summary>
    public class CacheConfiguration
    {
        /** Default size of rebalance thread pool. */
        public const int DefaultRebalanceThreadPoolSize = 2;

        /** Default rebalance timeout (ms).*/
        public const long DefaultRebalanceTimeout = 10000;

        /** Time in milliseconds to wait between rebalance messages to avoid overloading CPU. */
        public const long DefaultRebalanceThrottle = 0;

        /** Default number of backups. */
        public const int DefaultBackups = 0;

        /** Default caching mode. */
        public const CacheMode DefaultCacheMode = CacheMode.Partitioned;

        /** Default atomicity mode. */
        public const CacheAtomicityMode DefaultCacheAtomicityMode = CacheAtomicityMode.Atomic;

        /** Default lock timeout. */
        public const long DefaultLockTimeout = 0;

        /** Initial default cache size. */
        public const int DefaultStartSize = 1500000;

        /** Default cache size to use with eviction policy. */
        public const int DefaultCacheSize = 100000;

        /** Default value for 'invalidate' flag that indicates if this is invalidation-based cache. */
        public const bool DefaultInvalidate = false;

        /** Default rebalance mode for distributed cache. */
        public const CacheRebalanceMode DefaultRebalanceMode = CacheRebalanceMode.Async;

        /** Default rebalance batch size in bytes. */
        public const int DefaultRebalanceBatchSize = 512*1024; // 512K

        /** Default maximum eviction queue ratio. */
        public const float DefaultMaxEvictionOverflowRatio = 10;

        /** Default eviction synchronized flag. */
        public const bool DefaultEvictSynchronized = false;

        /** Default eviction key buffer size for batching synchronized evicts. */
        public const int DefaultEvictKeyBufferSize = 1024;

        /** Default synchronous eviction timeout in milliseconds. */
        public const long DefaultEvictSynchronizedTimeout = 10000;

        /** Default synchronous eviction concurrency level. */
        public const int DefaultEvictSynchronizedConcurrencyLevel = 4;

        /** Default value for eager ttl flag. */
        public const bool DefaultEagerTtl = true;

        /** Default off-heap storage size is {@code -1} which means that off-heap storage is disabled. */
        public const long DefaultOffheapMemory = -1;

        /** Default value for 'swapEnabled' flag. */
        public const bool DefaultSwapEnabled = false;

        /** Default value for 'maxConcurrentAsyncOps'. */
        public const int DefaultMaxConcurrentAsyncOperations = 500;

        /** Default value for 'writeBehindEnabled' flag. */
        public const bool DefaultWriteBehindEnabled = false;

        /** Default flush size for write-behind cache store. */
        public const int DefaultWriteBehindFlushSize = 10240; // 10K

        /** Default flush frequency for write-behind cache store in milliseconds. */
        public const long DefaultWriteBehindFlushFrequency = 5000;

        /** Default count of flush threads for write-behind cache store. */
        public const int DefaultWriteFromBehindFlushThreadCount = 1;

        /** Default batch size for write-behind cache store. */
        public const int DefaultWriteBehindBatchSize = 512;

        /** Default value for load previous value flag. */
        public const bool DefaultLoadPreviousValue = false;

        /** Default memory mode. */
        public const CacheMemoryMode DefaultMemoryMode = CacheMemoryMode.OnheapTiered;

        /** Default value for 'readFromBackup' flag. */
        public const bool DefaultReadFromBackup = true;

        /** Default timeout after which long query warning will be printed. */
        public const long DefaultLongQueryWarningTimeout = 3000;

        /** Default size for onheap SQL row cache size. */
        public const int DefaultSqlOnheapRowCacheSize = 10*1024;

        /** Default value for keep portable in store behavior .*/
        public const bool DefaultKeepPortableInStore = true;

        /** Default value for 'copyOnRead' flag. */
        public const bool DefaultCopyOnRead = true;

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

            Backups = DefaultBackups;
            AtomicityMode = DefaultCacheAtomicityMode;
            CacheMode = DefaultCacheMode;
            CopyOnRead = DefaultCopyOnRead;
            StartSize = DefaultCacheSize;
            EagerTtl = DefaultEagerTtl;
            EvictSynchronizedKeyBufferSize = DefaultEvictKeyBufferSize;
            EvictSynchronized = DefaultEvictSynchronized;
            EvictSynchronizedConcurrencyLevel = DefaultEvictSynchronizedConcurrencyLevel;
            EvictSynchronizedTimeout = DefaultEvictSynchronizedTimeout;
            Invalidate = DefaultInvalidate;
            KeepPortableInStore = DefaultKeepPortableInStore;
            LoadPreviousValue = DefaultLoadPreviousValue;
            LockTimeout = DefaultLockTimeout;
            LongQueryWarningTimeout = DefaultLongQueryWarningTimeout;
            MaxConcurrentAsyncOperations = DefaultMaxConcurrentAsyncOperations;
            MaxEvictionOverflowRatio = DefaultMaxEvictionOverflowRatio;
            MemoryMode = DefaultMemoryMode;
            OffHeapMaxMemory = DefaultOffheapMemory;
            ReadFromBackup = DefaultReadFromBackup;
            RebalanceBatchSize = DefaultRebalanceBatchSize;
            RebalanceMode = DefaultRebalanceMode;
            RebalanceThreadPoolSize = DefaultRebalanceThreadPoolSize;
            RebalanceThrottle = DefaultRebalanceThrottle;
            RebalanceTimeout = DefaultRebalanceTimeout;
            SqlOnheapRowCacheSize = DefaultSqlOnheapRowCacheSize;
            StartSize = DefaultStartSize;
            StartSize = DefaultStartSize;
            EnableSwap = DefaultSwapEnabled;
            WriteBehindBatchSize = DefaultWriteBehindBatchSize;
            WriteBehindEnabled = DefaultWriteBehindEnabled;
            WriteBehindFlushFrequency = DefaultWriteBehindFlushFrequency;
            WriteBehindFlushSize = DefaultWriteBehindFlushSize;
            WriteBehindFlushThreadCount= DefaultWriteFromBehindFlushThreadCount;
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
        /// Gets or sets write synchronization mode. This mode controls whether the main        
        /// caller should wait for update on other nodes to complete or not.
        /// </summary>
        public CacheWriteSynchronizationMode WriteSynchronizationMode { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether eviction is synchronized between primary, backup and near nodes.        
        /// If this parameter is true and swap is disabled then <see cref="ICache{TK,TV}.LocalEvict"/>
        /// will involve all nodes where an entry is kept.  
        /// If this property is set to false then eviction is done independently on different cache nodes.        
        /// Note that it's not recommended to set this value to true if cache store is configured since it will allow 
        /// to significantly improve cache performance.
        /// </summary>
        public bool EvictSynchronized { get; set; }

        /// <summary>
        /// Gets or sets size of the key buffer for synchronized evictions.
        /// </summary>
        public int EvictSynchronizedKeyBufferSize { get; set; }

        /// <summary>
        /// Gets or sets concurrency level for synchronized evictions. 
        /// This flag only makes sense with <see cref="EvictSynchronized"/> set to true. 
        /// When synchronized evictions are enabled, it is possible that local eviction policy will try 
        /// to evict entries faster than evictions can be synchronized with backup or near nodes. 
        /// This value specifies how many concurrent synchronous eviction sessions should be allowed 
        /// before the system is forced to wait and let synchronous evictions catch up with the eviction policy.       
        /// </summary>
        public int EvictSynchronizedConcurrencyLevel { get; set; }

        /// <summary>
        /// Gets or sets timeout for synchronized evictions
        /// </summary>
        public long EvictSynchronizedTimeout { get; set; }

        /// <summary>
        /// This value denotes the maximum size of eviction queue in percents of cache size 
        /// in case of distributed cache (replicated and partitioned) and using synchronized eviction
        /// <para/>        
        /// That queue is used internally as a buffer to decrease network costs for synchronized eviction. 
        /// Once queue size reaches specified value all required requests for all entries in the queue 
        /// are sent to remote nodes and the queue is cleared.
        /// </summary>
        public float MaxEvictionOverflowRatio { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether expired cache entries will be eagerly removed from cache. 
        /// When set to false, expired entries will be removed on next entry access.        
        /// </summary>
        public bool EagerTtl { get; set; }

        /// <summary>
        /// Gets or sets initial cache size which will be used to pre-create internal hash table after start.
        /// </summary>
        public int StartSize { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether value should be loaded from store if it is not in the cache 
        /// for the following cache operations:   
        /// <list type="bullet">
        /// <item><term><see cref="ICache{TK,TV}.PutIfAbsent"/></term></item>
        /// <item><term><see cref="ICache{TK,TV}.Replace(TK,TV)"/></term></item>
        /// <item><term><see cref="ICache{TK,TV}.Remove(TK)"/></term></item>
        /// <item><term><see cref="ICache{TK,TV}.GetAndPut"/></term></item>
        /// <item><term><see cref="ICache{TK,TV}.GetAndRemove"/></term></item>
        /// <item><term><see cref="ICache{TK,TV}.GetAndReplace"/></term></item>
        /// <item><term><see cref="ICache{TK,TV}.GetAndPutIfAbsent"/></term></item>
        /// </list>     
        /// </summary>
        public bool LoadPreviousValue { get; set; }

        /// <summary>
        /// Gets or sets factory for underlying persistent storage for read-through and write-through operations.
        /// </summary>
        public bool KeepPortableInStore { get; set; }

        /// <summary>
        /// Gets or sets caching mode to use.
        /// </summary>
        public CacheMode CacheMode { get; set; }

        /// <summary>
        /// Gets or sets cache atomicity mode.
        /// </summary>
        public CacheAtomicityMode AtomicityMode { get; set; }

        /// <summary>
        /// Gets or sets cache write ordering mode.
        /// </summary>
        public CacheAtomicWriteOrderMode AtomicWriteOrderMode { get; set; }

        /// <summary>
        /// Gets or sets number of nodes used to back up single partition for 
        /// <see cref="Configuration.CacheMode.Partitioned"/> cache.
        /// </summary>
        public int Backups { get; set; }

        /// <summary>
        /// Gets or sets default lock acquisition timeout.
        /// </summary>
        public long LockTimeout { get; set; }  // TODO: Timespan

        /// <summary>
        /// Invalidation flag. If true, values will be invalidated (nullified) upon commit in near cache.
        /// </summary>
        public bool Invalidate { get; set; }

        /// <summary>
        /// Gets or sets cache rebalance mode.
        /// </summary>
        public CacheRebalanceMode RebalanceMode { get; set; }

        /// <summary>
        /// Gets or sets size (in number bytes) to be loaded within a single rebalance message.
        /// Rebalancing algorithm will split total data set on every node into multiple batches prior to sending data.
        /// </summary>
        public int RebalanceBatchSize { get; set; }

        /// <summary>
        /// Flag indicating whether Ignite should use swap storage by default.
        /// </summary>
        public bool EnableSwap { get; set; }

        /// <summary>
        /// Gets or sets maximum number of allowed concurrent asynchronous operations, 0 for unlimited.
        /// </summary>
        public int MaxConcurrentAsyncOperations { get; set; }

        /// <summary>
        /// Flag indicating whether Ignite should use write-behind behaviour for the cache store.
        /// </summary>
        public bool WriteBehindEnabled { get; set; }

        /// <summary>
        /// Maximum size of the write-behind cache. If cache size exceeds this value, all cached items are flushed 
        /// to the cache store and write cache is cleared.
        /// </summary>
        public int WriteBehindFlushSize { get; set; }

        /// <summary>
        /// Frequency with which write-behind cache is flushed to the cache store in milliseconds.
        /// This value defines the maximum time interval between object insertion/deletion from the cache
        /// at the moment when corresponding operation is applied to the cache store.
        /// <para/> 
        /// If this value is 0, then flush is performed according to the flush size.
        /// <para/>
        /// Note that you cannot set both
        /// <see cref="WriteBehindFlushSize"/> and <see cref="WriteBehindFlushFrequency"/> to 0.
        /// </summary>
        public long WriteBehindFlushFrequency { get; set; }

        /// <summary>
        /// Number of threads that will perform cache flushing. Cache flushing is performed when cache size exceeds 
        /// value defined by <see cref="WriteBehindFlushSize"/>, or flush interval defined by 
        /// <see cref="WriteBehindFlushFrequency"/> is elapsed.
        /// </summary>
        public int WriteBehindFlushThreadCount { get; set; }

        /// <summary>
        /// Maximum batch size for write-behind cache store operations. 
        /// Store operations (get or remove) are combined in a batch of this size to be passed to 
        /// <see cref="ICacheStore.WriteAll"/> or <see cref="ICacheStore.DeleteAll"/> methods. 
        /// </summary>
        public int WriteBehindBatchSize { get; set; }

        /// <summary>
        /// Gets or sets size of rebalancing thread pool. Note that size serves as a hint and implementation 
        /// may create more threads for rebalancing than specified here (but never less threads).
        /// </summary>
        public int RebalanceThreadPoolSize { get; set; }

        /// <summary>
        /// Gets or sets rebalance timeout (ms).
        /// </summary>
        public long RebalanceTimeout { get; set; }  // TODO: Timespan

        /// <summary>
        /// Gets or sets delay in milliseconds upon a node joining or leaving topology (or crash) 
        /// after which rebalancing should be started automatically. 
        /// Rebalancing should be delayed if you plan to restart nodes
        /// after they leave topology, or if you plan to start multiple nodes at once or one after another
        /// and don't want to repartition and rebalance until all nodes are started.
        /// </summary>
        public long RebalanceDelay { get; set; }

        /// <summary>
        /// Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.
        /// When rebalancing large data sets, the CPU or network can get over-consumed with rebalancing messages,
        /// which consecutively may slow down the application performance. This parameter helps tune 
        /// the amount of time to wait between rebalance messages to make sure that rebalancing process
        /// does not have any negative performance impact. Note that application will continue to work
        /// properly while rebalancing is still in progress.
        /// <para/>
        /// Value of 0 means that throttling is disabled.
        /// </summary>
        public long RebalanceThrottle { get; set; }

        /// <summary>
        /// Gets or sets maximum amount of memory available to off-heap storage. Possible values are
        /// -1 means that off-heap storage is disabled. 0 means that Ignite will not limit off-heap storage 
        /// (it's up to user to properly add and remove entries from cache to ensure that off-heap storage 
        /// does not grow indefinitely.
        /// Any positive value specifies the limit of off-heap storage in bytes.
        /// </summary>
        public long OffHeapMaxMemory { get; set; }

        /// <summary>
        /// Gets or sets memory mode for cache.
        /// </summary>
        public CacheMemoryMode MemoryMode { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether data can be read from backup.
        /// </summary>
        public bool ReadFromBackup { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether copy of of the value stored in cache should be created
        /// for cache operation implying return value. 
        /// </summary>
        public bool CopyOnRead { get; set; }

        /// <summary>
        /// Gets or sets the timeout after which long query warning will be printed.
        /// </summary>
        public long LongQueryWarningTimeout { get; set; }  // TODO

        /// <summary>
        /// If true all the SQL table and field names will be escaped with double quotes like 
        /// ({ "tableName"."fieldsName"}). This enforces case sensitivity for field names and
        /// also allows having special characters in table and field names.
        /// </summary>
        public bool SqlEscapeAll { get; set; }

        /// <summary>
        /// Number of SQL rows which will be cached onheap to avoid deserialization on each SQL index access.
        /// This setting only makes sense when offheap is enabled for this cache.
        /// </summary>
        public int SqlOnheapRowCacheSize { get; set; }
    }
}

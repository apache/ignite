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
namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache.Affinity;

    /// <summary>
    /// Defines grid cache configuration.
    /// </summary>
    public class CacheConfiguration
    {
        /// <summary> Default size of rebalance thread pool. </summary>
        public const int DefaultRebalanceThreadPoolSize = 2;

        /// <summary> Default rebalance timeout.</summary>
        public static readonly TimeSpan DefaultRebalanceTimeout = TimeSpan.FromMilliseconds(10000);

        /// <summary> Time to wait between rebalance messages to avoid overloading CPU. </summary>
        public static readonly TimeSpan DefaultRebalanceThrottle = TimeSpan.Zero;

        /// <summary> Default number of backups. </summary>
        public const int DefaultBackups = 0;

        /// <summary> Default caching mode. </summary>
        public const CacheMode DefaultCacheMode = CacheMode.Partitioned;

        /// <summary> Default atomicity mode. </summary>
        public const CacheAtomicityMode DefaultAtomicityMode = CacheAtomicityMode.Atomic;

        /// <summary> Default lock timeout. </summary>
        public static readonly TimeSpan DefaultLockTimeout = TimeSpan.Zero;

        /// <summary> Initial default cache size. </summary>
        public const int DefaultStartSize = 1500000;

        /// <summary> Default cache size to use with eviction policy. </summary>
        public const int DefaultCacheSize = 100000;

        /// <summary> Default value for 'invalidate' flag that indicates if this is invalidation-based cache. </summary>
        public const bool DefaultInvalidate = false;

        /// <summary> Default rebalance mode for distributed cache. </summary>
        public const CacheRebalanceMode DefaultRebalanceMode = CacheRebalanceMode.Async;

        /// <summary> Default rebalance batch size in bytes. </summary>
        public const int DefaultRebalanceBatchSize = 512*1024; // 512K

        /// <summary> Default maximum eviction queue ratio. </summary>
        public const float DefaultMaxEvictionOverflowRatio = 10;

        /// <summary> Default eviction synchronized flag. </summary>
        public const bool DefaultEvictSynchronized = false;

        /// <summary> Default eviction key buffer size for batching synchronized evicts. </summary>
        public const int DefaultEvictSynchronizedKeyBufferSize = 1024;

        /// <summary> Default synchronous eviction timeout. </summary>
        public static readonly TimeSpan DefaultEvictSynchronizedTimeout = TimeSpan.FromMilliseconds(10000);

        /// <summary> Default synchronous eviction concurrency level. </summary>
        public const int DefaultEvictSynchronizedConcurrencyLevel = 4;

        /// <summary> Default value for eager ttl flag. </summary>
        public const bool DefaultEagerTtl = true;

        /// <summary> Default off-heap storage size is {@code -1} which means that off-heap storage is disabled. </summary>
        public const long DefaultOffHeapMaxMemory = -1;

        /// <summary> Default value for 'swapEnabled' flag. </summary>
        public const bool DefaultEnableSwap = false;

        /// <summary> Default value for 'maxConcurrentAsyncOps'. </summary>
        public const int DefaultMaxConcurrentAsyncOperations = 500;

        /// <summary> Default value for 'writeBehindEnabled' flag. </summary>
        public const bool DefaultWriteBehindEnabled = false;

        /// <summary> Default flush size for write-behind cache store. </summary>
        public const int DefaultWriteBehindFlushSize = 10240; // 10K

        /// <summary> Default flush frequency for write-behind cache store. </summary>
        public static readonly TimeSpan DefaultWriteBehindFlushFrequency = TimeSpan.FromMilliseconds(5000);

        /// <summary> Default count of flush threads for write-behind cache store. </summary>
        public const int DefaultWriteBehindFlushThreadCount = 1;

        /// <summary> Default batch size for write-behind cache store. </summary>
        public const int DefaultWriteBehindBatchSize = 512;

        /// <summary> Default value for load previous value flag. </summary>
        public const bool DefaultLoadPreviousValue = false;

        /// <summary> Default memory mode. </summary>
        public const CacheMemoryMode DefaultMemoryMode = CacheMemoryMode.OnheapTiered;

        /// <summary> Default value for 'readFromBackup' flag. </summary>
        public const bool DefaultReadFromBackup = true;

        /// <summary> Default timeout after which long query warning will be printed. </summary>
        public static readonly TimeSpan DefaultLongQueryWarningTimeout = TimeSpan.FromMilliseconds(3000);

        /// <summary> Default size for onheap SQL row cache size. </summary>
        public const int DefaultSqlOnheapRowCacheSize = 10*1024;

        /// <summary> Default value for keep portable in store behavior .</summary>
        public const bool DefaultKeepVinaryInStore = true;

        /// <summary> Default value for 'copyOnRead' flag. </summary>
        public const bool DefaultCopyOnRead = true;

        /// <summary> Default value for read-through behavior. </summary>
        public const bool DefaultReadThrough = false;

        /// <summary> Default value for write-through behavior. </summary>
        public const bool DefaultWriteThrough = false;

        /// <summary>
        /// Gets or sets the cache name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        public CacheConfiguration() : this((string) null)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="name">Cache name.</param>
        public CacheConfiguration(string name)
        {
            Name = name;

            Backups = DefaultBackups;
            AtomicityMode = DefaultAtomicityMode;
            CacheMode = DefaultCacheMode;
            CopyOnRead = DefaultCopyOnRead;
            EagerTtl = DefaultEagerTtl;
            EvictSynchronizedKeyBufferSize = DefaultEvictSynchronizedKeyBufferSize;
            EvictSynchronized = DefaultEvictSynchronized;
            EvictSynchronizedConcurrencyLevel = DefaultEvictSynchronizedConcurrencyLevel;
            EvictSynchronizedTimeout = DefaultEvictSynchronizedTimeout;
            Invalidate = DefaultInvalidate;
            KeepBinaryInStore = DefaultKeepVinaryInStore;
            LoadPreviousValue = DefaultLoadPreviousValue;
            LockTimeout = DefaultLockTimeout;
            LongQueryWarningTimeout = DefaultLongQueryWarningTimeout;
            MaxConcurrentAsyncOperations = DefaultMaxConcurrentAsyncOperations;
            MaxEvictionOverflowRatio = DefaultMaxEvictionOverflowRatio;
            MemoryMode = DefaultMemoryMode;
            OffHeapMaxMemory = DefaultOffHeapMaxMemory;
            ReadFromBackup = DefaultReadFromBackup;
            RebalanceBatchSize = DefaultRebalanceBatchSize;
            RebalanceMode = DefaultRebalanceMode;
            RebalanceThrottle = DefaultRebalanceThrottle;
            RebalanceTimeout = DefaultRebalanceTimeout;
            SqlOnheapRowCacheSize = DefaultSqlOnheapRowCacheSize;
            StartSize = DefaultStartSize;
            EnableSwap = DefaultEnableSwap;
            WriteBehindBatchSize = DefaultWriteBehindBatchSize;
            WriteBehindEnabled = DefaultWriteBehindEnabled;
            WriteBehindFlushFrequency = DefaultWriteBehindFlushFrequency;
            WriteBehindFlushSize = DefaultWriteBehindFlushSize;
            WriteBehindFlushThreadCount= DefaultWriteBehindFlushThreadCount;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class 
        /// and populates <see cref="QueryEntities"/> according to provided query types.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <param name="queryTypes">
        /// Collection of types to be registered as query entities. These types should use 
        /// <see cref="QuerySqlFieldAttribute"/> to configure query fields and properties.
        /// </param>
        public CacheConfiguration(string name, params Type[] queryTypes) : this(name)
        {
            QueryEntities = queryTypes.Select(type => new QueryEntity {ValueType = type}).ToArray();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <param name="queryEntities">Query entities.</param>
        public CacheConfiguration(string name, params QueryEntity[] queryEntities) : this(name)
        {
            QueryEntities = queryEntities;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal CacheConfiguration(IBinaryRawReader reader)
        {
            AtomicityMode = (CacheAtomicityMode) reader.ReadInt();
            AtomicWriteOrderMode = (CacheAtomicWriteOrderMode) reader.ReadInt();
            Backups = reader.ReadInt();
            CacheMode = (CacheMode) reader.ReadInt();
            CopyOnRead = reader.ReadBoolean();
            EagerTtl = reader.ReadBoolean();
            EnableSwap = reader.ReadBoolean();
            EvictSynchronized = reader.ReadBoolean();
            EvictSynchronizedConcurrencyLevel = reader.ReadInt();
            EvictSynchronizedKeyBufferSize = reader.ReadInt();
            EvictSynchronizedTimeout = reader.ReadLongAsTimespan();
            Invalidate = reader.ReadBoolean();
            KeepBinaryInStore = reader.ReadBoolean();
            LoadPreviousValue = reader.ReadBoolean();
            LockTimeout = reader.ReadLongAsTimespan();
            LongQueryWarningTimeout = reader.ReadLongAsTimespan();
            MaxConcurrentAsyncOperations = reader.ReadInt();
            MaxEvictionOverflowRatio = reader.ReadFloat();
            MemoryMode = (CacheMemoryMode) reader.ReadInt();
            Name = reader.ReadString();
            OffHeapMaxMemory = reader.ReadLong();
            ReadFromBackup = reader.ReadBoolean();
            RebalanceBatchSize = reader.ReadInt();
            RebalanceDelay = reader.ReadLongAsTimespan();
            RebalanceMode = (CacheRebalanceMode) reader.ReadInt();
            RebalanceThrottle = reader.ReadLongAsTimespan();
            RebalanceTimeout = reader.ReadLongAsTimespan();
            SqlEscapeAll = reader.ReadBoolean();
            SqlOnheapRowCacheSize = reader.ReadInt();
            StartSize = reader.ReadInt();
            WriteBehindBatchSize = reader.ReadInt();
            WriteBehindEnabled = reader.ReadBoolean();
            WriteBehindFlushFrequency = reader.ReadLongAsTimespan();
            WriteBehindFlushSize = reader.ReadInt();
            WriteBehindFlushThreadCount = reader.ReadInt();
            WriteSynchronizationMode = (CacheWriteSynchronizationMode) reader.ReadInt();
            ReadThrough = reader.ReadBoolean();
            WriteThrough = reader.ReadBoolean();
            CacheStoreFactory = reader.ReadObject<IFactory<ICacheStore>>();

            var count = reader.ReadInt();
            QueryEntities = count == 0 ? null : Enumerable.Range(0, count).Select(x => new QueryEntity(reader)).ToList();

            NearConfiguration = reader.ReadBoolean() ? new NearCacheConfiguration(reader) : null;

            EvictionPolicy = EvictionPolicyBase.Read(reader);
            AffinityFunction = AffinityFunctionSerializer.Read(reader);
        }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt((int) AtomicityMode);
            writer.WriteInt((int) AtomicWriteOrderMode);
            writer.WriteInt(Backups);
            writer.WriteInt((int) CacheMode);
            writer.WriteBoolean(CopyOnRead);
            writer.WriteBoolean(EagerTtl);
            writer.WriteBoolean(EnableSwap);
            writer.WriteBoolean(EvictSynchronized);
            writer.WriteInt(EvictSynchronizedConcurrencyLevel);
            writer.WriteInt(EvictSynchronizedKeyBufferSize);
            writer.WriteLong((long) EvictSynchronizedTimeout.TotalMilliseconds);
            writer.WriteBoolean(Invalidate);
            writer.WriteBoolean(KeepBinaryInStore);
            writer.WriteBoolean(LoadPreviousValue);
            writer.WriteLong((long) LockTimeout.TotalMilliseconds);
            writer.WriteLong((long) LongQueryWarningTimeout.TotalMilliseconds);
            writer.WriteInt(MaxConcurrentAsyncOperations);
            writer.WriteFloat(MaxEvictionOverflowRatio);
            writer.WriteInt((int) MemoryMode);
            writer.WriteString(Name);
            writer.WriteLong(OffHeapMaxMemory);
            writer.WriteBoolean(ReadFromBackup);
            writer.WriteInt(RebalanceBatchSize);
            writer.WriteLong((long) RebalanceDelay.TotalMilliseconds);
            writer.WriteInt((int) RebalanceMode);
            writer.WriteLong((long) RebalanceThrottle.TotalMilliseconds);
            writer.WriteLong((long) RebalanceTimeout.TotalMilliseconds);
            writer.WriteBoolean(SqlEscapeAll);
            writer.WriteInt(SqlOnheapRowCacheSize);
            writer.WriteInt(StartSize);
            writer.WriteInt(WriteBehindBatchSize);
            writer.WriteBoolean(WriteBehindEnabled);
            writer.WriteLong((long) WriteBehindFlushFrequency.TotalMilliseconds);
            writer.WriteInt(WriteBehindFlushSize);
            writer.WriteInt(WriteBehindFlushThreadCount);
            writer.WriteInt((int) WriteSynchronizationMode);
            writer.WriteBoolean(ReadThrough);
            writer.WriteBoolean(WriteThrough);
            writer.WriteObject(CacheStoreFactory);

            if (QueryEntities != null)
            {
                writer.WriteInt(QueryEntities.Count);

                foreach (var entity in QueryEntities)
                {
                    if (entity == null)
                        throw new InvalidOperationException("Invalid cache configuration: QueryEntity can't be null.");

                    entity.Write(writer);
                }
            }
            else
                writer.WriteInt(0);

            if (NearConfiguration != null)
            {
                writer.WriteBoolean(true);
                NearConfiguration.Write(writer);
            }
            else
                writer.WriteBoolean(false);

            EvictionPolicyBase.Write(writer, EvictionPolicy);
            AffinityFunctionSerializer.Write(writer, AffinityFunction);
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
        [DefaultValue(DefaultEvictSynchronized)]
        public bool EvictSynchronized { get; set; }

        /// <summary>
        /// Gets or sets size of the key buffer for synchronized evictions.
        /// </summary>
        [DefaultValue(DefaultEvictSynchronizedKeyBufferSize)]
        public int EvictSynchronizedKeyBufferSize { get; set; }

        /// <summary>
        /// Gets or sets concurrency level for synchronized evictions. 
        /// This flag only makes sense with <see cref="EvictSynchronized"/> set to true. 
        /// When synchronized evictions are enabled, it is possible that local eviction policy will try 
        /// to evict entries faster than evictions can be synchronized with backup or near nodes. 
        /// This value specifies how many concurrent synchronous eviction sessions should be allowed 
        /// before the system is forced to wait and let synchronous evictions catch up with the eviction policy.       
        /// </summary>
        [DefaultValue(DefaultEvictSynchronizedConcurrencyLevel)]
        public int EvictSynchronizedConcurrencyLevel { get; set; }

        /// <summary>
        /// Gets or sets timeout for synchronized evictions
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:10")]
        public TimeSpan EvictSynchronizedTimeout { get; set; }

        /// <summary>
        /// This value denotes the maximum size of eviction queue in percents of cache size 
        /// in case of distributed cache (replicated and partitioned) and using synchronized eviction
        /// <para/>        
        /// That queue is used internally as a buffer to decrease network costs for synchronized eviction. 
        /// Once queue size reaches specified value all required requests for all entries in the queue 
        /// are sent to remote nodes and the queue is cleared.
        /// </summary>
        [DefaultValue(DefaultMaxEvictionOverflowRatio)]
        public float MaxEvictionOverflowRatio { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether expired cache entries will be eagerly removed from cache. 
        /// When set to false, expired entries will be removed on next entry access.        
        /// </summary>
        [DefaultValue(DefaultEagerTtl)]
        public bool EagerTtl { get; set; }

        /// <summary>
        /// Gets or sets initial cache size which will be used to pre-create internal hash table after start.
        /// </summary>
        [DefaultValue(DefaultStartSize)]
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
        [DefaultValue(DefaultLoadPreviousValue)]
        public bool LoadPreviousValue { get; set; }

        /// <summary>
        /// Gets or sets the flag indicating whether <see cref="ICacheStore"/> is working with binary objects 
        /// instead of deserialized objects.
        /// </summary>
        [DefaultValue(DefaultKeepVinaryInStore)]
        public bool KeepBinaryInStore { get; set; }

        /// <summary>
        /// Gets or sets caching mode to use.
        /// </summary>
        [DefaultValue(DefaultCacheMode)]
        public CacheMode CacheMode { get; set; }

        /// <summary>
        /// Gets or sets cache atomicity mode.
        /// </summary>
        [DefaultValue(DefaultAtomicityMode)]
        public CacheAtomicityMode AtomicityMode { get; set; }

        /// <summary>
        /// Gets or sets cache write ordering mode.
        /// </summary>
        public CacheAtomicWriteOrderMode AtomicWriteOrderMode { get; set; }

        /// <summary>
        /// Gets or sets number of nodes used to back up single partition for 
        /// <see cref="Configuration.CacheMode.Partitioned"/> cache.
        /// </summary>
        [DefaultValue(DefaultBackups)]
        public int Backups { get; set; }

        /// <summary>
        /// Gets or sets default lock acquisition timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:00")]
        public TimeSpan LockTimeout { get; set; }

        /// <summary>
        /// Invalidation flag. If true, values will be invalidated (nullified) upon commit in near cache.
        /// </summary>
        [DefaultValue(DefaultInvalidate)]
        public bool Invalidate { get; set; }

        /// <summary>
        /// Gets or sets cache rebalance mode.
        /// </summary>
        [DefaultValue(DefaultRebalanceMode)]
        public CacheRebalanceMode RebalanceMode { get; set; }

        /// <summary>
        /// Gets or sets size (in number bytes) to be loaded within a single rebalance message.
        /// Rebalancing algorithm will split total data set on every node into multiple batches prior to sending data.
        /// </summary>
        [DefaultValue(DefaultRebalanceBatchSize)]
        public int RebalanceBatchSize { get; set; }

        /// <summary>
        /// Flag indicating whether Ignite should use swap storage by default.
        /// </summary>
        [DefaultValue(DefaultEnableSwap)]
        public bool EnableSwap { get; set; }

        /// <summary>
        /// Gets or sets maximum number of allowed concurrent asynchronous operations, 0 for unlimited.
        /// </summary>
        [DefaultValue(DefaultMaxConcurrentAsyncOperations)]
        public int MaxConcurrentAsyncOperations { get; set; }

        /// <summary>
        /// Flag indicating whether Ignite should use write-behind behaviour for the cache store.
        /// </summary>
        [DefaultValue(DefaultWriteBehindEnabled)]
        public bool WriteBehindEnabled { get; set; }

        /// <summary>
        /// Maximum size of the write-behind cache. If cache size exceeds this value, all cached items are flushed 
        /// to the cache store and write cache is cleared.
        /// </summary>
        [DefaultValue(DefaultWriteBehindFlushSize)]
        public int WriteBehindFlushSize { get; set; }

        /// <summary>
        /// Frequency with which write-behind cache is flushed to the cache store.
        /// This value defines the maximum time interval between object insertion/deletion from the cache
        /// at the moment when corresponding operation is applied to the cache store.
        /// <para/> 
        /// If this value is 0, then flush is performed according to the flush size.
        /// <para/>
        /// Note that you cannot set both
        /// <see cref="WriteBehindFlushSize"/> and <see cref="WriteBehindFlushFrequency"/> to 0.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan WriteBehindFlushFrequency { get; set; }

        /// <summary>
        /// Number of threads that will perform cache flushing. Cache flushing is performed when cache size exceeds 
        /// value defined by <see cref="WriteBehindFlushSize"/>, or flush interval defined by 
        /// <see cref="WriteBehindFlushFrequency"/> is elapsed.
        /// </summary>
        [DefaultValue(DefaultWriteBehindFlushThreadCount)]
        public int WriteBehindFlushThreadCount { get; set; }

        /// <summary>
        /// Maximum batch size for write-behind cache store operations. 
        /// Store operations (get or remove) are combined in a batch of this size to be passed to 
        /// <see cref="ICacheStore.WriteAll"/> or <see cref="ICacheStore.DeleteAll"/> methods. 
        /// </summary>
        [DefaultValue(DefaultWriteBehindBatchSize)]
        public int WriteBehindBatchSize { get; set; }

        /// <summary>
        /// Gets or sets rebalance timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:10")]
        public TimeSpan RebalanceTimeout { get; set; }

        /// <summary>
        /// Gets or sets delay upon a node joining or leaving topology (or crash) 
        /// after which rebalancing should be started automatically. 
        /// Rebalancing should be delayed if you plan to restart nodes
        /// after they leave topology, or if you plan to start multiple nodes at once or one after another
        /// and don't want to repartition and rebalance until all nodes are started.
        /// </summary>
        public TimeSpan RebalanceDelay { get; set; }

        /// <summary>
        /// Time to wait between rebalance messages to avoid overloading of CPU or network.
        /// When rebalancing large data sets, the CPU or network can get over-consumed with rebalancing messages,
        /// which consecutively may slow down the application performance. This parameter helps tune 
        /// the amount of time to wait between rebalance messages to make sure that rebalancing process
        /// does not have any negative performance impact. Note that application will continue to work
        /// properly while rebalancing is still in progress.
        /// <para/>
        /// Value of 0 means that throttling is disabled.
        /// </summary>
        public TimeSpan RebalanceThrottle { get; set; }

        /// <summary>
        /// Gets or sets maximum amount of memory available to off-heap storage. Possible values are
        /// -1 means that off-heap storage is disabled. 0 means that Ignite will not limit off-heap storage 
        /// (it's up to user to properly add and remove entries from cache to ensure that off-heap storage 
        /// does not grow indefinitely.
        /// Any positive value specifies the limit of off-heap storage in bytes.
        /// </summary>
        [DefaultValue(DefaultOffHeapMaxMemory)]
        public long OffHeapMaxMemory { get; set; }

        /// <summary>
        /// Gets or sets memory mode for cache.
        /// </summary>
        [DefaultValue(DefaultMemoryMode)]
        public CacheMemoryMode MemoryMode { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether data can be read from backup.
        /// </summary>
        [DefaultValue(DefaultReadFromBackup)]
        public bool ReadFromBackup { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether copy of the value stored in cache should be created
        /// for cache operation implying return value. 
        /// </summary>
        [DefaultValue(DefaultCopyOnRead)]
        public bool CopyOnRead { get; set; }

        /// <summary>
        /// Gets or sets the timeout after which long query warning will be printed.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:03")]
        public TimeSpan LongQueryWarningTimeout { get; set; }

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
        [DefaultValue(DefaultSqlOnheapRowCacheSize)]
        public int SqlOnheapRowCacheSize { get; set; }

        /// <summary>
        /// Gets or sets the factory for underlying persistent storage for read-through and write-through operations.
        /// <para />
        /// See <see cref="ReadThrough"/> and <see cref="WriteThrough"/> properties to enable read-through and 
        /// write-through behavior so that cache store is invoked on get and/or put operations.
        /// <para />
        /// If both <see cref="ReadThrough"/> and <see cref="WriteThrough"/> are <code>false</code>, cache store 
        /// will be invoked only on <see cref="ICache{TK,TV}.LoadCache"/> calls.
        /// </summary>
        public IFactory<ICacheStore> CacheStoreFactory { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether read-through should be enabled for cache operations.
        /// <para />
        /// When in read-through mode, cache misses that occur due to cache entries not existing 
        /// as a result of performing a "get" operations will appropriately cause the 
        /// configured <see cref="ICacheStore"/> (see <see cref="CacheStoreFactory"/>) to be invoked.
        /// </summary>
        [DefaultValue(DefaultReadThrough)]
        public bool ReadThrough { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether write-through should be enabled for cache operations.
        /// <para />
        /// When in "write-through" mode, cache updates that occur as a result of performing "put" operations
        /// will appropriately cause the configured 
        /// <see cref="ICacheStore"/> (see <see cref="CacheStoreFactory"/>) to be invoked.
        /// </summary>
        [DefaultValue(DefaultWriteThrough)]
        public bool WriteThrough { get; set; }

        /// <summary>
        /// Gets or sets the query entity configuration.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryEntity> QueryEntities { get; set; }

        /// <summary>
        /// Gets or sets the near cache configuration.
        /// </summary>
        public NearCacheConfiguration NearConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the eviction policy.
        /// Null value means disabled evictions.
        /// </summary>
        public IEvictionPolicy EvictionPolicy { get; set; }

        /// <summary>
        /// Gets or sets the affinity function to provide mapping from keys to nodes.
        /// <para />
        /// Predefined implementations: 
        /// <see cref="RendezvousAffinityFunction"/>, <see cref="FairAffinityFunction"/>.
        /// </summary>
        public IAffinityFunction AffinityFunction { get; set; }
    }
}

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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Xml.Serialization;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Cache.Expiry;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Plugin.Cache;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Defines grid cache configuration.
    /// </summary>
    public class CacheConfiguration : IBinaryRawWriteAware<BinaryWriter>
    {
        /// <summary> Default size of rebalance thread pool. </summary>
        public const int DefaultRebalanceThreadPoolSize = 4;

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

        /// <summary> Default cache size to use with eviction policy. </summary>
        public const int DefaultCacheSize = 100000;

        /// <summary> Default value for 'invalidate' flag that indicates if this is invalidation-based cache. </summary>
        public const bool DefaultInvalidate = false;

        /// <summary> Default rebalance mode for distributed cache. </summary>
        public const CacheRebalanceMode DefaultRebalanceMode = CacheRebalanceMode.Async;

        /// <summary> Default rebalance batch size in bytes. </summary>
        public const int DefaultRebalanceBatchSize = 512*1024; // 512K

        /// <summary> Default value for <see cref="WriteSynchronizationMode"/> property.</summary>
        public const CacheWriteSynchronizationMode DefaultWriteSynchronizationMode = 
            CacheWriteSynchronizationMode.PrimarySync;

        /// <summary> Default value for eager ttl flag. </summary>
        public const bool DefaultEagerTtl = true;

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

        /// <summary> Default value for 'readFromBackup' flag. </summary>
        public const bool DefaultReadFromBackup = true;

        /// <summary> Default timeout after which long query warning will be printed. </summary>
        public static readonly TimeSpan DefaultLongQueryWarningTimeout = TimeSpan.FromMilliseconds(3000);

        /// <summary> Default value for keep portable in store behavior .</summary>
        [Obsolete("Use DefaultKeepBinaryInStore instead.")]
        public const bool DefaultKeepVinaryInStore = true;

        /// <summary> Default value for <see cref="KeepBinaryInStore"/> property.</summary>
        public const bool DefaultKeepBinaryInStore = false;

        /// <summary> Default value for 'copyOnRead' flag. </summary>
        public const bool DefaultCopyOnRead = true;

        /// <summary> Default value for read-through behavior. </summary>
        public const bool DefaultReadThrough = false;

        /// <summary> Default value for write-through behavior. </summary>
        public const bool DefaultWriteThrough = false;

        /// <summary> Default value for <see cref="WriteBehindCoalescing"/>. </summary>
        public const bool DefaultWriteBehindCoalescing = true;

        /// <summary> Default value for <see cref="PartitionLossPolicy"/>. </summary>
        public const PartitionLossPolicy DefaultPartitionLossPolicy = PartitionLossPolicy.Ignore;

        /// <summary> Default value for <see cref="SqlIndexMaxInlineSize"/>. </summary>
        public const int DefaultSqlIndexMaxInlineSize = -1;

        /// <summary> Default value for <see cref="StoreConcurrentLoadAllThreshold"/>. </summary>
        public const int DefaultStoreConcurrentLoadAllThreshold = 5;

        /// <summary> Default value for <see cref="RebalanceOrder"/>. </summary>
        public const int DefaultRebalanceOrder = 0;

        /// <summary> Default value for <see cref="RebalanceBatchesPrefetchCount"/>. </summary>
        public const long DefaultRebalanceBatchesPrefetchCount = 3;

        /// <summary> Default value for <see cref="MaxQueryIteratorsCount"/>. </summary>
        public const int DefaultMaxQueryIteratorsCount = 1024;

        /// <summary> Default value for <see cref="QueryDetailMetricsSize"/>. </summary>
        public const int DefaultQueryDetailMetricsSize = 0;

        /// <summary> Default value for <see cref="QueryParallelism"/>. </summary>
        public const int DefaultQueryParallelism = 1;

        /// <summary> Default value for <see cref="EncryptionEnabled"/>. </summary>
        public const bool DefaultEncryptionEnabled = false;

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
            WriteSynchronizationMode = DefaultWriteSynchronizationMode;
            EagerTtl = DefaultEagerTtl;
            Invalidate = DefaultInvalidate;
            KeepBinaryInStore = DefaultKeepBinaryInStore;
            LoadPreviousValue = DefaultLoadPreviousValue;
            LockTimeout = DefaultLockTimeout;
#pragma warning disable 618
            LongQueryWarningTimeout = DefaultLongQueryWarningTimeout;
#pragma warning restore 618
            MaxConcurrentAsyncOperations = DefaultMaxConcurrentAsyncOperations;
            ReadFromBackup = DefaultReadFromBackup;
            RebalanceBatchSize = DefaultRebalanceBatchSize;
            RebalanceMode = DefaultRebalanceMode;
            RebalanceThrottle = DefaultRebalanceThrottle;
            RebalanceTimeout = DefaultRebalanceTimeout;
            WriteBehindBatchSize = DefaultWriteBehindBatchSize;
            WriteBehindEnabled = DefaultWriteBehindEnabled;
            WriteBehindFlushFrequency = DefaultWriteBehindFlushFrequency;
            WriteBehindFlushSize = DefaultWriteBehindFlushSize;
            WriteBehindFlushThreadCount= DefaultWriteBehindFlushThreadCount;
            WriteBehindCoalescing = DefaultWriteBehindCoalescing;
            PartitionLossPolicy = DefaultPartitionLossPolicy;
            SqlIndexMaxInlineSize = DefaultSqlIndexMaxInlineSize;
            StoreConcurrentLoadAllThreshold = DefaultStoreConcurrentLoadAllThreshold;
            RebalanceOrder = DefaultRebalanceOrder;
            RebalanceBatchesPrefetchCount = DefaultRebalanceBatchesPrefetchCount;
            MaxQueryIteratorsCount = DefaultMaxQueryIteratorsCount;
            QueryParallelism = DefaultQueryParallelism;
            EncryptionEnabled = DefaultEncryptionEnabled;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class 
        /// and populates <see cref="QueryEntities"/> according to provided query types.
        /// This constructor is depricated, please use <see cref="CacheConfiguration(string, QueryEntity[])"/>
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <param name="queryTypes">
        /// Collection of types to be registered as query entities. These types should use 
        /// <see cref="QuerySqlFieldAttribute"/> to configure query fields and properties.
        /// </param>
        [Obsolete("This constructor is deprecated, please use CacheConfiguration(string, QueryEntity[]) instead.")]
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
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class,
        /// performing a deep copy of specified cache configuration.
        /// </summary>
        /// <param name="other">The other configuration to perfrom deep copy from.</param>
        public CacheConfiguration(CacheConfiguration other)
        {
            if (other != null)
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    other.Write(BinaryUtils.Marshaller.StartMarshal(stream));

                    stream.SynchronizeOutput();
                    stream.Seek(0, SeekOrigin.Begin);

                    Read(BinaryUtils.Marshaller.StartUnmarshal(stream));
                }

                CopyLocalProperties(other);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal CacheConfiguration(BinaryReader reader)
        {
            Read(reader);
        }

        /// <summary>
        /// Reads data into this instance from the specified reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        private void Read(BinaryReader reader)
        {
            // Make sure system marshaller is used.
            Debug.Assert(reader.Marshaller == BinaryUtils.Marshaller);

            AtomicityMode = (CacheAtomicityMode) reader.ReadInt();
            Backups = reader.ReadInt();
            CacheMode = (CacheMode) reader.ReadInt();
            CopyOnRead = reader.ReadBoolean();
            EagerTtl = reader.ReadBoolean();
            Invalidate = reader.ReadBoolean();
            KeepBinaryInStore = reader.ReadBoolean();
            LoadPreviousValue = reader.ReadBoolean();
            LockTimeout = reader.ReadLongAsTimespan();
#pragma warning disable 618
            LongQueryWarningTimeout = reader.ReadLongAsTimespan();
#pragma warning restore 618
            MaxConcurrentAsyncOperations = reader.ReadInt();
            Name = reader.ReadString();
            ReadFromBackup = reader.ReadBoolean();
            RebalanceBatchSize = reader.ReadInt();
            RebalanceDelay = reader.ReadLongAsTimespan();
            RebalanceMode = (CacheRebalanceMode) reader.ReadInt();
            RebalanceThrottle = reader.ReadLongAsTimespan();
            RebalanceTimeout = reader.ReadLongAsTimespan();
            SqlEscapeAll = reader.ReadBoolean();
            WriteBehindBatchSize = reader.ReadInt();
            WriteBehindEnabled = reader.ReadBoolean();
            WriteBehindFlushFrequency = reader.ReadLongAsTimespan();
            WriteBehindFlushSize = reader.ReadInt();
            WriteBehindFlushThreadCount = reader.ReadInt();
            WriteBehindCoalescing = reader.ReadBoolean();
            WriteSynchronizationMode = (CacheWriteSynchronizationMode) reader.ReadInt();
            ReadThrough = reader.ReadBoolean();
            WriteThrough = reader.ReadBoolean();
            EnableStatistics = reader.ReadBoolean();
            DataRegionName = reader.ReadString();
            PartitionLossPolicy = (PartitionLossPolicy) reader.ReadInt();
            GroupName = reader.ReadString();
            CacheStoreFactory = reader.ReadObject<IFactory<ICacheStore>>();
            SqlIndexMaxInlineSize = reader.ReadInt();
            OnheapCacheEnabled = reader.ReadBoolean();
            StoreConcurrentLoadAllThreshold = reader.ReadInt();
            RebalanceOrder = reader.ReadInt();
            RebalanceBatchesPrefetchCount = reader.ReadLong();
            MaxQueryIteratorsCount = reader.ReadInt();
            QueryDetailMetricsSize = reader.ReadInt();
            QueryParallelism = reader.ReadInt();
            SqlSchema = reader.ReadString();
            EncryptionEnabled = reader.ReadBoolean();

            QueryEntities = reader.ReadCollectionRaw(r => new QueryEntity(r));

            NearConfiguration = reader.ReadBoolean() ? new NearCacheConfiguration(reader) : null;

            EvictionPolicy = EvictionPolicyBase.Read(reader);
            AffinityFunction = AffinityFunctionSerializer.Read(reader);
            ExpiryPolicyFactory = ExpiryPolicySerializer.ReadPolicyFactory(reader);

            KeyConfiguration = reader.ReadCollectionRaw(r => new CacheKeyConfiguration(r));
            
            if (reader.ReadBoolean())
            {
                PlatformCacheConfiguration = new PlatformCacheConfiguration(reader);
            }

            var count = reader.ReadInt();

            if (count > 0)
            {
                PluginConfigurations = new List<ICachePluginConfiguration>(count);
                for (int i = 0; i < count; i++)
                {
                    if (reader.ReadBoolean())
                    {
                        // FactoryId-based plugin: skip.
                        reader.ReadInt();  // Skip factory id.
                        var size = reader.ReadInt();
                        reader.Stream.Seek(size, SeekOrigin.Current);  // Skip custom data.
                    }
                    else
                    {
                        // Pure .NET plugin.
                        PluginConfigurations.Add(reader.ReadObject<ICachePluginConfiguration>());
                    }
                }
            }
        }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        void IBinaryRawWriteAware<BinaryWriter>.Write(BinaryWriter writer)
        {
            Write(writer);
        }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal void Write(BinaryWriter writer)
        {
            // Make sure system marshaller is used.
            Debug.Assert(writer.Marshaller == BinaryUtils.Marshaller);

            writer.WriteInt((int) AtomicityMode);
            writer.WriteInt(Backups);
            writer.WriteInt((int) CacheMode);
            writer.WriteBoolean(CopyOnRead);
            writer.WriteBoolean(EagerTtl);
            writer.WriteBoolean(Invalidate);
            writer.WriteBoolean(KeepBinaryInStore);
            writer.WriteBoolean(LoadPreviousValue);
            writer.WriteLong((long) LockTimeout.TotalMilliseconds);
#pragma warning disable 618
            writer.WriteLong((long) LongQueryWarningTimeout.TotalMilliseconds);
#pragma warning restore 618
            writer.WriteInt(MaxConcurrentAsyncOperations);
            writer.WriteString(Name);
            writer.WriteBoolean(ReadFromBackup);
            writer.WriteInt(RebalanceBatchSize);
            writer.WriteLong((long) RebalanceDelay.TotalMilliseconds);
            writer.WriteInt((int) RebalanceMode);
            writer.WriteLong((long) RebalanceThrottle.TotalMilliseconds);
            writer.WriteLong((long) RebalanceTimeout.TotalMilliseconds);
            writer.WriteBoolean(SqlEscapeAll);
            writer.WriteInt(WriteBehindBatchSize);
            writer.WriteBoolean(WriteBehindEnabled);
            writer.WriteLong((long) WriteBehindFlushFrequency.TotalMilliseconds);
            writer.WriteInt(WriteBehindFlushSize);
            writer.WriteInt(WriteBehindFlushThreadCount);
            writer.WriteBoolean(WriteBehindCoalescing);
            writer.WriteInt((int) WriteSynchronizationMode);
            writer.WriteBoolean(ReadThrough);
            writer.WriteBoolean(WriteThrough);
            writer.WriteBoolean(EnableStatistics);
            writer.WriteString(DataRegionName);
            writer.WriteInt((int) PartitionLossPolicy);
            writer.WriteString(GroupName);
            writer.WriteObject(CacheStoreFactory);
            writer.WriteInt(SqlIndexMaxInlineSize);
            writer.WriteBoolean(OnheapCacheEnabled);
            writer.WriteInt(StoreConcurrentLoadAllThreshold);
            writer.WriteInt(RebalanceOrder);
            writer.WriteLong(RebalanceBatchesPrefetchCount);
            writer.WriteInt(MaxQueryIteratorsCount);
            writer.WriteInt(QueryDetailMetricsSize);
            writer.WriteInt(QueryParallelism);
            writer.WriteString(SqlSchema);
            writer.WriteBoolean(EncryptionEnabled);

            writer.WriteCollectionRaw(QueryEntities);

            if (NearConfiguration != null)
            {
                writer.WriteBoolean(true);
                NearConfiguration.Write(writer);
            }
            else
                writer.WriteBoolean(false);

            EvictionPolicyBase.Write(writer, EvictionPolicy);
            AffinityFunctionSerializer.Write(writer, AffinityFunction);
            ExpiryPolicySerializer.WritePolicyFactory(writer, ExpiryPolicyFactory);

            writer.WriteCollectionRaw(KeyConfiguration);
            
            if (PlatformCacheConfiguration != null)
            {
                writer.WriteBoolean(true);
                PlatformCacheConfiguration.Write(writer);
            }
            else
            {
                writer.WriteBoolean(false);
            }

            if (PluginConfigurations != null)
            {
                writer.WriteInt(PluginConfigurations.Count);

                foreach (var cachePlugin in PluginConfigurations)
                {
                    if (cachePlugin == null)
                        throw new InvalidOperationException("Invalid cache configuration: " +
                                                            "ICachePluginConfiguration can't be null.");

                    if (cachePlugin.CachePluginConfigurationClosureFactoryId != null)
                    {
                        writer.WriteBoolean(true);
                        writer.WriteInt(cachePlugin.CachePluginConfigurationClosureFactoryId.Value);

                        int pos = writer.Stream.Position;
                        writer.WriteInt(0);  // Reserve size.

                        cachePlugin.WriteBinary(writer);

                        writer.Stream.WriteInt(pos, writer.Stream.Position - pos - 4);  // Write size.
                    }
                    else
                    {
                        writer.WriteBoolean(false);
                        writer.WriteObject(cachePlugin);
                    }
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }

        /// <summary>
        /// Copies the local properties (properties that are not written in Write method).
        /// </summary>
        internal void CopyLocalProperties(CacheConfiguration cfg)
        {
            Debug.Assert(cfg != null);

            PluginConfigurations = cfg.PluginConfigurations;

            if (QueryEntities != null && cfg.QueryEntities != null)
            {
                var entities = cfg.QueryEntities.Where(x => x != null).ToDictionary(x => GetQueryEntityKey(x), x => x);

                foreach (var entity in QueryEntities.Where(x => x != null))
                {
                    QueryEntity src;

                    if (entities.TryGetValue(GetQueryEntityKey(entity), out src))
                    {
                        entity.CopyLocalProperties(src);
                    }
                }
            }
        }

        /// <summary>
        /// Gets the query entity key.
        /// </summary>
        private static string GetQueryEntityKey(QueryEntity x)
        {
            return x.KeyTypeName + "^" + x.ValueTypeName;
        }

        /// <summary>
        /// Validates this instance and outputs information to the log, if necessary.
        /// </summary>
        internal void Validate(ILogger log)
        {
            Debug.Assert(log != null);

            var entities = QueryEntities;
            if (entities != null)
            {
                foreach (var entity in entities)
                    entity.Validate(log, string.Format("Validating cache configuration '{0}'", Name ?? ""));
            }
        }

        /// <summary>
        /// Gets or sets write synchronization mode. This mode controls whether the main        
        /// caller should wait for update on other nodes to complete or not.
        /// </summary>
        [DefaultValue(DefaultWriteSynchronizationMode)]
        public CacheWriteSynchronizationMode WriteSynchronizationMode { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether expired cache entries will be eagerly removed from cache. 
        /// When set to false, expired entries will be removed on next entry access.        
        /// </summary>
        [DefaultValue(DefaultEagerTtl)]
        public bool EagerTtl { get; set; }

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
        [DefaultValue(DefaultKeepBinaryInStore)]
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
        /// <see cref="ICacheStore{K, V}.WriteAll"/> or <see cref="ICacheStore{K, V}.DeleteAll"/> methods. 
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
        /// <para />
        /// This property is obsolete, use <see cref="IgniteConfiguration.LongQueryWarningTimeout"/> instead.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:03")]
        [Obsolete("Use IgniteConfiguration.LongQueryWarningTimeout instead.")]
        public TimeSpan LongQueryWarningTimeout { get; set; }

        /// <summary>
        /// If true all the SQL table and field names will be escaped with double quotes like 
        /// ({ "tableName"."fieldsName"}). This enforces case sensitivity for field names and
        /// also allows having special characters in table and field names.
        /// </summary>
        public bool SqlEscapeAll { get; set; }

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
        /// <see cref="RendezvousAffinityFunction"/>.
        /// </summary>
        public IAffinityFunction AffinityFunction { get; set; }

        /// <summary>
        /// Gets or sets the factory for <see cref="IExpiryPolicy"/> to be used for all cache operations,
        /// unless <see cref="ICache{TK,TV}.WithExpiryPolicy"/> is called.
        /// <para />
        /// Default is null, which means no expiration.
        /// </summary>
        public IFactory<IExpiryPolicy> ExpiryPolicyFactory { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether statistics gathering is enabled on a cache.
        /// These statistics can be retrieved via <see cref="ICache{TK,TV}.GetMetrics()"/>.
        /// </summary>
        public bool EnableStatistics { get; set; }

        /// <summary>
        /// Gets or sets the plugin configurations.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<ICachePluginConfiguration> PluginConfigurations { get; set; }

        /// <summary>
        /// Gets or sets the name of the <see cref="MemoryPolicyConfiguration"/> for this cache.
        /// See <see cref="IgniteConfiguration.MemoryConfiguration"/>.
        /// </summary>
        [Obsolete("Use DataRegionName.")]
        [XmlIgnore]
        public string MemoryPolicyName
        {
            get { return DataRegionName; }
            set { DataRegionName = value; }
        }

        /// <summary>
        /// Gets or sets the name of the data region, see <see cref="DataRegionConfiguration"/>.
        /// </summary>
        public string DataRegionName { get; set; }

        /// <summary>
        /// Gets or sets write coalescing flag for write-behind cache store operations.
        /// Store operations (get or remove) with the same key are combined or coalesced to single,
        /// resulting operation to reduce pressure to underlying cache store.
        /// </summary>
        [DefaultValue(DefaultWriteBehindCoalescing)]
        public bool WriteBehindCoalescing { get; set; }

        /// <summary>
        /// Gets or sets the partition loss policy. This policy defines how Ignite will react to
        /// a situation when all nodes for some partition leave the cluster.
        /// </summary>
        [DefaultValue(DefaultPartitionLossPolicy)]
        public PartitionLossPolicy PartitionLossPolicy { get; set; }

        /// <summary>
        /// Gets or sets the cache group name. Caches with the same group name share single underlying 'physical'
        /// cache (partition set), but are logically isolated. 
        /// <para />
        /// Since underlying cache is shared, the following configuration properties should be the same within group:
        /// <see cref="AffinityFunction"/>, <see cref="CacheMode"/>, <see cref="PartitionLossPolicy"/>,
        /// <see cref="DataRegionName"/>
        /// <para />
        /// Grouping caches reduces overall overhead, since internal data structures are shared.
        /// </summary>
        public string GroupName { get;set; }

        /// <summary>
        /// Gets or sets maximum inline size in bytes for sql indexes. See also <see cref="QueryIndex.InlineSize"/>.
        /// -1 for automatic.
        /// </summary>
        [DefaultValue(DefaultSqlIndexMaxInlineSize)]
        public int SqlIndexMaxInlineSize { get; set; }

        /// <summary>
        /// Gets or sets the key configuration.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<CacheKeyConfiguration> KeyConfiguration { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether on-heap cache is enabled for the off-heap based page memory.
        /// </summary>
        public bool OnheapCacheEnabled { get; set; }

        /// <summary>
        /// Gets or sets the threshold to use when multiple keys are being loaded from an underlying cache store
        /// (see <see cref="CacheStoreFactory"/>).
        /// 
        /// In the situation when several threads load the same or intersecting set of keys
        /// and the total number of keys to load is less or equal to this threshold then there will be no
        /// second call to the storage in order to load a key from thread A if the same key is already being
        /// loaded by thread B.
        ///
        /// The threshold should be controlled wisely. On the one hand if it's set to a big value then the
        /// interaction with a storage during the load of missing keys will be minimal.On the other hand the big
        /// value may result in significant performance degradation because it is needed to check
        /// for every key whether it's being loaded or not.
        /// </summary>
        [DefaultValue(DefaultStoreConcurrentLoadAllThreshold)]
        public int StoreConcurrentLoadAllThreshold { get; set; }

        /// <summary>
        /// Gets or sets the cache rebalance order. Caches with bigger RebalanceOrder are rebalanced later than caches
        /// with smaller RebalanceOrder.
        /// <para />
        /// Default is 0, which means unordered rebalance. All caches with RebalanceOrder=0 are rebalanced without any
        /// delay concurrently.
        /// <para />
        /// This parameter is applicable only for caches with <see cref="RebalanceMode"/> of
        /// <see cref="CacheRebalanceMode.Sync"/> and <see cref="CacheRebalanceMode.Async"/>.
        /// </summary>
        [DefaultValue(DefaultRebalanceOrder)]
        public int RebalanceOrder { get; set; }

        /// <summary>
        /// Gets or sets the rebalance batches prefetch count.
        /// <para />
        /// Source node can provide more than one batch at rebalance start to improve performance.
        /// Default is <see cref="DefaultRebalanceBatchesPrefetchCount"/>, minimum is 2.
        /// </summary>
        [DefaultValue(DefaultRebalanceBatchesPrefetchCount)]
        public long RebalanceBatchesPrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of active query iterators.
        /// </summary>
        [DefaultValue(DefaultMaxQueryIteratorsCount)]
        public int MaxQueryIteratorsCount { get; set; }

        /// <summary>
        /// Gets or sets the size of the query detail metrics to be stored in memory.
        /// <para />
        /// 0 means disabled metrics.
        /// </summary>
        [DefaultValue(DefaultQueryDetailMetricsSize)]
        public int QueryDetailMetricsSize { get; set; }

        /// <summary>
        /// Gets or sets the SQL schema.
        /// Non-quoted identifiers are not case sensitive. Quoted identifiers are case sensitive.
        /// <para />
        /// Quoted <see cref="Name"/> is used by default.
        /// </summary>
        public string SqlSchema { get; set; }

        /// <summary>
        /// Gets or sets the desired query parallelism within a single node.
        /// Query executor may or may not use this hint, depending on estimated query cost.
        /// <para />
        /// Default is <see cref="DefaultQueryParallelism"/>.
        /// </summary>
        [DefaultValue(DefaultQueryParallelism)]
        public int QueryParallelism { get; set; }

        /// <summary>
        /// Gets or sets encryption flag.
        /// Default is false.
        /// </summary>
        [DefaultValue(DefaultEncryptionEnabled)]
        public bool EncryptionEnabled { get; set; }

        /// <summary>
        /// Gets or sets platform cache configuration.
        /// More details: <see cref="PlatformCacheConfiguration"/>. 
        /// </summary>
        [IgniteExperimental]
        public PlatformCacheConfiguration PlatformCacheConfiguration { get; set; }
    }
}

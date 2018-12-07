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

namespace Apache.Ignite.Core.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Client.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client cache configuration.
    /// Same thing as <see cref="CacheConfiguration"/>, but with a subset of properties that can be accessed from
    /// Ignite thin client (see <see cref="IIgniteClient"/>).
    /// <para />
    /// Note that caches created from server nodes can be accessed from thin client, and vice versa.
    /// The only difference is that thin client can not read or write certain <see cref="CacheConfiguration"/>
    /// properties, so a separate class exists to make it clear which properties can be used.
    /// </summary>
    public class CacheClientConfiguration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        public CacheClientConfiguration() : this((string) null)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="name">Cache name.</param>
        public CacheClientConfiguration(string name)
        {
            Name = name;

            Backups = CacheConfiguration.DefaultBackups;
            AtomicityMode = CacheConfiguration.DefaultAtomicityMode;
            CacheMode = CacheConfiguration.DefaultCacheMode;
            CopyOnRead = CacheConfiguration.DefaultCopyOnRead;
            WriteSynchronizationMode = CacheConfiguration.DefaultWriteSynchronizationMode;
            EagerTtl = CacheConfiguration.DefaultEagerTtl;
            LockTimeout = CacheConfiguration.DefaultLockTimeout;
            MaxConcurrentAsyncOperations = CacheConfiguration.DefaultMaxConcurrentAsyncOperations;
            ReadFromBackup = CacheConfiguration.DefaultReadFromBackup;
            RebalanceBatchSize = CacheConfiguration.DefaultRebalanceBatchSize;
            RebalanceMode = CacheConfiguration.DefaultRebalanceMode;
            RebalanceThrottle = CacheConfiguration.DefaultRebalanceThrottle;
            RebalanceTimeout = CacheConfiguration.DefaultRebalanceTimeout;
            PartitionLossPolicy = CacheConfiguration.DefaultPartitionLossPolicy;
            SqlIndexMaxInlineSize = CacheConfiguration.DefaultSqlIndexMaxInlineSize;
            RebalanceOrder = CacheConfiguration.DefaultRebalanceOrder;
            RebalanceBatchesPrefetchCount = CacheConfiguration.DefaultRebalanceBatchesPrefetchCount;
            MaxQueryIteratorsCount = CacheConfiguration.DefaultMaxQueryIteratorsCount;
            QueryParallelism = CacheConfiguration.DefaultQueryParallelism;
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
        public CacheClientConfiguration(string name, params Type[] queryTypes) : this(name)
        {
            QueryEntities = queryTypes.Select(type => new QueryEntity { ValueType = type }).ToArray();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <param name="queryEntities">Query entities.</param>
        public CacheClientConfiguration(string name, params QueryEntity[] queryEntities) : this(name)
        {
            QueryEntities = queryEntities;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheConfiguration"/> class,
        /// performing a deep copy of specified cache configuration.
        /// </summary>
        /// <param name="other">The other configuration to perfrom deep copy from.</param>
        public CacheClientConfiguration(CacheClientConfiguration other)
        {
            if (other != null)
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    ClientCacheConfigurationSerializer.Write(stream, other, ClientSocket.CurrentProtocolVersion, true);

                    stream.SynchronizeOutput();
                    stream.Seek(0, SeekOrigin.Begin);

                    ClientCacheConfigurationSerializer.Read(stream, this, ClientSocket.CurrentProtocolVersion);
                }

                CopyLocalProperties(other);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClientConfiguration" /> class, copying properties from
        /// provided server cache configuration. See also <see cref="ToCacheConfiguration"/>.
        /// </summary>
        /// <param name="cacheConfiguration">Server cache configuration.</param>
        /// <param name="ignoreUnsupportedProperties">If set to <c>true</c>,
        /// ignores unsupported properties instead of throwing an exception.</param>
        public CacheClientConfiguration(CacheConfiguration cacheConfiguration, bool ignoreUnsupportedProperties)
        {
            IgniteArgumentCheck.NotNull(cacheConfiguration, "cacheConfiguration");

            ClientCacheConfigurationSerializer.Copy(cacheConfiguration, this, ignoreUnsupportedProperties);
        }

        /// <summary>
        /// Converts this instance to full <see cref="CacheConfiguration"/>.
        /// </summary>
        public CacheConfiguration ToCacheConfiguration()
        {
            var cfg = new CacheConfiguration();

            ClientCacheConfigurationSerializer.Copy(this, cfg);

            return cfg;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClientConfiguration"/> class.
        /// </summary>
        internal CacheClientConfiguration(IBinaryStream stream, ClientProtocolVersion srvVer)
        {
            Debug.Assert(stream != null);

            ClientCacheConfigurationSerializer.Read(stream, this, srvVer);
        }

        /// <summary>
        /// Copies the local properties (properties that are not written in Write method).
        /// </summary>
        private void CopyLocalProperties(CacheClientConfiguration cfg)
        {
            Debug.Assert(cfg != null);

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
        /// Gets or sets the cache name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets write synchronization mode. This mode controls whether the main        
        /// caller should wait for update on other nodes to complete or not.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultWriteSynchronizationMode)]
        public CacheWriteSynchronizationMode WriteSynchronizationMode { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether expired cache entries will be eagerly removed from cache. 
        /// When set to false, expired entries will be removed on next entry access.        
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultEagerTtl)]
        public bool EagerTtl { get; set; }

        /// <summary>
        /// Gets or sets caching mode to use.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultCacheMode)]
        public CacheMode CacheMode { get; set; }

        /// <summary>
        /// Gets or sets cache atomicity mode.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultAtomicityMode)]
        public CacheAtomicityMode AtomicityMode { get; set; }

        /// <summary>
        /// Gets or sets number of nodes used to back up single partition for 
        /// <see cref="Core.Cache.Configuration.CacheMode.Partitioned"/> cache.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultBackups)]
        public int Backups { get; set; }

        /// <summary>
        /// Gets or sets default lock acquisition timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:00")]
        public TimeSpan LockTimeout { get; set; }

        /// <summary>
        /// Gets or sets cache rebalance mode.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultRebalanceMode)]
        public CacheRebalanceMode RebalanceMode { get; set; }

        /// <summary>
        /// Gets or sets size (in number bytes) to be loaded within a single rebalance message.
        /// Rebalancing algorithm will split total data set on every node into multiple batches prior to sending data.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultRebalanceBatchSize)]
        public int RebalanceBatchSize { get; set; }

        /// <summary>
        /// Gets or sets maximum number of allowed concurrent asynchronous operations, 0 for unlimited.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultMaxConcurrentAsyncOperations)]
        public int MaxConcurrentAsyncOperations { get; set; }

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
        [DefaultValue(CacheConfiguration.DefaultReadFromBackup)]
        public bool ReadFromBackup { get; set; }

        /// <summary>
        /// Gets or sets flag indicating whether copy of the value stored in cache should be created
        /// for cache operation implying return value. 
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultCopyOnRead)]
        public bool CopyOnRead { get; set; }

        /// <summary>
        /// If true all the SQL table and field names will be escaped with double quotes like 
        /// ({ "tableName"."fieldsName"}). This enforces case sensitivity for field names and
        /// also allows having special characters in table and field names.
        /// </summary>
        public bool SqlEscapeAll { get; set; }

        /// <summary>
        /// Gets or sets the query entity configuration.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryEntity> QueryEntities { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether statistics gathering is enabled on a cache.
        /// These statistics can be retrieved via <see cref="ICache{TK,TV}.GetMetrics()"/>.
        /// </summary>
        public bool EnableStatistics { get; set; }

        /// <summary>
        /// Gets or sets the name of the data region, see <see cref="DataRegionConfiguration"/>.
        /// </summary>
        public string DataRegionName { get; set; }

        /// <summary>
        /// Gets or sets the partition loss policy. This policy defines how Ignite will react to
        /// a situation when all nodes for some partition leave the cluster.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultPartitionLossPolicy)]
        public PartitionLossPolicy PartitionLossPolicy { get; set; }

        /// <summary>
        /// Gets or sets the cache group name. Caches with the same group name share single underlying 'physical'
        /// cache (partition set), but are logically isolated. 
        /// <para />
        /// Since underlying cache is shared, the following configuration properties should be the same within group:
        /// <see cref="CacheMode"/>, <see cref="PartitionLossPolicy"/>, <see cref="DataRegionName"/>.
        /// <para />
        /// Grouping caches reduces overall overhead, since internal data structures are shared.
        /// </summary>
        public string GroupName { get; set; }

        /// <summary>
        /// Gets or sets maximum inline size in bytes for sql indexes. See also <see cref="QueryIndex.InlineSize"/>.
        /// -1 for automatic.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultSqlIndexMaxInlineSize)]
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
        /// Gets or sets the cache rebalance order. Caches with bigger RebalanceOrder are rebalanced later than caches
        /// with smaller RebalanceOrder.
        /// <para />
        /// Default is 0, which means unordered rebalance. All caches with RebalanceOrder=0 are rebalanced without any
        /// delay concurrently.
        /// <para />
        /// This parameter is applicable only for caches with <see cref="RebalanceMode"/> of
        /// <see cref="CacheRebalanceMode.Sync"/> and <see cref="CacheRebalanceMode.Async"/>.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultRebalanceOrder)]
        public int RebalanceOrder { get; set; }

        /// <summary>
        /// Gets or sets the rebalance batches prefetch count.
        /// <para />
        /// Source node can provide more than one batch at rebalance start to improve performance.
        /// Default is <see cref="CacheConfiguration.DefaultRebalanceBatchesPrefetchCount"/>, minimum is 2.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultRebalanceBatchesPrefetchCount)]
        public long RebalanceBatchesPrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of active query iterators.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultMaxQueryIteratorsCount)]
        public int MaxQueryIteratorsCount { get; set; }

        /// <summary>
        /// Gets or sets the size of the query detail metrics to be stored in memory.
        /// <para />
        /// 0 means disabled metrics.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultQueryDetailMetricsSize)]
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
        /// Default is <see cref="CacheConfiguration.DefaultQueryParallelism"/>.
        /// </summary>
        [DefaultValue(CacheConfiguration.DefaultQueryParallelism)]
        public int QueryParallelism { get; set; }
    }
}

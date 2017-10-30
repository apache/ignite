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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Writes and reads <see cref="CacheConfiguration"/> for thin client mode.
    /// <para />
    /// Thin client supports a subset of <see cref="CacheConfiguration"/> properties, so
    /// <see cref="CacheConfiguration.Read"/> is not suitable.
    /// </summary>
    internal static class ClientCacheConfigurationSerializer
    {
        /// <summary>
        /// Writes the specified config.
        /// </summary>
        public static void Write(IBinaryStream stream, CacheConfiguration cfg)
        {
            Debug.Assert(stream != null);
            Debug.Assert(cfg != null);

            // Configuration should be written with a system marshaller.
            var writer = BinaryUtils.Marshaller.StartMarshal(stream);

            writer.WriteInt((int) cfg.AtomicityMode);
            writer.WriteInt(cfg.Backups);
            writer.WriteInt((int) cfg.CacheMode);
            writer.WriteBoolean(cfg.CopyOnRead);
            writer.WriteString(cfg.DataRegionName);
            writer.WriteBoolean(cfg.EagerTtl);
            writer.WriteBoolean(cfg.EnableStatistics);
            writer.WriteString(cfg.GroupName);
            writer.WriteBoolean(cfg.Invalidate);
            writer.WriteTimeSpanAsLong(cfg.LockTimeout);
            writer.WriteInt(cfg.MaxConcurrentAsyncOperations);
            writer.WriteInt(cfg.MaxQueryIteratorsCount);
            writer.WriteString(cfg.Name);
            writer.WriteBoolean(cfg.OnheapCacheEnabled);
            writer.WriteInt((int) cfg.PartitionLossPolicy);
            writer.WriteInt(cfg.QueryDetailMetricsSize);
            writer.WriteInt(cfg.QueryParallelism);
            writer.WriteBoolean(cfg.ReadFromBackup);
            writer.WriteInt(cfg.RebalanceBatchSize);
            writer.WriteLong(cfg.RebalanceBatchesPrefetchCount);
            writer.WriteTimeSpanAsLong(cfg.RebalanceDelay);
            writer.WriteInt((int) cfg.RebalanceMode);
            writer.WriteInt(cfg.RebalanceOrder);
            writer.WriteTimeSpanAsLong(cfg.RebalanceThrottle);
            writer.WriteTimeSpanAsLong(cfg.RebalanceTimeout);
            writer.WriteBoolean(cfg.SqlEscapeAll);
            writer.WriteInt(cfg.SqlIndexMaxInlineSize);
            writer.WriteString(cfg.SqlSchema);
            writer.WriteInt((int) cfg.WriteSynchronizationMode);

            writer.WriteCollectionRaw(cfg.KeyConfiguration);
            writer.WriteCollectionRaw(cfg.QueryEntities);

            // Unsupported complex properties.
            ThrowUnsupportedIfNotDefault(cfg.AffinityFunction, "AffinityFunction");
            ThrowUnsupportedIfNotDefault(cfg.EvictionPolicy, "EvictionPolicy");
            ThrowUnsupportedIfNotDefault(cfg.ExpiryPolicyFactory, "ExpiryPolicyFactory");
            ThrowUnsupportedIfNotDefault(cfg.PluginConfigurations, "PluginConfigurations");
            ThrowUnsupportedIfNotDefault(cfg.CacheStoreFactory, "CacheStoreFactory");
            ThrowUnsupportedIfNotDefault(cfg.NearConfiguration, "NearConfiguration");
            
            // Unsupported store-related properties.
            ThrowUnsupportedIfNotDefault(cfg.KeepBinaryInStore, "KeepBinaryInStore");
            ThrowUnsupportedIfNotDefault(cfg.LoadPreviousValue, "LoadPreviousValue");
            ThrowUnsupportedIfNotDefault(cfg.ReadThrough, "ReadThrough");
            ThrowUnsupportedIfNotDefault(cfg.WriteThrough, "WriteThrough");
            ThrowUnsupportedIfNotDefault(cfg.StoreConcurrentLoadAllThreshold, "StoreConcurrentLoadAllThreshold", 
                CacheConfiguration.DefaultStoreConcurrentLoadAllThreshold);
            ThrowUnsupportedIfNotDefault(cfg.WriteBehindBatchSize, "WriteBehindBatchSize",
                CacheConfiguration.DefaultWriteBehindBatchSize);
            ThrowUnsupportedIfNotDefault(cfg.WriteBehindCoalescing, "WriteBehindCoalescing",
                CacheConfiguration.DefaultWriteBehindCoalescing);
            ThrowUnsupportedIfNotDefault(cfg.WriteBehindEnabled, "WriteBehindEnabled");
            ThrowUnsupportedIfNotDefault(cfg.WriteBehindFlushFrequency, "WriteBehindFlushFrequency",
                CacheConfiguration.DefaultWriteBehindFlushFrequency);
            ThrowUnsupportedIfNotDefault(cfg.WriteBehindFlushSize, "WriteBehindFlushSize",
                CacheConfiguration.DefaultWriteBehindFlushSize);
            ThrowUnsupportedIfNotDefault(cfg.WriteBehindFlushThreadCount, "WriteBehindFlushThreadCount",
                CacheConfiguration.DefaultWriteBehindFlushThreadCount);
        }

        /// <summary>
        /// Writes the specified config.
        /// </summary>
        public static void Write(IBinaryStream stream, CacheClientConfiguration cfg)
        {
            Debug.Assert(stream != null);
            Debug.Assert(cfg != null);

            // Configuration should be written with a system marshaller.
            var writer = BinaryUtils.Marshaller.StartMarshal(stream);

            writer.WriteInt((int)cfg.AtomicityMode);
            writer.WriteInt(cfg.Backups);
            writer.WriteInt((int)cfg.CacheMode);
            writer.WriteBoolean(cfg.CopyOnRead);
            writer.WriteString(cfg.DataRegionName);
            writer.WriteBoolean(cfg.EagerTtl);
            writer.WriteBoolean(cfg.EnableStatistics);
            writer.WriteString(cfg.GroupName);
            writer.WriteBoolean(cfg.Invalidate);
            writer.WriteTimeSpanAsLong(cfg.LockTimeout);
            writer.WriteInt(cfg.MaxConcurrentAsyncOperations);
            writer.WriteInt(cfg.MaxQueryIteratorsCount);
            writer.WriteString(cfg.Name);
            writer.WriteBoolean(cfg.OnheapCacheEnabled);
            writer.WriteInt((int)cfg.PartitionLossPolicy);
            writer.WriteInt(cfg.QueryDetailMetricsSize);
            writer.WriteInt(cfg.QueryParallelism);
            writer.WriteBoolean(cfg.ReadFromBackup);
            writer.WriteInt(cfg.RebalanceBatchSize);
            writer.WriteLong(cfg.RebalanceBatchesPrefetchCount);
            writer.WriteTimeSpanAsLong(cfg.RebalanceDelay);
            writer.WriteInt((int)cfg.RebalanceMode);
            writer.WriteInt(cfg.RebalanceOrder);
            writer.WriteTimeSpanAsLong(cfg.RebalanceThrottle);
            writer.WriteTimeSpanAsLong(cfg.RebalanceTimeout);
            writer.WriteBoolean(cfg.SqlEscapeAll);
            writer.WriteInt(cfg.SqlIndexMaxInlineSize);
            writer.WriteString(cfg.SqlSchema);
            writer.WriteInt((int)cfg.WriteSynchronizationMode);

            writer.WriteCollectionRaw(cfg.KeyConfiguration);
            writer.WriteCollectionRaw(cfg.QueryEntities);
        }
        /// <summary>
        /// Reads the config.
        /// </summary>
        public static void Read(IBinaryStream stream, CacheClientConfiguration cfg)
        {
            Debug.Assert(stream != null);

            // Configuration should be read with system marshaller.
            var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

            cfg.AtomicityMode = (CacheAtomicityMode)reader.ReadInt();
            cfg.Backups = reader.ReadInt();
            cfg.CacheMode = (CacheMode)reader.ReadInt();
            cfg.CopyOnRead = reader.ReadBoolean();
            cfg.DataRegionName = reader.ReadString();
            cfg.EagerTtl = reader.ReadBoolean();
            cfg.EnableStatistics = reader.ReadBoolean();
            cfg.GroupName = reader.ReadString();
            cfg.Invalidate = reader.ReadBoolean();
            cfg.LockTimeout = reader.ReadLongAsTimespan();
            cfg.MaxConcurrentAsyncOperations = reader.ReadInt();
            cfg.MaxQueryIteratorsCount = reader.ReadInt();
            cfg.Name = reader.ReadString();
            cfg.OnheapCacheEnabled = reader.ReadBoolean();
            cfg.PartitionLossPolicy = (PartitionLossPolicy)reader.ReadInt();
            cfg.QueryDetailMetricsSize = reader.ReadInt();
            cfg.QueryParallelism = reader.ReadInt();
            cfg.ReadFromBackup = reader.ReadBoolean();
            cfg.RebalanceBatchSize = reader.ReadInt();
            cfg.RebalanceBatchesPrefetchCount = reader.ReadLong();
            cfg.RebalanceDelay = reader.ReadLongAsTimespan();
            cfg.RebalanceMode = (CacheRebalanceMode)reader.ReadInt();
            cfg.RebalanceOrder = reader.ReadInt();
            cfg.RebalanceThrottle = reader.ReadLongAsTimespan();
            cfg.RebalanceTimeout = reader.ReadLongAsTimespan();
            cfg.SqlEscapeAll = reader.ReadBoolean();
            cfg.SqlIndexMaxInlineSize = reader.ReadInt();
            cfg.SqlSchema = reader.ReadString();
            cfg.WriteSynchronizationMode = (CacheWriteSynchronizationMode)reader.ReadInt();
            cfg.KeyConfiguration = reader.ReadCollectionRaw(r => new CacheKeyConfiguration(r));
            cfg.QueryEntities = reader.ReadCollectionRaw(r => new QueryEntity(r));
        }

        /// <summary>
        /// Throws the unsupported exception if property is not default.
        /// </summary>
        // ReSharper disable ParameterOnlyUsedForPreconditionCheck.Local
        private static void ThrowUnsupportedIfNotDefault<T>(T obj, string propertyName, T defaultValue = default(T))
        {
            if (!Equals(obj, defaultValue))
            {
                throw new NotSupportedException(
                    string.Format("{0}.{1} property is not supported in thin client mode.",
                        typeof(CacheConfiguration).Name, propertyName));
            }
        }
    }
}

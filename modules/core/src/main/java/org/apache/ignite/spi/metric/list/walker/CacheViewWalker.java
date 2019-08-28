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

package org.apache.ignite.spi.metric.list.walker;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.CacheView;

/** */
public class CacheViewWalker implements MonitoringRowAttributeWalker<CacheView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "topologyValidator", String.class);
        v.accept(1, "atomicityMode", CacheAtomicityMode.class);
        v.accept(2, "affinity", String.class);
        v.accept(3, "nodeFilter", String.class);
        v.accept(4, "dataRegionName", String.class);
        v.accept(5, "partitionLossPolicy", PartitionLossPolicy.class);
        v.accept(6, "rebalanceMode", CacheRebalanceMode.class);
        v.acceptLong(7, "rebalanceDelay");
        v.acceptInt(8, "rebalanceOrder");
        v.accept(9, "backups", Integer.class);
        v.acceptInt(10, "groupId");
        v.accept(11, "groupName", String.class);
        v.accept(12, "cacheMode", CacheMode.class);
        v.accept(13, "cacheName", String.class);
        v.acceptBoolean(14, "nearCacheEnabled");
        v.acceptLong(15, "rebalanceTimeout");
        v.acceptInt(16, "sqlOnheapCacheMaxSize");
        v.acceptInt(17, "rebalanceBatchSize");
        v.acceptInt(18, "writeBehindFlushSize");
        v.acceptInt(19, "writeBehindBatchSize");
        v.acceptBoolean(20, "writeBehindCoalescing");
        v.acceptLong(21, "rebalanceThrottle");
        v.accept(22, "interceptor", String.class);
        v.accept(23, "sqlSchema", String.class);
        v.acceptBoolean(24, "isOnheapCacheEnabled");
        v.acceptBoolean(25, "isSqlOnheapCacheEnabled");
        v.acceptBoolean(26, "isEagerTtl");
        v.acceptBoolean(27, "isLoadPreviousValue");
        v.acceptBoolean(28, "isStoreKeepBinary");
        v.acceptBoolean(29, "isInvalidate");
        v.acceptBoolean(30, "isWriteBehindEnabled");
        v.acceptBoolean(31, "isReadFromBackup");
        v.acceptBoolean(32, "isCopyOnRead");
        v.acceptBoolean(33, "isSqlEscapeAll");
        v.acceptBoolean(34, "isReadThrough");
        v.acceptBoolean(35, "isWriteThrough");
        v.acceptBoolean(36, "isEventsDisabled");
        v.acceptBoolean(37, "isEncryptionEnabled");
        v.accept(38, "cacheLoaderFactory", String.class);
        v.accept(39, "cacheWriterFactory", String.class);
        v.accept(40, "expiryPolicyFactory", String.class);
        v.acceptBoolean(41, "isManagementEnabled");
        v.acceptBoolean(42, "isStatisticsEnabled");
        v.acceptLong(43, "rebalanceBatchesPrefetchCount");
        v.accept(44, "writeSynchronizationMode", CacheWriteSynchronizationMode.class);
        v.accept(45, "cacheId", Integer.class);
        v.accept(46, "cacheType", CacheType.class);
        v.accept(47, "affinityMapper", String.class);
        v.accept(48, "evictionFilter", String.class);
        v.accept(49, "evictionPolicyFactory", String.class);
        v.accept(50, "nearEvictionPolicyFactory", String.class);
        v.acceptLong(51, "nearStartSize");
        v.acceptLong(52, "defaultLockTimeout");
        v.accept(53, "cacheStoreFactory", String.class);
        v.acceptLong(54, "writeBehindFlushFrequency");
        v.acceptInt(55, "writeBehindFlushThreadCount");
        v.acceptInt(56, "maxConcurrentAsyncOperations");
        v.acceptInt(57, "sqlIndexMaxInlineSize");
        v.acceptInt(58, "queryDetailMetricsSize");
        v.acceptInt(59, "queryParallelism");
        v.acceptInt(60, "maxQueryIteratorsCount");
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(CacheView row, AttributeWithValueVisitor v) {
        v.accept(0, "topologyValidator", String.class, row.topologyValidator());
        v.accept(1, "atomicityMode", CacheAtomicityMode.class, row.atomicityMode());
        v.accept(2, "affinity", String.class, row.affinity());
        v.accept(3, "nodeFilter", String.class, row.nodeFilter());
        v.accept(4, "dataRegionName", String.class, row.dataRegionName());
        v.accept(5, "partitionLossPolicy", PartitionLossPolicy.class, row.partitionLossPolicy());
        v.accept(6, "rebalanceMode", CacheRebalanceMode.class, row.rebalanceMode());
        v.acceptLong(7, "rebalanceDelay", row.rebalanceDelay());
        v.acceptInt(8, "rebalanceOrder", row.rebalanceOrder());
        v.accept(9, "backups", Integer.class, row.backups());
        v.acceptInt(10, "groupId", row.groupId());
        v.accept(11, "groupName", String.class, row.groupName());
        v.accept(12, "cacheMode", CacheMode.class, row.cacheMode());
        v.accept(13, "cacheName", String.class, row.cacheName());
        v.acceptBoolean(14, "nearCacheEnabled", row.nearCacheEnabled());
        v.acceptLong(15, "rebalanceTimeout", row.rebalanceTimeout());
        v.acceptInt(16, "sqlOnheapCacheMaxSize", row.sqlOnheapCacheMaxSize());
        v.acceptInt(17, "rebalanceBatchSize", row.rebalanceBatchSize());
        v.acceptInt(18, "writeBehindFlushSize", row.writeBehindFlushSize());
        v.acceptInt(19, "writeBehindBatchSize", row.writeBehindBatchSize());
        v.acceptBoolean(20, "writeBehindCoalescing", row.writeBehindCoalescing());
        v.acceptLong(21, "rebalanceThrottle", row.rebalanceThrottle());
        v.accept(22, "interceptor", String.class, row.interceptor());
        v.accept(23, "sqlSchema", String.class, row.sqlSchema());
        v.acceptBoolean(24, "isOnheapCacheEnabled", row.isOnheapCacheEnabled());
        v.acceptBoolean(25, "isSqlOnheapCacheEnabled", row.isSqlOnheapCacheEnabled());
        v.acceptBoolean(26, "isEagerTtl", row.isEagerTtl());
        v.acceptBoolean(27, "isLoadPreviousValue", row.isLoadPreviousValue());
        v.acceptBoolean(28, "isStoreKeepBinary", row.isStoreKeepBinary());
        v.acceptBoolean(29, "isInvalidate", row.isInvalidate());
        v.acceptBoolean(30, "isWriteBehindEnabled", row.isWriteBehindEnabled());
        v.acceptBoolean(31, "isReadFromBackup", row.isReadFromBackup());
        v.acceptBoolean(32, "isCopyOnRead", row.isCopyOnRead());
        v.acceptBoolean(33, "isSqlEscapeAll", row.isSqlEscapeAll());
        v.acceptBoolean(34, "isReadThrough", row.isReadThrough());
        v.acceptBoolean(35, "isWriteThrough", row.isWriteThrough());
        v.acceptBoolean(36, "isEventsDisabled", row.isEventsDisabled());
        v.acceptBoolean(37, "isEncryptionEnabled", row.isEncryptionEnabled());
        v.accept(38, "cacheLoaderFactory", String.class, row.cacheLoaderFactory());
        v.accept(39, "cacheWriterFactory", String.class, row.cacheWriterFactory());
        v.accept(40, "expiryPolicyFactory", String.class, row.expiryPolicyFactory());
        v.acceptBoolean(41, "isManagementEnabled", row.isManagementEnabled());
        v.acceptBoolean(42, "isStatisticsEnabled", row.isStatisticsEnabled());
        v.acceptLong(43, "rebalanceBatchesPrefetchCount", row.rebalanceBatchesPrefetchCount());
        v.accept(44, "writeSynchronizationMode", CacheWriteSynchronizationMode.class, row.writeSynchronizationMode());
        v.accept(45, "cacheId", Integer.class, row.cacheId());
        v.accept(46, "cacheType", CacheType.class, row.cacheType());
        v.accept(47, "affinityMapper", String.class, row.affinityMapper());
        v.accept(48, "evictionFilter", String.class, row.evictionFilter());
        v.accept(49, "evictionPolicyFactory", String.class, row.evictionPolicyFactory());
        v.accept(50, "nearEvictionPolicyFactory", String.class, row.nearEvictionPolicyFactory());
        v.acceptLong(51, "nearStartSize", row.nearStartSize());
        v.acceptLong(52, "defaultLockTimeout", row.defaultLockTimeout());
        v.accept(53, "cacheStoreFactory", String.class, row.cacheStoreFactory());
        v.acceptLong(54, "writeBehindFlushFrequency", row.writeBehindFlushFrequency());
        v.acceptInt(55, "writeBehindFlushThreadCount", row.writeBehindFlushThreadCount());
        v.acceptInt(56, "maxConcurrentAsyncOperations", row.maxConcurrentAsyncOperations());
        v.acceptInt(57, "sqlIndexMaxInlineSize", row.sqlIndexMaxInlineSize());
        v.acceptInt(58, "queryDetailMetricsSize", row.queryDetailMetricsSize());
        v.acceptInt(59, "queryParallelism", row.queryParallelism());
        v.acceptInt(60, "maxQueryIteratorsCount", row.maxQueryIteratorsCount());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 61;
    }
}


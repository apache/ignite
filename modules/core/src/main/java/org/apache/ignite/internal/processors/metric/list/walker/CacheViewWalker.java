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

package org.apache.ignite.internal.processors.metric.list.walker;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.CacheView;

/** */
public class CacheViewWalker implements MonitoringRowAttributeWalker<CacheView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "affinity", String.class);
        v.accept(1, "affinityMapper", String.class);
        v.accept(2, "atomicityMode", CacheAtomicityMode.class);
        v.accept(3, "backups", Integer.class);
        v.accept(4, "cacheId", Integer.class);
        v.accept(5, "cacheLoaderFactory", String.class);
        v.accept(6, "cacheMode", CacheMode.class);
        v.accept(7, "cacheName", String.class);
        v.accept(8, "cacheStoreFactory", String.class);
        v.accept(9, "cacheType", CacheType.class);
        v.accept(10, "cacheWriterFactory", String.class);
        v.accept(11, "dataRegionName", String.class);
        v.acceptLong(12, "defaultLockTimeout");
        v.accept(13, "evictionFilter", String.class);
        v.accept(14, "evictionPolicyFactory", String.class);
        v.accept(15, "expiryPolicyFactory", String.class);
        v.acceptInt(16, "groupId");
        v.accept(17, "groupName", String.class);
        v.accept(18, "interceptor", String.class);
        v.acceptBoolean(19, "isCopyOnRead");
        v.acceptBoolean(20, "isEagerTtl");
        v.acceptBoolean(21, "isEncryptionEnabled");
        v.acceptBoolean(22, "isEventsDisabled");
        v.acceptBoolean(23, "isInvalidate");
        v.acceptBoolean(24, "isLoadPreviousValue");
        v.acceptBoolean(25, "isManagementEnabled");
        v.acceptBoolean(26, "isOnheapCacheEnabled");
        v.acceptBoolean(27, "isReadFromBackup");
        v.acceptBoolean(28, "isReadThrough");
        v.acceptBoolean(29, "isSqlEscapeAll");
        v.acceptBoolean(30, "isSqlOnheapCacheEnabled");
        v.acceptBoolean(31, "isStatisticsEnabled");
        v.acceptBoolean(32, "isStoreKeepBinary");
        v.acceptBoolean(33, "isWriteBehindEnabled");
        v.acceptBoolean(34, "isWriteThrough");
        v.acceptInt(35, "maxConcurrentAsyncOperations");
        v.acceptInt(36, "maxQueryIteratorsCount");
        v.acceptBoolean(37, "nearCacheEnabled");
        v.accept(38, "nearEvictionPolicyFactory", String.class);
        v.acceptLong(39, "nearStartSize");
        v.accept(40, "nodeFilter", String.class);
        v.accept(41, "partitionLossPolicy", PartitionLossPolicy.class);
        v.acceptInt(42, "queryDetailMetricsSize");
        v.acceptInt(43, "queryParallelism");
        v.acceptInt(44, "rebalanceBatchSize");
        v.acceptLong(45, "rebalanceBatchesPrefetchCount");
        v.acceptLong(46, "rebalanceDelay");
        v.accept(47, "rebalanceMode", CacheRebalanceMode.class);
        v.acceptInt(48, "rebalanceOrder");
        v.acceptLong(49, "rebalanceThrottle");
        v.acceptLong(50, "rebalanceTimeout");
        v.acceptInt(51, "sqlIndexMaxInlineSize");
        v.acceptInt(52, "sqlOnheapCacheMaxSize");
        v.accept(53, "sqlSchema", String.class);
        v.accept(54, "topologyValidator", String.class);
        v.acceptInt(55, "writeBehindBatchSize");
        v.acceptBoolean(56, "writeBehindCoalescing");
        v.acceptLong(57, "writeBehindFlushFrequency");
        v.acceptInt(58, "writeBehindFlushSize");
        v.acceptInt(59, "writeBehindFlushThreadCount");
        v.accept(60, "writeSynchronizationMode", CacheWriteSynchronizationMode.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(CacheView row, AttributeWithValueVisitor v) {
        v.accept(0, "affinity", String.class, row.affinity());
        v.accept(1, "affinityMapper", String.class, row.affinityMapper());
        v.accept(2, "atomicityMode", CacheAtomicityMode.class, row.atomicityMode());
        v.accept(3, "backups", Integer.class, row.backups());
        v.accept(4, "cacheId", Integer.class, row.cacheId());
        v.accept(5, "cacheLoaderFactory", String.class, row.cacheLoaderFactory());
        v.accept(6, "cacheMode", CacheMode.class, row.cacheMode());
        v.accept(7, "cacheName", String.class, row.cacheName());
        v.accept(8, "cacheStoreFactory", String.class, row.cacheStoreFactory());
        v.accept(9, "cacheType", CacheType.class, row.cacheType());
        v.accept(10, "cacheWriterFactory", String.class, row.cacheWriterFactory());
        v.accept(11, "dataRegionName", String.class, row.dataRegionName());
        v.acceptLong(12, "defaultLockTimeout", row.defaultLockTimeout());
        v.accept(13, "evictionFilter", String.class, row.evictionFilter());
        v.accept(14, "evictionPolicyFactory", String.class, row.evictionPolicyFactory());
        v.accept(15, "expiryPolicyFactory", String.class, row.expiryPolicyFactory());
        v.acceptInt(16, "groupId", row.groupId());
        v.accept(17, "groupName", String.class, row.groupName());
        v.accept(18, "interceptor", String.class, row.interceptor());
        v.acceptBoolean(19, "isCopyOnRead", row.isCopyOnRead());
        v.acceptBoolean(20, "isEagerTtl", row.isEagerTtl());
        v.acceptBoolean(21, "isEncryptionEnabled", row.isEncryptionEnabled());
        v.acceptBoolean(22, "isEventsDisabled", row.isEventsDisabled());
        v.acceptBoolean(23, "isInvalidate", row.isInvalidate());
        v.acceptBoolean(24, "isLoadPreviousValue", row.isLoadPreviousValue());
        v.acceptBoolean(25, "isManagementEnabled", row.isManagementEnabled());
        v.acceptBoolean(26, "isOnheapCacheEnabled", row.isOnheapCacheEnabled());
        v.acceptBoolean(27, "isReadFromBackup", row.isReadFromBackup());
        v.acceptBoolean(28, "isReadThrough", row.isReadThrough());
        v.acceptBoolean(29, "isSqlEscapeAll", row.isSqlEscapeAll());
        v.acceptBoolean(30, "isSqlOnheapCacheEnabled", row.isSqlOnheapCacheEnabled());
        v.acceptBoolean(31, "isStatisticsEnabled", row.isStatisticsEnabled());
        v.acceptBoolean(32, "isStoreKeepBinary", row.isStoreKeepBinary());
        v.acceptBoolean(33, "isWriteBehindEnabled", row.isWriteBehindEnabled());
        v.acceptBoolean(34, "isWriteThrough", row.isWriteThrough());
        v.acceptInt(35, "maxConcurrentAsyncOperations", row.maxConcurrentAsyncOperations());
        v.acceptInt(36, "maxQueryIteratorsCount", row.maxQueryIteratorsCount());
        v.acceptBoolean(37, "nearCacheEnabled", row.nearCacheEnabled());
        v.accept(38, "nearEvictionPolicyFactory", String.class, row.nearEvictionPolicyFactory());
        v.acceptLong(39, "nearStartSize", row.nearStartSize());
        v.accept(40, "nodeFilter", String.class, row.nodeFilter());
        v.accept(41, "partitionLossPolicy", PartitionLossPolicy.class, row.partitionLossPolicy());
        v.acceptInt(42, "queryDetailMetricsSize", row.queryDetailMetricsSize());
        v.acceptInt(43, "queryParallelism", row.queryParallelism());
        v.acceptInt(44, "rebalanceBatchSize", row.rebalanceBatchSize());
        v.acceptLong(45, "rebalanceBatchesPrefetchCount", row.rebalanceBatchesPrefetchCount());
        v.acceptLong(46, "rebalanceDelay", row.rebalanceDelay());
        v.accept(47, "rebalanceMode", CacheRebalanceMode.class, row.rebalanceMode());
        v.acceptInt(48, "rebalanceOrder", row.rebalanceOrder());
        v.acceptLong(49, "rebalanceThrottle", row.rebalanceThrottle());
        v.acceptLong(50, "rebalanceTimeout", row.rebalanceTimeout());
        v.acceptInt(51, "sqlIndexMaxInlineSize", row.sqlIndexMaxInlineSize());
        v.acceptInt(52, "sqlOnheapCacheMaxSize", row.sqlOnheapCacheMaxSize());
        v.accept(53, "sqlSchema", String.class, row.sqlSchema());
        v.accept(54, "topologyValidator", String.class, row.topologyValidator());
        v.acceptInt(55, "writeBehindBatchSize", row.writeBehindBatchSize());
        v.acceptBoolean(56, "writeBehindCoalescing", row.writeBehindCoalescing());
        v.acceptLong(57, "writeBehindFlushFrequency", row.writeBehindFlushFrequency());
        v.acceptInt(58, "writeBehindFlushSize", row.writeBehindFlushSize());
        v.acceptInt(59, "writeBehindFlushThreadCount", row.writeBehindFlushThreadCount());
        v.accept(60, "writeSynchronizationMode", CacheWriteSynchronizationMode.class, row.writeSynchronizationMode());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 61;
    }
}


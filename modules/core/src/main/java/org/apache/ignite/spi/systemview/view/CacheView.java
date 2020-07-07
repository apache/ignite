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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Cache representation for a {@link SystemView}.
 */
public class CacheView {
    /** Cache descriptor. */
    private DynamicCacheDescriptor cache;

    /**
     * @param cache Cache descriptor.
     */
    public CacheView(DynamicCacheDescriptor cache) {
        this.cache = cache;
    }

    /** @see DynamicCacheDescriptor#groupId() */
    public int cacheGroupId() {
        return cache.groupId();
    }

    /** @see CacheGroupDescriptor#cacheOrGroupName() */
    @Order(5)
    public String cacheGroupName() {
        return cache.groupDescriptor().cacheOrGroupName();
    }

    /** @see DynamicCacheDescriptor#cacheId() */
    @Order(1)
    public int cacheId() {
        return cache.cacheId();
    }

    /** @see CacheConfiguration#getName() */
    @Order
    public String cacheName() {
        return cache.cacheConfiguration().getName();
    }

    /** @see DynamicCacheDescriptor#cacheType() */
    @Order(2)
    public CacheType cacheType() {
        return cache.cacheType();
    }

    /** @see CacheConfiguration#getCacheMode() */
    @Order(3)
    public CacheMode cacheMode() {
        return cache.cacheConfiguration().getCacheMode();
    }

    /** @see CacheConfiguration#getAtomicityMode() */
    @Order(4)
    public CacheAtomicityMode atomicityMode() {
        return cache.cacheConfiguration().getAtomicityMode();
    }

    /** @see CacheConfiguration#isOnheapCacheEnabled() */
    public boolean isOnheapCacheEnabled() {
        return cache.cacheConfiguration().isOnheapCacheEnabled();
    }

    /** @see CacheConfiguration#isCopyOnRead() */
    public boolean isCopyOnRead() {
        return cache.cacheConfiguration().isCopyOnRead();
    }

    /** @see CacheConfiguration#isLoadPreviousValue() */
    public boolean isLoadPreviousValue() {
        return cache.cacheConfiguration().isLoadPreviousValue();
    }

    /** @see CacheConfiguration#isReadFromBackup() */
    public boolean isReadFromBackup() {
        return cache.cacheConfiguration().isReadFromBackup();
    }

    /** @see CacheConfiguration#getPartitionLossPolicy() */
    public PartitionLossPolicy partitionLossPolicy() {
        return cache.cacheConfiguration().getPartitionLossPolicy();
    }

    /** @see CacheConfiguration#getNodeFilter() */
    public String nodeFilter() {
        return CacheGroupView.nodeFilter(cache.cacheConfiguration());
    }

    /** @see CacheConfiguration#getTopologyValidator() */
    public String topologyValidator() {
        TopologyValidator validator = cache.cacheConfiguration().getTopologyValidator();

        return validator == null ? null : toStringSafe(validator);
    }

    /** @see CacheConfiguration#isEagerTtl() */
    public boolean isEagerTtl() {
        return cache.cacheConfiguration().isEagerTtl();
    }

    /** @see CacheConfiguration#getWriteSynchronizationMode() */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return cache.cacheConfiguration().getWriteSynchronizationMode();
    }

    /** @see CacheConfiguration#isInvalidate() */
    public boolean isInvalidate() {
        return cache.cacheConfiguration().isInvalidate();
    }

    /** @see CacheConfiguration#isEventsDisabled() */
    public boolean isEventsDisabled() {
        return cache.cacheConfiguration().isEventsDisabled();
    }

    /** @see CacheConfiguration#isStatisticsEnabled() */
    public boolean isStatisticsEnabled() {
        return cache.cacheConfiguration().isStatisticsEnabled();
    }

    /** @see CacheConfiguration#isManagementEnabled() */
    public boolean isManagementEnabled() {
        return cache.cacheConfiguration().isManagementEnabled();
    }

    /** @see CacheConfiguration#isEncryptionEnabled()  */
    public boolean isEncryptionEnabled() {
        return cache.cacheConfiguration().isEncryptionEnabled();
    }

    /** @see CacheConfiguration#getBackups() */
    public int backups() {
        return cache.cacheConfiguration().getBackups();
    }

    /** @see CacheConfiguration#getAffinity() */
    public String affinity() {
        AffinityFunction aff = cache.cacheConfiguration().getAffinity();

        return aff != null ? toStringSafe(aff) : null;
    }

    /** @see CacheConfiguration#getAffinityMapper() */
    public String affinityMapper() {
        AffinityKeyMapper affMap = cache.cacheConfiguration().getAffinityMapper();

        return affMap != null ? toStringSafe(affMap) : null;
    }

    /** @see CacheConfiguration#getRebalanceMode() */
    public CacheRebalanceMode rebalanceMode() {
        return cache.cacheConfiguration().getRebalanceMode();
    }

    /** @see CacheConfiguration#getRebalanceBatchSize() */
    public int rebalanceBatchSize() {
        return cache.cacheConfiguration().getRebalanceBatchSize();
    }

    /** @see CacheConfiguration#getRebalanceTimeout() */
    public long rebalanceTimeout() {
        return cache.cacheConfiguration().getRebalanceTimeout();
    }

    /** @see CacheConfiguration#getRebalanceDelay() */
    public long rebalanceDelay() {
        return cache.cacheConfiguration().getRebalanceDelay();
    }

    /** @see CacheConfiguration#getRebalanceThrottle() */
    public long rebalanceThrottle() {
        return cache.cacheConfiguration().getRebalanceThrottle();
    }

    /** @see CacheConfiguration#getRebalanceBatchesPrefetchCount() */
    public long rebalanceBatchesPrefetchCount() {
        return cache.cacheConfiguration().getRebalanceBatchesPrefetchCount();
    }

    /** @see CacheConfiguration#getRebalanceOrder() */
    public int rebalanceOrder() {
        return cache.cacheConfiguration().getRebalanceOrder();
    }

    /** @see CacheConfiguration#getEvictionFilter() */
    public String evictionFilter() {
        return toStringSafe(cache.cacheConfiguration().getEvictionFilter());
    }

    /** @see CacheConfiguration#getEvictionPolicyFactory() */
    public String evictionPolicyFactory() {
        return toStringSafe(cache.cacheConfiguration().getEvictionPolicyFactory());
    }

    /** @see CacheConfiguration#getNearConfiguration() */
    public boolean isNearCacheEnabled() {
        return cache.cacheConfiguration().getNearConfiguration() != null;
    }

    /** @see NearCacheConfiguration#getNearEvictionPolicyFactory() */
    public String nearCacheEvictionPolicyFactory() {
        if (cache.cacheConfiguration().getNearConfiguration() == null)
            return null;

        return toStringSafe(cache.cacheConfiguration().getNearConfiguration().getNearEvictionPolicyFactory());
    }

    /** @see NearCacheConfiguration#getNearStartSize() */
    public int nearCacheStartSize() {
        if (cache.cacheConfiguration().getNearConfiguration() == null)
            return 0;

        return cache.cacheConfiguration().getNearConfiguration().getNearStartSize();
    }

    /** @see CacheConfiguration#getDefaultLockTimeout() */
    public long defaultLockTimeout() {
        return cache.cacheConfiguration().getDefaultLockTimeout();
    }

    /** @see CacheConfiguration#getInterceptor() */
    public String interceptor() {
        return toStringSafe(cache.cacheConfiguration().getInterceptor());
    }

    /** @see CacheConfiguration#getCacheStoreFactory() */
    public String cacheStoreFactory() {
        return toStringSafe(cache.cacheConfiguration().getCacheStoreFactory());
    }

    /** @see CacheConfiguration#isStoreKeepBinary() */
    public boolean isStoreKeepBinary() {
        return cache.cacheConfiguration().isStoreKeepBinary();
    }

    /** @see CacheConfiguration#isReadThrough() */
    public boolean isReadThrough() {
        return cache.cacheConfiguration().isReadThrough();
    }

    /** @see CacheConfiguration#isWriteThrough() */
    public boolean isWriteThrough() {
        return cache.cacheConfiguration().isWriteThrough();
    }

    /** @see CacheConfiguration#isWriteBehindEnabled() */
    public boolean isWriteBehindEnabled() {
        return cache.cacheConfiguration().isWriteBehindEnabled();
    }

    /** @see CacheConfiguration#getWriteBehindCoalescing()  */
    public boolean writeBehindCoalescing() {
        return cache.cacheConfiguration().getWriteBehindCoalescing();
    }

    /** @see CacheConfiguration#getWriteBehindFlushSize() */
    public int writeBehindFlushSize() {
        return cache.cacheConfiguration().getWriteBehindFlushSize();
    }

    /** @see CacheConfiguration#getWriteBehindFlushFrequency() */
    public long writeBehindFlushFrequency() {
        return cache.cacheConfiguration().getWriteBehindFlushFrequency();
    }

    /** @see CacheConfiguration#getWriteBehindFlushThreadCount() */
    public int writeBehindFlushThreadCount() {
        return cache.cacheConfiguration().getWriteBehindFlushThreadCount();
    }

    /** @see CacheConfiguration#getWriteBehindBatchSize() */
    public int writeBehindBatchSize() {
        return cache.cacheConfiguration().getWriteBehindBatchSize();
    }

    /** @see CacheConfiguration#getMaxConcurrentAsyncOperations() */
    public int maxConcurrentAsyncOperations() {
        return cache.cacheConfiguration().getMaxConcurrentAsyncOperations();
    }

    /** @see CacheConfiguration#getCacheLoaderFactory() */
    public String cacheLoaderFactory() {
        return toStringSafe(cache.cacheConfiguration().getCacheLoaderFactory());
    }

    /** @see CacheConfiguration#getCacheWriterFactory() */
    public String cacheWriterFactory() {
        return toStringSafe(cache.cacheConfiguration().getCacheWriterFactory());
    }

    /** @see CacheConfiguration#getExpiryPolicyFactory() */
    public String expiryPolicyFactory() {
        return toStringSafe(cache.cacheConfiguration().getExpiryPolicyFactory());
    }

    /** @see CacheConfiguration#isSqlEscapeAll() */
    public boolean isSqlEscapeAll() {
        return cache.cacheConfiguration().isSqlEscapeAll();
    }

    /** @see CacheConfiguration#getSqlSchema() */
    public String sqlSchema() {
        return cache.cacheConfiguration().getSqlSchema();
    }

    /** @see CacheConfiguration#getSqlIndexMaxInlineSize() */
    public int sqlIndexMaxInlineSize() {
        return cache.cacheConfiguration().getSqlIndexMaxInlineSize();
    }

    /** @see CacheConfiguration#isSqlOnheapCacheEnabled() */
    public boolean isSqlOnheapCacheEnabled() {
        return cache.cacheConfiguration().isSqlOnheapCacheEnabled();
    }

    /** @see CacheConfiguration#getSqlOnheapCacheMaxSize() */
    public int sqlOnheapCacheMaxSize() {
        return cache.cacheConfiguration().getSqlOnheapCacheMaxSize();
    }

    /** @see CacheConfiguration#getQueryDetailMetricsSize() */
    public int queryDetailMetricsSize() {
        return cache.cacheConfiguration().getQueryDetailMetricsSize();
    }

    /** @see CacheConfiguration#getQueryParallelism() */
    public int queryParallelism() {
        return cache.cacheConfiguration().getQueryParallelism();
    }

    /** @see CacheConfiguration#getMaxQueryIteratorsCount() */
    public int maxQueryIteratorsCount() {
        return cache.cacheConfiguration().getMaxQueryIteratorsCount();
    }

    /** @see CacheConfiguration#getDataRegionName() */
    public String dataRegionName() {
        return cache.cacheConfiguration().getDataRegionName();
    }
}

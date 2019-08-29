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

package org.apache.ignite.spi.metric.list.view;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.spi.metric.list.MonitoringRow;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/** */
public class CacheView implements MonitoringRow<String> {
    /** */
    private DynamicCacheDescriptor cache;

    /**
     * @param cache Cache descriptor.
     */
    public CacheView(DynamicCacheDescriptor cache) {
        this.cache = cache;
    }

    /** */
    public int groupId() {
        return cache.groupId();
    }

    /** */
    public String groupName() {
        return cache.groupDescriptor().cacheOrGroupName();
    }

    /** */
    public Integer cacheId() {
        return cache.cacheId();
    }

    /** */
    public String cacheName() {
        return cache.cacheConfiguration().getName();
    }

    /** */
    public CacheType cacheType() {
        return cache.cacheType();
    }

    /** */
    public CacheMode cacheMode() {
        return cache.cacheConfiguration().getCacheMode();
    }

    /** */
    public CacheAtomicityMode atomicityMode() {
        return cache.cacheConfiguration().getAtomicityMode();
    }

    /** */
    public boolean isOnheapCacheEnabled() {
        return cache.cacheConfiguration().isOnheapCacheEnabled();
    }

    /** */
    public boolean isCopyOnRead() {
        return cache.cacheConfiguration().isCopyOnRead();
    }

    /** */
    public boolean isLoadPreviousValue() {
        return cache.cacheConfiguration().isLoadPreviousValue();
    }

    /** */
    public boolean isReadFromBackup() {
        return cache.cacheConfiguration().isReadFromBackup();
    }

    /** */
    public PartitionLossPolicy partitionLossPolicy() {
        return cache.cacheConfiguration().getPartitionLossPolicy();
    }

    /** */
    public String nodeFilter() {
        return CacheGroupView.nodeFilter(cache.cacheConfiguration());
    }

    /** */
    public String topologyValidator() {
        TopologyValidator validator = cache.cacheConfiguration().getTopologyValidator();

        return validator == null ? null : toStringSafe(validator);
    }

    /** */
    public boolean isEagerTtl() {
        return cache.cacheConfiguration().isEagerTtl();
    }

    /** */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return cache.cacheConfiguration().getWriteSynchronizationMode();
    }

    /** */
    public boolean isInvalidate() {
        return cache.cacheConfiguration().isInvalidate();
    }

    /** */
    public boolean isEventsDisabled() {
        return cache.cacheConfiguration().isEventsDisabled();
    }

    /** */
    public boolean isStatisticsEnabled() {
        return cache.cacheConfiguration().isStatisticsEnabled();
    }

    /** */
    public boolean isManagementEnabled() {
        return cache.cacheConfiguration().isManagementEnabled();
    }

    /** */
    public boolean isEncryptionEnabled() {
        return cache.cacheConfiguration().isEncryptionEnabled();
    }

    /** */
    public Integer backups() {
        return cache.cacheConfiguration().getCacheMode() == CacheMode.REPLICATED ?
            null : cache.cacheConfiguration().getBackups();
    }

    /** */
    public String affinity() {
        AffinityFunction aff = cache.cacheConfiguration().getAffinity();

        return aff != null ? toStringSafe(aff) : null;
    }

    /** */
    public String affinityMapper() {
        AffinityKeyMapper affMap = cache.cacheConfiguration().getAffinityMapper();

        return affMap != null ? toStringSafe(affMap) : null;
    }

    /** */
    public CacheRebalanceMode rebalanceMode() {
        return cache.cacheConfiguration().getRebalanceMode();
    }

    /** */
    public int rebalanceBatchSize() {
        return cache.cacheConfiguration().getRebalanceBatchSize();
    }

    /** */
    public long rebalanceTimeout() {
        return cache.cacheConfiguration().getRebalanceTimeout();
    }

    /** */
    public long rebalanceDelay() {
        return cache.cacheConfiguration().getRebalanceDelay();
    }

    /** */
    public long rebalanceThrottle() {
        return cache.cacheConfiguration().getRebalanceThrottle();
    }

    /** */
    public long rebalanceBatchesPrefetchCount() {
        return cache.cacheConfiguration().getRebalanceBatchesPrefetchCount();
    }

    /** */
    public int rebalanceOrder() {
        return cache.cacheConfiguration().getRebalanceOrder();
    }

    /** */
    public String evictionFilter() {
        return toStringSafe(cache.cacheConfiguration().getEvictionFilter());
    }

    /** */
    public String evictionPolicyFactory() {
        return toStringSafe(cache.cacheConfiguration().getEvictionPolicyFactory());
    }

    /** */
    public boolean nearCacheEnabled() {
        return cache.cacheConfiguration().getNearConfiguration() != null;
    }

    /** */
    public String nearEvictionPolicyFactory() {
        if (cache.cacheConfiguration().getNearConfiguration() == null)
            return null;

        return toStringSafe(cache.cacheConfiguration().getNearConfiguration().getNearEvictionPolicyFactory());
    }

    /** */
    public long nearStartSize() {
        if (cache.cacheConfiguration().getNearConfiguration() == null)
            return 0;

        return cache.cacheConfiguration().getNearConfiguration().getNearStartSize();
    }

    /** */
    public long defaultLockTimeout() {
        return cache.cacheConfiguration().getDefaultLockTimeout();
    }

    /** */
    public String interceptor() {
        return toStringSafe(cache.cacheConfiguration().getInterceptor());
    }

    /** */
    public String cacheStoreFactory() {
        return toStringSafe(cache.cacheConfiguration().getCacheStoreFactory());
    }

    /** */
    public boolean isStoreKeepBinary() {
        return cache.cacheConfiguration().isStoreKeepBinary();
    }

    /** */
    public boolean isReadThrough() {
        return cache.cacheConfiguration().isReadThrough();
    }

    /** */
    public boolean isWriteThrough() {
        return cache.cacheConfiguration().isWriteThrough();
    }

    /** */
    public boolean isWriteBehindEnabled() {
        return cache.cacheConfiguration().isWriteBehindEnabled();
    }

    /** */
    public boolean writeBehindCoalescing() {
        return cache.cacheConfiguration().getWriteBehindCoalescing();
    }

    /** */
    public int writeBehindFlushSize() {
        return cache.cacheConfiguration().getWriteBehindFlushSize();
    }

    /** */
    public long writeBehindFlushFrequency() {
        return cache.cacheConfiguration().getWriteBehindFlushFrequency();
    }

    /** */
    public int writeBehindFlushThreadCount() {
        return cache.cacheConfiguration().getWriteBehindFlushThreadCount();
    }

    /** */
    public int writeBehindBatchSize() {
        return cache.cacheConfiguration().getWriteBehindBatchSize();
    }

    /** */
    public int maxConcurrentAsyncOperations() {
        return cache.cacheConfiguration().getMaxConcurrentAsyncOperations();
    }

    /** */
    public String cacheLoaderFactory() {
        return toStringSafe(cache.cacheConfiguration().getCacheLoaderFactory());
    }

    /** */
    public String cacheWriterFactory() {
        return toStringSafe(cache.cacheConfiguration().getCacheWriterFactory());
    }

    /** */
    public String expiryPolicyFactory() {
        return toStringSafe(cache.cacheConfiguration().getExpiryPolicyFactory());
    }

    /** */
    public boolean isSqlEscapeAll() {
        return cache.cacheConfiguration().isSqlEscapeAll();
    }

    /** */
    public String sqlSchema() {
        return cache.cacheConfiguration().getSqlSchema();
    }

    /** */
    public int sqlIndexMaxInlineSize() {
        return cache.cacheConfiguration().getSqlIndexMaxInlineSize();
    }

    /** */
    public boolean isSqlOnheapCacheEnabled() {
        return cache.cacheConfiguration().isSqlOnheapCacheEnabled();
    }

    /** */
    public int sqlOnheapCacheMaxSize() {
        return cache.cacheConfiguration().getSqlOnheapCacheMaxSize();
    }

    /** */
    public int queryDetailMetricsSize() {
        return cache.cacheConfiguration().getQueryDetailMetricsSize();
    }

    /** */
    public int queryParallelism() {
        return cache.cacheConfiguration().getQueryParallelism();
    }

    /** */
    public int maxQueryIteratorsCount() {
        return cache.cacheConfiguration().getMaxQueryIteratorsCount();
    }

    /** */
    public String dataRegionName() {
        return cache.cacheConfiguration().getDataRegionName();
    }
}

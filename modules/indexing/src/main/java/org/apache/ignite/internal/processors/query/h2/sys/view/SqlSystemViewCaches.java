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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: caches.
 */
public class SqlSystemViewCaches extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewCaches(GridKernalContext ctx) {
        super("CACHES", "Ignite caches", ctx, "NAME",
            newColumn("NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_TYPE"),
            newColumn("GROUP_ID", Value.INT),
            newColumn("GROUP_NAME"),
            newColumn("CACHE_MODE"),
            newColumn("ATOMICITY_MODE"),
            newColumn("IS_ONHEAP_CACHE_ENABLED", Value.BOOLEAN),
            newColumn("IS_COPY_ON_READ", Value.BOOLEAN),
            newColumn("IS_LOAD_PREVIOUS_VALUE", Value.BOOLEAN),
            newColumn("IS_READ_FROM_BACKUP", Value.BOOLEAN),
            newColumn("PARTITION_LOSS_POLICY"),
            newColumn("NODE_FILTER"),
            newColumn("TOPOLOGY_VALIDATOR"),
            newColumn("IS_EAGER_TTL", Value.BOOLEAN),
            newColumn("WRITE_SYNCHRONIZATION_MODE"),
            newColumn("IS_INVALIDATE", Value.BOOLEAN),
            newColumn("IS_EVENTS_DISABLED", Value.BOOLEAN),
            newColumn("IS_STATISTICS_ENABLED", Value.BOOLEAN),
            newColumn("IS_MANAGEMENT_ENABLED", Value.BOOLEAN),
            newColumn("BACKUPS", Value.INT),
            newColumn("AFFINITY"),
            newColumn("AFFINITY_MAPPER"),
            newColumn("REBALANCE_MODE"),
            newColumn("REBALANCE_BATCH_SIZE", Value.INT),
            newColumn("REBALANCE_TIMEOUT", Value.LONG),
            newColumn("REBALANCE_DELAY", Value.LONG),
            newColumn("REBALANCE_THROTTLE", Value.LONG),
            newColumn("REBALANCE_BATCHES_PREFETCH_COUNT", Value.LONG),
            newColumn("REBALANCE_ORDER", Value.INT),
            newColumn("EVICTION_FILTER"),
            newColumn("EVICTION_POLICY_FACTORY"),
            newColumn("IS_NEAR_CACHE_ENABLED", Value.BOOLEAN),
            newColumn("NEAR_CACHE_EVICTION_POLICY_FACTORY"),
            newColumn("NEAR_CACHE_START_SIZE", Value.INT),
            newColumn("DEFAULT_LOCK_TIMEOUT", Value.LONG),
            newColumn("CACHE_INTERCEPTOR"),
            newColumn("CACHE_STORE_FACTORY"),
            newColumn("IS_STORE_KEEP_BINARY", Value.BOOLEAN),
            newColumn("IS_READ_THROUGH", Value.BOOLEAN),
            newColumn("IS_WRITE_THROUGH", Value.BOOLEAN),
            newColumn("IS_WRITE_BEHIND_ENABLED", Value.BOOLEAN),
            newColumn("WRITE_BEHIND_COALESCING", Value.BOOLEAN),
            newColumn("WRITE_BEHIND_FLUSH_SIZE", Value.INT),
            newColumn("WRITE_BEHIND_FLUSH_FREQUENCY", Value.LONG),
            newColumn("WRITE_BEHIND_FLUSH_THREAD_COUNT", Value.INT),
            newColumn("WRITE_BEHIND_FLUSH_BATCH_SIZE", Value.INT),
            newColumn("MAX_CONCURRENT_ASYNC_OPERATIONS", Value.INT),
            newColumn("CACHE_LOADER_FACTORY"),
            newColumn("CACHE_WRITER_FACTORY"),
            newColumn("EXPIRY_POLICY_FACTORY"),
            newColumn("IS_SQL_ESCAPE_ALL", Value.BOOLEAN),
            newColumn("SQL_SCHEMA"),
            newColumn("SQL_INDEX_MAX_INLINE_SIZE", Value.INT),
            newColumn("IS_SQL_ONHEAP_CACHE_ENABLED", Value.BOOLEAN),
            newColumn("SQL_ONHEAP_CACHE_MAX_SIZE", Value.INT),
            newColumn("QUERY_DETAILS_METRICS_SIZE", Value.INT),
            newColumn("QUERY_PARALLELISM", Value.INT),
            newColumn("MAX_QUERY_ITERATORS_COUNT", Value.INT),
            newColumn("DATA_REGION_NAME")
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("NAME", first, last);

        Collection<DynamicCacheDescriptor> caches;

        if (nameCond.isEquality()) {
            DynamicCacheDescriptor cache = ctx.cache().cacheDescriptor(nameCond.valueForEquality().getString());

            caches = cache == null ? Collections.emptySet() : Collections.singleton(cache);
        }
        else
            caches = ctx.cache().cacheDescriptors().values();

        AtomicLong rowKey = new AtomicLong();

        return F.iterator(caches,
            cache -> createRow(ses, rowKey.incrementAndGet(),
                cache.cacheName(),
                cache.cacheId(),
                cache.cacheType(),
                cache.groupId(),
                cache.groupDescriptor().groupName(),
                cache.cacheConfiguration().getCacheMode(),
                cache.cacheConfiguration().getAtomicityMode(),
                cache.cacheConfiguration().isOnheapCacheEnabled(),
                cache.cacheConfiguration().isCopyOnRead(),
                cache.cacheConfiguration().isLoadPreviousValue(),
                cache.cacheConfiguration().isReadFromBackup(),
                cache.cacheConfiguration().getPartitionLossPolicy(),
                cache.cacheConfiguration().getNodeFilter(),
                cache.cacheConfiguration().getTopologyValidator(),
                cache.cacheConfiguration().isEagerTtl(),
                cache.cacheConfiguration().getWriteSynchronizationMode(),
                cache.cacheConfiguration().isInvalidate(),
                cache.cacheConfiguration().isEventsDisabled(),
                cache.cacheConfiguration().isStatisticsEnabled(),
                cache.cacheConfiguration().isManagementEnabled(),
                cache.cacheConfiguration().getBackups(),
                cache.cacheConfiguration().getAffinity(),
                cache.cacheConfiguration().getAffinityMapper(),
                cache.cacheConfiguration().getRebalanceMode(),
                cache.cacheConfiguration().getRebalanceBatchSize(),
                cache.cacheConfiguration().getRebalanceTimeout(),
                cache.cacheConfiguration().getRebalanceDelay(),
                cache.cacheConfiguration().getRebalanceThrottle(),
                cache.cacheConfiguration().getRebalanceBatchesPrefetchCount(),
                cache.cacheConfiguration().getRebalanceOrder(),
                cache.cacheConfiguration().getEvictionFilter(),
                cache.cacheConfiguration().getEvictionPolicyFactory(),
                cache.cacheConfiguration().getNearConfiguration() != null,
                cache.cacheConfiguration().getNearConfiguration() != null ?
                    cache.cacheConfiguration().getNearConfiguration().getNearEvictionPolicyFactory() : null,
                cache.cacheConfiguration().getNearConfiguration() != null ?
                    cache.cacheConfiguration().getNearConfiguration().getNearStartSize() : null,
                cache.cacheConfiguration().getDefaultLockTimeout(),
                cache.cacheConfiguration().getInterceptor(),
                cache.cacheConfiguration().getCacheStoreFactory(),
                cache.cacheConfiguration().isStoreKeepBinary(),
                cache.cacheConfiguration().isReadThrough(),
                cache.cacheConfiguration().isWriteThrough(),
                cache.cacheConfiguration().isWriteBehindEnabled(),
                cache.cacheConfiguration().getWriteBehindCoalescing(),
                cache.cacheConfiguration().getWriteBehindFlushSize(),
                cache.cacheConfiguration().getWriteBehindFlushFrequency(),
                cache.cacheConfiguration().getWriteBehindFlushThreadCount(),
                cache.cacheConfiguration().getWriteBehindBatchSize(),
                cache.cacheConfiguration().getMaxConcurrentAsyncOperations(),
                cache.cacheConfiguration().getCacheLoaderFactory(),
                cache.cacheConfiguration().getCacheWriterFactory(),
                cache.cacheConfiguration().getExpiryPolicyFactory(),
                cache.cacheConfiguration().isSqlEscapeAll(),
                cache.cacheConfiguration().getSqlSchema(),
                cache.cacheConfiguration().getSqlIndexMaxInlineSize(),
                cache.cacheConfiguration().isSqlOnheapCacheEnabled(),
                cache.cacheConfiguration().getSqlOnheapCacheMaxSize(),
                cache.cacheConfiguration().getQueryDetailMetricsSize(),
                cache.cacheConfiguration().getQueryParallelism(),
                cache.cacheConfiguration().getMaxQueryIteratorsCount(),
                cache.cacheConfiguration().getDataRegionName()
            ), true);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.cache().cacheDescriptors().size();
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
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
        super("CACHES", "Ignite caches", ctx, "CACHE_NAME",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("CACHE_ID", Value.INT),
            newColumn("CACHE_NAME"),
            newColumn("CACHE_TYPE"),
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
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("CACHE_NAME", first, last);

        Collection<DynamicCacheDescriptor> caches;

        if (nameCond.isEquality()) {
            DynamicCacheDescriptor cache = ctx.cache().cacheDescriptor(nameCond.valueForEquality().getString());

            caches = cache == null ? Collections.emptySet() : Collections.singleton(cache);
        }
        else
            caches = ctx.cache().cacheDescriptors().values();

        return F.iterator(caches,
            cache -> {
                CacheConfiguration ccfg = cache.cacheConfiguration();

                return createRow(
                    ses,
                    cache.groupId(),
                    cache.groupDescriptor().cacheOrGroupName(),
                    cache.cacheId(),
                    cache.cacheName(),
                    cache.cacheType(),
                    ccfg.getCacheMode(),
                    ccfg.getAtomicityMode(),
                    ccfg.isOnheapCacheEnabled(),
                    ccfg.isCopyOnRead(),
                    ccfg.isLoadPreviousValue(),
                    ccfg.isReadFromBackup(),
                    ccfg.getPartitionLossPolicy(),
                    nodeFilter(ccfg),
                    toStringSafe(ccfg.getTopologyValidator()),
                    ccfg.isEagerTtl(),
                    ccfg.getWriteSynchronizationMode(),
                    ccfg.isInvalidate(),
                    ccfg.isEventsDisabled(),
                    ccfg.isStatisticsEnabled(),
                    ccfg.isManagementEnabled(),
                    ccfg.getCacheMode() == CacheMode.REPLICATED ? null : ccfg.getBackups(),
                    toStringSafe(ccfg.getAffinity()),
                    toStringSafe(ccfg.getAffinityMapper()),
                    ccfg.getRebalanceMode(),
                    ccfg.getRebalanceBatchSize(),
                    ccfg.getRebalanceTimeout(),
                    ccfg.getRebalanceDelay(),
                    ccfg.getRebalanceThrottle(),
                    ccfg.getRebalanceBatchesPrefetchCount(),
                    ccfg.getRebalanceOrder(),
                    toStringSafe(ccfg.getEvictionFilter()),
                    toStringSafe(ccfg.getEvictionPolicyFactory()),
                    ccfg.getNearConfiguration() != null,
                    ccfg.getNearConfiguration() != null ?
                        toStringSafe(ccfg.getNearConfiguration().getNearEvictionPolicyFactory()) : null,
                    ccfg.getNearConfiguration() != null ?
                        ccfg.getNearConfiguration().getNearStartSize() : null,
                    ccfg.getDefaultLockTimeout(),
                    toStringSafe(ccfg.getInterceptor()),
                    toStringSafe(ccfg.getCacheStoreFactory()),
                    ccfg.isStoreKeepBinary(),
                    ccfg.isReadThrough(),
                    ccfg.isWriteThrough(),
                    ccfg.isWriteBehindEnabled(),
                    ccfg.getWriteBehindCoalescing(),
                    ccfg.getWriteBehindFlushSize(),
                    ccfg.getWriteBehindFlushFrequency(),
                    ccfg.getWriteBehindFlushThreadCount(),
                    ccfg.getWriteBehindBatchSize(),
                    ccfg.getMaxConcurrentAsyncOperations(),
                    toStringSafe(ccfg.getCacheLoaderFactory()),
                    toStringSafe(ccfg.getCacheWriterFactory()),
                    toStringSafe(ccfg.getExpiryPolicyFactory()),
                    ccfg.isSqlEscapeAll(),
                    ccfg.getSqlSchema(),
                    ccfg.getSqlIndexMaxInlineSize(),
                    ccfg.isSqlOnheapCacheEnabled(),
                    ccfg.getSqlOnheapCacheMaxSize(),
                    ccfg.getQueryDetailMetricsSize(),
                    ccfg.getQueryParallelism(),
                    ccfg.getMaxQueryIteratorsCount(),
                    ccfg.getDataRegionName()
                );
            }, true);
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

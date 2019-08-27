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

import java.util.Collections;
import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.list.CacheView;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: caches.
 *
 * @deprecated Use monitoring list instead.
 */
@Deprecated
public class SqlSystemViewCaches extends SqlAbstractLocalSystemView {
    /** Caches monitoring list. */
    private final MonitoringList<String, CacheView> caches;

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

        this.caches = ctx.metric().list("caches", "Caches", CacheView.class);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("CACHE_NAME", first, last);

        Iterator<CacheView> res;

        if (nameCond.isEquality()) {
            CacheView cache = caches.get(nameCond.valueForEquality().getString());

            res = cache == null ? Collections.emptyIterator() : Collections.singleton(cache).iterator();
        }
        else
            res = caches.iterator();

        return F.iterator(res, c -> createRow(
            ses,
            c.groupId(),
            c.groupName(),
            c.cacheId(),
            c.cacheName(),
            c.cacheType(),
            c.cacheMode(),
            c.atomicityMode(),
            c.isOnheapCacheEnabled(),
            c.isCopyOnRead(),
            c.isLoadPreviousValue(),
            c.isReadFromBackup(),
            c.partitionLossPolicy(),
            c.nodeFilter(),
            c.topologyValidator(),
            c.isEagerTtl(),
            c.writeSynchronizationMode(),
            c.isInvalidate(),
            c.isEventsDisabled(),
            c.isStatisticsEnabled(),
            c.isManagementEnabled(),
            c.backups(),
            c.affinity(),
            c.affinityMapper(),
            c.rebalanceMode(),
            c.rebalanceBatchSize(),
            c.rebalanceTimeout(),
            c.rebalanceDelay(),
            c.rebalanceThrottle(),
            c.rebalanceBatchesPrefetchCount(),
            c.rebalanceOrder(),
            c.evictionFilter(),
            c.evictionPolicyFactory(),
            c.nearCacheEnabled(),
            c.nearEvictionPolicyFactory(),
            c.nearStartSize(),
            c.defaultLockTimeout(),
            c.interceptor(),
            c.cacheStoreFactory(),
            c.isStoreKeepBinary(),
            c.isReadThrough(),
            c.isWriteThrough(),
            c.isWriteBehindEnabled(),
            c.writeBehindCoalescing(),
            c.writeBehindFlushSize(),
            c.writeBehindFlushFrequency(),
            c.writeBehindFlushThreadCount(),
            c.writeBehindBatchSize(),
            c.maxConcurrentAsyncOperations(),
            c.cacheLoaderFactory(),
            c.cacheWriterFactory(),
            c.expiryPolicyFactory(),
            c.isSqlEscapeAll(),
            c.sqlSchema(),
            c.sqlIndexMaxInlineSize(),
            c.isSqlOnheapCacheEnabled(),
            c.sqlOnheapCacheMaxSize(),
            c.queryDetailMetricsSize(),
            c.queryParallelism(),
            c.maxQueryIteratorsCount(),
            c.dataRegionName()
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

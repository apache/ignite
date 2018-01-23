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

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: cache metrics.
 */
public class GridH2SysViewImplCacheMetrics extends GridH2SysView {
    /** Is aggregated cluster metrics. */
    private final boolean clusterMetrics;

    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplCacheMetrics(GridKernalContext ctx, boolean clusterMetrics) {
        super("CACHE_" + (clusterMetrics ? "CLUSTER" : "NODE") + "_METRICS",
            "Ignite cache " + (clusterMetrics ? "cluster" : "node") + " metrics", ctx, "NODE_ID,CACHE_NAME",
            newColumn("CACHE_NAME"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("CACHE_GETS", Value.LONG),
            newColumn("CACHE_PUTS", Value.LONG),
            newColumn("CACHE_REMOVALS", Value.LONG),
            newColumn("CACHE_EVICTIONS", Value.LONG),
            newColumn("CACHE_HITS", Value.LONG),
            newColumn("CACHE_HITS_PRC", Value.FLOAT),
            newColumn("CACHE_MISSES", Value.LONG),
            newColumn("CACHE_MISSES_PRC", Value.FLOAT),
            newColumn("CACHE_TX_COMMITS", Value.LONG),
            newColumn("CACHE_TX_ROLLBACKS", Value.LONG),
            newColumn("AVG_GET_TIME_NS", Value.FLOAT),
            newColumn("AVG_PUT_TIME_NS", Value.FLOAT),
            newColumn("AVG_REMOVE_TIME_NS", Value.FLOAT),
            newColumn("AVG_TX_COMMIT_TIME_NS", Value.FLOAT),
            newColumn("AVG_TX_ROLLBACK_TIME_NS", Value.FLOAT),
            newColumn("OFFHEAP_GETS", Value.LONG),
            newColumn("OFFHEAP_PUTS", Value.LONG),
            newColumn("OFFHEAP_REMOVALS", Value.LONG),
            newColumn("OFFHEAP_EVICTIONS", Value.LONG),
            newColumn("OFFHEAP_HITS", Value.LONG),
            newColumn("OFFHEAP_HITS_PRC", Value.FLOAT),
            newColumn("OFFHEAP_MISSES", Value.LONG),
            newColumn("OFFHEAP_MISSES_PRC", Value.FLOAT),
            newColumn("HEAP_ENTRIES_CNT", Value.LONG),
            newColumn("OFFHEAP_ENTRIES_CNT", Value.LONG),
            newColumn("OFFHEAP_PRI_ENTRIES_CNT", Value.LONG),
            newColumn("OFFHEAP_BAK_ENTRIES_CNT", Value.LONG),
            newColumn("OFFHEAP_ALLOC_SIZE", Value.LONG),
            newColumn("SIZE", Value.LONG),
            newColumn("IS_EMPTY", Value.BOOLEAN),
            newColumn("DHT_EVICT_Q_SIZE", Value.INT),
            newColumn("TX_THREAD_MAP_SIZE", Value.INT),
            newColumn("TX_XID_MAP_SIZE", Value.INT),
            newColumn("TX_DHT_COMMIT_Q_SIZE", Value.INT),
            newColumn("TX_DHT_PREPARE_Q_SIZE", Value.INT),
            newColumn("TX_START_VERSIONS_SIZE", Value.INT),
            newColumn("TX_COMMITED_VERSIONS_SIZE", Value.INT),
            newColumn("TX_ROLLEDBACK_VERSIONS_SIZE", Value.INT),
            newColumn("WRITE_BEHIND_ENABLED", Value.BOOLEAN),
            newColumn("WRITE_BEHIND_FLUSH_SIZE", Value.INT),
            newColumn("WRITE_BEHIND_FLUSH_THREAD_CNT", Value.INT),
            newColumn("WRITE_BEHIND_FLUSH_FREQ", Value.LONG),
            newColumn("WRITE_BEHIND_STORE_BATCH_SIZE", Value.INT),
            newColumn("WRITE_BEHIND_TOTAL_CRIT_OVF_CNT", Value.INT),
            newColumn("WRITE_BEHIND_CRIT_OVF_CNT", Value.INT),
            newColumn("WRITE_BEHIND_BUFFER_SIZE", Value.INT),
            newColumn("KEY_TYPE"),
            newColumn("VALUE_TYPE"),
            newColumn("IS_STORE_BY_VALUE", Value.BOOLEAN),
            newColumn("TOTAL_PART_CNT", Value.INT),
            newColumn("REBALANCING_PART_CNT", Value.INT),
            newColumn("KEYS_TO_REBALANCE_LEFT", Value.LONG),
            newColumn("REBALANCING_KEYS_RATE", Value.LONG),
            newColumn("REBALANCING_BYTES_RATE", Value.LONG),
            newColumn("REBALANCING_START_TIME", Value.TIMESTAMP),
            newColumn("EST_REBALANCING_FINISH_TIME", Value.TIMESTAMP),
            newColumn("IS_STATISTICS_ENABLED", Value.BOOLEAN),
            newColumn("IS_MANAGEMENT_ENABLED", Value.BOOLEAN),
            newColumn("IS_READ_THROUGH", Value.BOOLEAN),
            newColumn("IS_WRITE_THROUGH", Value.BOOLEAN),
            newColumn("IS_VALID_FOR_READING", Value.BOOLEAN),
            newColumn("IS_VALID_FOR_WRITING", Value.BOOLEAN)
        );

        this.clusterMetrics = clusterMetrics;
    }

    /**
     * @param metrics Metrics.
     * @param node Node.
     */
    private Object[] createRowData(CacheMetrics metrics, ClusterNode node) {
        return new Object[] {
            metrics.name(),
            node == null ? null : node.id(),
            metrics.getCacheGets(),
            metrics.getCachePuts(),
            metrics.getCacheRemovals(),
            metrics.getCacheEvictions(),
            metrics.getCacheHits(),
            metrics.getCacheHitPercentage(),
            metrics.getCacheMisses(),
            metrics.getCacheMissPercentage(),
            metrics.getCacheTxCommits(),
            metrics.getCacheTxRollbacks(),
            metrics.getAverageGetTime(),
            metrics.getAveragePutTime(),
            metrics.getAverageRemoveTime(),
            metrics.getAverageTxCommitTime(),
            metrics.getAverageTxRollbackTime(),
            metrics.getOffHeapGets(),
            metrics.getOffHeapPuts(),
            metrics.getOffHeapRemovals(),
            metrics.getOffHeapEvictions(),
            metrics.getOffHeapHits(),
            metrics.getOffHeapHitPercentage(),
            metrics.getOffHeapMisses(),
            metrics.getOffHeapMissPercentage(),
            metrics.getHeapEntriesCount(),
            metrics.getOffHeapEntriesCount(),
            metrics.getOffHeapPrimaryEntriesCount(),
            metrics.getOffHeapBackupEntriesCount(),
            metrics.getOffHeapAllocatedSize(),
            metrics.getSize(),
            metrics.isEmpty(),
            metrics.getDhtEvictQueueCurrentSize(),
            metrics.getTxThreadMapSize(),
            metrics.getTxXidMapSize(),
            metrics.getTxDhtCommitQueueSize(),
            metrics.getTxDhtPrepareQueueSize(),
            metrics.getTxStartVersionCountsSize(),
            metrics.getTxCommittedVersionsSize(),
            metrics.getTxRolledbackVersionsSize(),
            metrics.isWriteBehindEnabled(),
            metrics.getWriteBehindFlushSize(),
            metrics.getWriteBehindFlushThreadCount(),
            metrics.getWriteBehindFlushFrequency(),
            metrics.getWriteBehindStoreBatchSize(),
            metrics.getWriteBehindTotalCriticalOverflowCount(),
            metrics.getWriteBehindCriticalOverflowCount(),
            metrics.getWriteBehindBufferSize(),
            metrics.getKeyType(),
            metrics.getValueType(),
            metrics.isStoreByValue(),
            metrics.getTotalPartitionsCount(),
            metrics.getRebalancingPartitionsCount(),
            metrics.getKeysToRebalanceLeft(),
            metrics.getRebalancingKeysRate(),
            metrics.getRebalancingBytesRate(),
            valueTimestampFromMillis(metrics.getRebalancingStartTime()),
            valueTimestampFromMillis(metrics.getEstimatedRebalancingFinishTime()),
            metrics.isStatisticsEnabled(),
            metrics.isManagementEnabled(),
            metrics.isReadThrough(),
            metrics.isWriteThrough(),
            metrics.isValidForReading(),
            metrics.isValidForWriting()
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        ColumnCondition nameCond = conditionForColumn("CACHE_NAME", first, last);

        final Integer cacheIdFilter;

        Collection<IgniteInternalCache<?, ?>> caches;

        if (nameCond.isEquality()) {
            log.debug("Get cache metrics: cache name");

            IgniteInternalCache<?, ?> cache = ctx.cache().cache(nameCond.getValue().getString());

            if (cache != null) {
                cacheIdFilter = cache.context().cacheId();

                caches = Collections.<IgniteInternalCache<?, ?>>singleton(cache);
            }
            else {
                cacheIdFilter = null;

                caches = Collections.emptySet();
            }
        }
        else {
            log.debug("Get cache metrics: full scan");

            cacheIdFilter = null;

            caches = ctx.cache().caches();
        }

        if (clusterMetrics) {
            List<Row> rows = new ArrayList<>();

            for (IgniteInternalCache<?, ?> cache : caches) {
                CacheMetrics metrics = cache.clusterMetrics();

                rows.add(createRow(ses, rows.size(), createRowData(metrics, null)));
            }

            return rows;
        }
        else {
            return new ParentChildRowIterable<ClusterNode, Map.Entry<Integer, CacheMetrics>>(ses,
                ctx.grid().cluster().nodes(),
                new IgniteClosure<ClusterNode, Iterator<Map.Entry<Integer, CacheMetrics>>>() {
                    @Override public Iterator<Map.Entry<Integer, CacheMetrics>> apply(ClusterNode node) {
                        if (!(node instanceof TcpDiscoveryNode))
                            return Collections.emptyIterator();

                        Map<Integer, CacheMetrics> metricsMap = ((TcpDiscoveryNode)node).cacheMetrics();

                        if (cacheIdFilter != null)
                            return !metricsMap.containsKey(cacheIdFilter) ? Collections.emptyIterator() :
                                new IgniteBiTuple<Integer, CacheMetrics>(cacheIdFilter, metricsMap.get(cacheIdFilter))
                                    .entrySet().iterator();
                        else
                            return metricsMap.entrySet().iterator();
                    }
                },
                new IgniteBiClosure<ClusterNode, Map.Entry<Integer, CacheMetrics>, Object[]>() {
                    @Override public Object[] apply(ClusterNode node, Map.Entry<Integer, CacheMetrics> entry) {
                        return createRowData(entry.getValue(), node);
                    }
                }
            );
        }
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.cache().caches().size();
    }
}

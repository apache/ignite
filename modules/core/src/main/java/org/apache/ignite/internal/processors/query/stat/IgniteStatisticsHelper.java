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

package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods to statistics messages generation.
 */
public class IgniteStatisticsHelper {
    /** Logger. */
    private final IgniteLogger log;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param locNodeId Local node id.
     * @param schemaMgr Schema manager.
     * @param logSupplier Ignite logger supplier to get logger from.
     */
    public IgniteStatisticsHelper(
        UUID locNodeId,
        SchemaManager schemaMgr,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.schemaMgr = schemaMgr;
        this.log = logSupplier.apply(IgniteStatisticsHelper.class);
    }

    /**
     * Get cache group context by specified statistics key.
     *
     * @param key Statistics key to get context by.
     * @return Cache group context for the given key.
     * @throws IgniteCheckedException If unable to find table by specified key.
     */
    public CacheGroupContext groupContext(StatisticsKey key) throws IgniteCheckedException {
        TableDescriptor tbl = schemaMgr.table(key.schema(), key.obj());

        if (tbl == null)
            throw new IgniteCheckedException(String.format("Can't find object %s.%s", key.schema(), key.obj()));

        return tbl.cacheInfo().cacheContext().group();
    }

    /**
     * Generate local statistics requests.
     *
     * @param target Statistics target to request local statistics by.
     * @param cfg Statistics configuration.
     * @return Collection of statistics request.
     */
    public List<StatisticsAddressedRequest> generateGatheringRequests(
        StatisticsTarget target,
        StatisticsObjectConfiguration cfg
    ) throws IgniteCheckedException {
        List<String> cols = (target.columns() == null) ? null : Arrays.asList(target.columns());
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(target.schema(), target.obj(), cols);

        Map<String, Long> versions = cfg.columns().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().version()));
        CacheGroupContext grpCtx = groupContext(target.key());
        AffinityTopologyVersion topVer = grpCtx.affinity().lastVersion();

        StatisticsRequest req = new StatisticsRequest(UUID.randomUUID(), keyMsg, StatisticsType.LOCAL, topVer, versions);

        List<List<ClusterNode>> assignments = grpCtx.affinity().assignments(topVer);
        Set<UUID> nodes = new HashSet<>();

        for (List<ClusterNode> partNodes : assignments) {
            if (F.isEmpty(partNodes))
                continue;

            nodes.add(partNodes.get(0).id());
        }

        List<StatisticsAddressedRequest> res = new ArrayList<>(nodes.size());

        for (UUID nodeId : nodes)
            res.add(new StatisticsAddressedRequest(req, nodeId));

        return res;
    }

    /**
     * Aggregate specified partition level statistics to local level statistics.
     *
     * @param cfg Statistics object configuration.
     * @param stats Collection of all local partition level or local level statistics by specified key to aggregate.
     * @return Local level aggregated statistics.
     */
    public ObjectStatisticsImpl aggregateLocalStatistics(
        StatisticsObjectConfiguration cfg,
        Collection<? extends ObjectStatisticsImpl> stats
    ) {
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(
            cfg.key().schema(),
            cfg.key().obj(),
            new ArrayList<>(cfg.columns().keySet())
        );

        // For now there can be only tables
        TableDescriptor tbl = schemaMgr.table(keyMsg.schema(), keyMsg.obj());

        if (tbl == null) {
            // remove all loaded statistics.
            if (log.isDebugEnabled())
                log.debug(String.format("Removing statistics for object %s.%s cause table doesn't exists.",
                    keyMsg.schema(), keyMsg.obj()));

            return null;
        }

        return aggregateLocalStatistics(tbl.type(), cfg, stats, log);
    }

    /**
     * Aggregate partition level statistics to local level one or local statistics to global one.
     *
     * @param tbl Table to aggregate statistics by.
     * @param cfg Statistics object configuration.
     * @param stats Collection of partition level or local level statistics to aggregate.
     * @param log Logger.
     * @return Local level statistics.
     */
    public static ObjectStatisticsImpl aggregateLocalStatistics(
        GridQueryTypeDescriptor tbl,
        StatisticsObjectConfiguration cfg,
        Collection<? extends ObjectStatisticsImpl> stats,
        IgniteLogger log
    ) {
        assert !stats.isEmpty();
        List<String> selectedCols = filterColumns(tbl, cfg.columns().keySet()).stream().map(T2::getValue)
            .collect(Collectors.toList());

        Map<String, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedCols.size());
        long rowCnt = 0;

        for (String col : selectedCols)
            colPartStats.put(col, new ArrayList<>());

        for (ObjectStatisticsImpl partStat : stats) {
            for (String col : selectedCols) {
                ColumnStatistics colPartStat = partStat.columnStatistics(col);

                if (colPartStat != null)
                    colPartStats.get(col).add(colPartStat);
            }

            rowCnt += partStat.rowCount();
        }

        Map<String, ColumnStatistics> colStats = new HashMap<>(selectedCols.size());

        for (String col : selectedCols) {
            StatisticsColumnConfiguration colCfg = cfg.columns().get(col);
            ColumnStatistics stat = ColumnStatisticsCollector.aggregate(colPartStats.get(col),
                colCfg.overrides());

            if (log.isDebugEnabled())
                log.debug("Aggregate column statistic done [col=" + col + ", stat=" + stat + ']');

            colStats.put(col, stat);
        }

        rowCnt = calculateRowCount(cfg, rowCnt);

        return new ObjectStatisticsImpl(rowCnt, colStats);
    }

    /**
     * Calculate effective row count. If there are some overrides in statistics configuration - maximum value will be
     * choosen. If not - will return actualRowCount.
     *
     * @param cfg Statistics configuration to dig overrides row count from.
     * @param actualRowCount Actual row count.
     * @return Effective row count.
     */
    public static long calculateRowCount(StatisticsObjectConfiguration cfg, long actualRowCount) {
        long overridedRowCnt = -1;

        for (StatisticsColumnConfiguration ccfg : cfg.columns().values()) {
            if (ccfg.overrides() != null && ccfg.overrides().total() != null) {
                Long colRowCnt = ccfg.overrides().total();

                overridedRowCnt = Math.max(overridedRowCnt, colRowCnt);
            }
        }

        return (overridedRowCnt == -1) ? actualRowCount : overridedRowCnt;
    }

    /**
     * Build object configurations array with all default parameters from specified targets.
     *
     * @param targets Targets to build configurations from.
     * @return StatisticsObjectConfiguration array.
     */
    public static StatisticsObjectConfiguration[] buildDefaultConfigurations(StatisticsTarget... targets) {
        StatisticsObjectConfiguration[] res = Arrays.stream(targets)
            .map(t -> {
                List<StatisticsColumnConfiguration> colCfgs;
                if (t.columns() == null)
                    colCfgs = Collections.emptyList();
                else
                    colCfgs = Arrays.stream(t.columns()).map(name -> new StatisticsColumnConfiguration(name, null))
                        .collect(Collectors.toList());

                return new StatisticsObjectConfiguration(t.key(), colCfgs,
                    StatisticsObjectConfiguration.DEFAULT_OBSOLESCENCE_MAX_PERCENT);
            }).toArray(StatisticsObjectConfiguration[]::new);

        return res;
    }

    /**
     * Filter columns by specified names.
     *
     * @param typeDescriptor Table descriptor.
     * @param colNames Column names.
     * @return Column with specified names.
     */
    public static List<T2<Integer, String>> filterColumns(
        GridQueryTypeDescriptor typeDescriptor,
        @Nullable Collection<String> colNames
    ) {
        Stream<T2<Integer, String>> colStream = enumerate(typeDescriptor.fields().keySet().stream(),
            QueryUtils.DEFAULT_COLUMNS_COUNT);

        if (F.isEmpty(colNames)) {
            return colStream.filter(col -> !QueryUtils.KEY_FIELD_NAME.equals(col.getValue()) &&
                    !QueryUtils.VAL_FIELD_NAME.equals(col.getValue())).collect(Collectors.toList());
        }

        Set<String> colNamesSet = new HashSet<>(colNames);

        return colStream.filter(col -> colNamesSet.contains(col.getValue())).collect(Collectors.toList());
    }

    /** */
    private static <T> Stream<T2<Integer, T>> enumerate(Stream<? extends T> stream, int startIdx) {
        Iterator<T2<Integer, T>> iter = new Iterator<T2<Integer, T>>() {
            private final Iterator<? extends T> streamIter = stream.iterator();

            private int idx = startIdx;

            @Override public boolean hasNext() {
                return streamIter.hasNext();
            }

            @Override public T2<Integer, T> next() {
                return new T2<>(idx++, streamIter.next());
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED |
            Spliterator.IMMUTABLE), false);
    }
}

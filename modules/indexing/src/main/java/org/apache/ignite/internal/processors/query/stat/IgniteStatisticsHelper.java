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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.table.Column;
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
        GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());

        if (tbl == null) {
            // remove all loaded statistics.
            if (log.isDebugEnabled())
                log.debug(String.format("Removing statistics for object %s.%s cause table doesn't exists.",
                    keyMsg.schema(), keyMsg.obj()));

            return null;
        }

        return aggregateLocalStatistics(tbl, cfg, stats, log);
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
        GridH2Table tbl,
        StatisticsObjectConfiguration cfg,
        Collection<? extends ObjectStatisticsImpl> stats,
        IgniteLogger log
    ) {
        assert !stats.isEmpty();
        Column[] selectedCols = filterColumns(tbl.getColumns(), cfg.columns().keySet());

        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedCols.length);
        long rowCnt = 0;

        for (Column col : selectedCols)
            colPartStats.put(col, new ArrayList<>());

        for (ObjectStatisticsImpl partStat : stats) {
            for (Column col : selectedCols) {
                ColumnStatistics colPartStat = partStat.columnStatistics(col.getName());

                if (colPartStat != null) {
                    colPartStats.computeIfPresent(col, (k, v) -> {
                        v.add(colPartStat);

                        return v;
                    });
                }
            }

            rowCnt += partStat.rowCount();
        }

        Map<String, ColumnStatistics> colStats = new HashMap<>(selectedCols.length);

        for (Column col : selectedCols) {
            StatisticsColumnConfiguration colCfg = cfg.columns().get(col.getName());
            ColumnStatistics stat = ColumnStatisticsCollector.aggregate(tbl::compareTypeSafe, colPartStats.get(col), colCfg.overrides());

            if (log.isDebugEnabled())
                log.debug("Aggregate column statistic done [col=" + col.getName() + ", stat=" + stat + ']');

            colStats.put(col.getName(), stat);
        }

        long overridedRowCnt = -1;

        for (StatisticsColumnConfiguration ccfg : cfg.columns().values()) {
            if (ccfg.overrides() != null && ccfg.overrides().total() != null) {
                Long colRowCnt = ccfg.overrides().total();

                overridedRowCnt = Math.max(overridedRowCnt, colRowCnt);
            }
        }

        rowCnt = (overridedRowCnt == -1) ? rowCnt : overridedRowCnt;

        ObjectStatisticsImpl tblStats = new ObjectStatisticsImpl(rowCnt, colStats);

        return tblStats;
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
     * @param cols Columns to filter.
     * @param colNames Column names.
     * @return Column with specified names.
     */
    public static Column[] filterColumns(Column[] cols, @Nullable Collection<String> colNames) {
        if (F.isEmpty(colNames)) {
            return Arrays.stream(cols)
                .filter(c -> c.getColumnId() >= QueryUtils.DEFAULT_COLUMNS_COUNT)
                .toArray(Column[]::new);
        }

        Set<String> colNamesSet = new HashSet<>(colNames);

        return Arrays.stream(cols).filter(c -> colNamesSet.contains(c.getName())).toArray(Column[]::new);
    }
}

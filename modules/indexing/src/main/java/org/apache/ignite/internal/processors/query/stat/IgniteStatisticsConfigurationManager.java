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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnConfigurationViewWalker;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnConfigurationView;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;

/**
 * Holds statistic configuration objects at the distributed metastore
 * and match local statistics with target statistic configuration.
 */
public class IgniteStatisticsConfigurationManager {
    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** */
    private static final String STAT_CFG_VIEW_NAME = "statistics.configuration";

    /** */
    private static final String STAT_CFG_VIEW_DESCRIPTION = "Statistics configuration";

    /** Empty strings array. */
    public static final String[] EMPTY_STRINGS = new String[0];

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Distributed metastore. */
    private volatile DistributedMetaStorage distrMetaStorage;

    /** Statistics repository.*/
    private final IgniteStatisticsRepository repo;

    /** Statistic gatherer. */
    private final StatisticsGatherer gatherer;

    /** */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Logger. */
    private final IgniteLogger log;

    /** Started flag (used to skip updates of the distributed metastorage on start). */
    private volatile boolean started;

    /** Monitor to synchronize changes repository: aggregate after collects and drop statistics. */
    private final Object mux = new Object();

    /** */
    private final GridClusterStateProcessor cluster;

    /** */
    private final GridInternalSubscriptionProcessor subscriptionProcessor;

    /** */
    private final GridCachePartitionExchangeManager exchange;

    /** */
    private final DistributedMetastorageLifecycleListener distrMetaStoreLsnr = new DistributedMetastorageLifecycleListener() {
        @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
            distrMetaStorage = (DistributedMetaStorage)metastorage;

            distrMetaStorage.listen(
                (metaKey) -> metaKey.startsWith(STAT_OBJ_PREFIX),
                (k, oldV, newV) -> {
                    // Skip invoke on start node (see 'ReadableDistributedMetaStorage#listen' the second case)
                    // The update statistics on start node is handled by 'scanAndCheckLocalStatistic' method
                    // called on exchange done.
                    if (!started)
                        return;

                    mgmtPool.submit(() -> {
                        try {
                            onChangeStatisticConfiguration(
                                (StatisticsObjectConfiguration)oldV,
                                (StatisticsObjectConfiguration)newV
                            );
                        }
                        catch (Throwable e) {
                            log.warning("Unexpected exception on change statistic configuration [old="
                                + oldV + ", new=" + newV + ']', e);
                        }
                    });
                }
            );
        }
    };

    /** Exchange listener. */
    private final PartitionsExchangeAware exchAwareLsnr = new PartitionsExchangeAware() {
        @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
            started = true;

            // Skip join/left client nodes.
            if (fut.exchangeType() != GridDhtPartitionsExchangeFuture.ExchangeType.ALL ||
                cluster.clusterState().lastState() != ClusterState.ACTIVE)
                return;

            DiscoveryEvent evt = fut.firstEvent();

            // Skip create/destroy caches.
            if (evt.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                if (msg instanceof DynamicCacheChangeBatch)
                    return;
            }

            scanAndCheckLocalStatistics(fut.topologyVersion());
        }
    };

    /** Drop columns listener. */
    private final BiConsumer<GridH2Table, List<String>> dropColsLsnr = new BiConsumer<GridH2Table, List<String>>() {
        /**
         * Drop statistics after columns dropped.
         *
         * @param tbl Table.
         * @param cols Dropped columns.
         */
        @Override public void accept(GridH2Table tbl, List<String> cols) {
            assert !F.isEmpty(cols);

            dropStatistics(
                Collections.singletonList(
                    new StatisticsTarget(
                        tbl.identifier().schema(),
                        tbl.getName(),
                        cols.toArray(EMPTY_STRINGS)
                    )
                ),
                false
            );
        }
    };

    /** Drop table listener. */
    private final BiConsumer<String, String> dropTblLsnr = new BiConsumer<String, String>() {
        /**
         * Drop statistics after table dropped.
         *
         * @param schema Schema name.
         * @param name Table name.
         */
        @Override public void accept(String schema, String name) {
            assert !F.isEmpty(schema) && !F.isEmpty(name) : schema + ":" + name;

            StatisticsKey key = new StatisticsKey(schema, name);

            try {
                StatisticsObjectConfiguration cfg = config(key);

                if (cfg != null && !F.isEmpty(cfg.columns()))
                    dropStatistics(Collections.singletonList(new StatisticsTarget(schema, name)), false);
            }
            catch (Throwable e) {
                if (!X.hasCause(e, NodeStoppingException.class))
                    throw new IgniteSQLException("Error on drop statistics for dropped table [key=" + key + ']', e);
            }
        }
    };

    /** */
    public IgniteStatisticsConfigurationManager(
        SchemaManager schemaMgr,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        GridSystemViewManager sysViewMgr,
        GridClusterStateProcessor cluster,
        GridCachePartitionExchangeManager exchange,
        IgniteStatisticsRepository repo,
        StatisticsGatherer gatherer,
        IgniteThreadPoolExecutor mgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.schemaMgr = schemaMgr;
        log = logSupplier.apply(IgniteStatisticsConfigurationManager.class);
        this.repo = repo;
        this.mgmtPool = mgmtPool;
        this.gatherer = gatherer;
        this.cluster = cluster;
        this.subscriptionProcessor = subscriptionProcessor;
        this.exchange = exchange;

        this.subscriptionProcessor.registerDistributedMetastorageListener(distrMetaStoreLsnr);

        sysViewMgr.registerFiltrableView(STAT_CFG_VIEW_NAME, STAT_CFG_VIEW_DESCRIPTION,
            new StatisticsColumnConfigurationViewWalker(), this::columnConfigurationViewSupplier, Function.identity());
    }

    /**
     * Statistics column configuration view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with selected statistics column configuration views.
     */
    private Iterable<StatisticsColumnConfigurationView> columnConfigurationViewSupplier(Map<String, Object> filter) {
        String schema = (String)filter.get(StatisticsColumnConfigurationViewWalker.SCHEMA_FILTER);
        String name = (String)filter.get(StatisticsColumnConfigurationViewWalker.NAME_FILTER);

        Collection<StatisticsObjectConfiguration> configs;
        try {
            if (!F.isEmpty(schema) && !F.isEmpty(name)) {
                StatisticsKey key = new StatisticsKey(schema, name);
                StatisticsObjectConfiguration keyCfg = config(key);

                if (keyCfg == null)
                    return Collections.emptyList();

                configs = Collections.singletonList(keyCfg);
            }
            else
                configs = getAllConfig();
        }
        catch (IgniteCheckedException e) {
            log.warning("Error while getting statistics configuration: " + e.getMessage(), e);

            configs = Collections.emptyList();
        }

        List<StatisticsColumnConfigurationView> res = new ArrayList<>();

        for (StatisticsObjectConfiguration cfg : configs) {
            for (StatisticsColumnConfiguration colCfg : cfg.columnsAll().values()) {
                if (!colCfg.tombstone())
                    res.add(new StatisticsColumnConfigurationView(cfg, colCfg));
            }
        }

        return res;
    }

    /**
     * Get statistics configurations for all objects.
     *
     * @return Collection of all statistics configuration.
     * @throws IgniteCheckedException In case of error.
     */
    public Collection<StatisticsObjectConfiguration> getAllConfig() throws IgniteCheckedException {
        List<StatisticsObjectConfiguration> res = new ArrayList<>();

        distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> res.add((StatisticsObjectConfiguration) v));

        return res;
    }

    /**
     * Start tracking configuration changes and do initial loading.
     */
    public void start() {
        if (log.isTraceEnabled())
            log.trace("Statistics configuration manager starting...");

        exchange.registerExchangeAwareComponent(exchAwareLsnr);

        schemaMgr.registerDropColumnsListener(dropColsLsnr);
        schemaMgr.registerDropTableListener(dropTblLsnr);

        if (log.isDebugEnabled())
            log.debug("Statistics configuration manager started.");

        if (distrMetaStorage != null)
            scanAndCheckLocalStatistics(exchange.readyAffinityVersion());
    }

    /**
     * Stop tracking configuration changes.
     */
    public void stop() {
        if (log.isTraceEnabled())
            log.trace("Statistics configuration manager stopping...");

        exchange.unregisterExchangeAwareComponent(exchAwareLsnr);

        schemaMgr.unregisterDropColumnsListener(dropColsLsnr);
        schemaMgr.unregisterDropTableListener(dropTblLsnr);

        if (log.isDebugEnabled())
            log.debug("Statistics configuration manager stopped.");
    }

    /** */
    private void scanAndCheckLocalStatistics(AffinityTopologyVersion topVer) {
        mgmtPool.submit(() -> {
            Map<StatisticsObjectConfiguration, Set<Integer>> res = new HashMap<>();

            try {
                distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> {
                    StatisticsObjectConfiguration cfg = (StatisticsObjectConfiguration)v;

                    Set<Integer> parts = checkLocalStatistics(cfg, topVer);

                    res.put(cfg, parts);
                });

                repo.checkObsolescenceInfo(res);
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception on check local statistic on start", e);
            }
        });
    }

    /**
     * Update local statistic for specified database objects on the cluster.
     * Each node will scan local primary partitions to collect and update local statistic.
     *
     * @param targets DB objects to statistics update.
     */
    public void updateStatistics(StatisticsObjectConfiguration... targets) {
        if (log.isDebugEnabled())
            log.debug("Update statistics [targets=" + targets + ']');

        for (StatisticsObjectConfiguration target : targets) {

            GridH2Table tbl = schemaMgr.dataTable(target.key().schema(), target.key().obj());

            validate(target, tbl);

            List<StatisticsColumnConfiguration> colCfgs;
            if (F.isEmpty(target.columns()))
                colCfgs = Arrays.stream(tbl.getColumns())
                    .filter(c -> c.getColumnId() >= QueryUtils.DEFAULT_COLUMNS_COUNT)
                    .map(c -> new StatisticsColumnConfiguration(c.getName(), null))
                    .collect(Collectors.toList());
            else
                colCfgs = new ArrayList<>(target.columns().values());

            StatisticsObjectConfiguration newCfg = new StatisticsObjectConfiguration(target.key(), colCfgs,
                target.maxPartitionObsolescencePercent());

            try {
                while (true) {
                    String key = key2String(newCfg.key());

                    StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);
                    StatisticsObjectConfiguration resultCfg = (oldCfg == null) ? newCfg :
                        StatisticsObjectConfiguration.merge(oldCfg, newCfg);

                    if (distrMetaStorage.compareAndSet(key, oldCfg, resultCfg))
                        break;
                }
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
            }
        }
    }

    /**
     * Drop local statistic for specified database objects on the cluster.
     * Remove local aggregated and partitioned statistics that are stored at the local metastorage.
     *
     * @param targets DB objects to update statistics by.
     * @param validate if {@code true} - validate statistics existence, otherwise - just try to remove.
     */
    public void dropStatistics(List<StatisticsTarget> targets, boolean validate) {
        if (log.isDebugEnabled())
            log.debug("Drop statistics [targets=" + targets + ']');

        for (StatisticsTarget target : targets) {
            String key = key2String(target.key());

            try {
                while (true) {
                    StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

                    if (validate)
                        validateDropRefresh(target, oldCfg);

                    if (oldCfg == null)
                        return;

                    StatisticsObjectConfiguration newCfg = oldCfg.dropColumns(
                        target.columns() != null ?
                            Arrays.stream(target.columns()).collect(Collectors.toSet()) :
                            Collections.emptySet()
                    );

                    if (distrMetaStorage.compareAndSet(key, oldCfg, newCfg))
                        break;
                }
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteSQLException(
                    "Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
            }
        }
    }

    /**
     * Drop all local statistics on the cluster.
     */
    public void dropAll() {
        try {
            final List<StatisticsTarget> targetsToRemove = new ArrayList<>();

            distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> {
                    StatisticsKey statKey = ((StatisticsObjectConfiguration)v).key();

                    StatisticsObjectConfiguration cfg = (StatisticsObjectConfiguration)v;

                    if (!F.isEmpty(cfg.columns()))
                        targetsToRemove.add(new StatisticsTarget(statKey, null));
                }
            );

            dropStatistics(targetsToRemove, false);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException(
                "Unexpected exception drop all statistics", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /**
     * Refresh local statistic for specified database objects on the cluster.
     *
     * @param targets DB objects to statistics update.
     */
    public void refreshStatistics(List<StatisticsTarget> targets) {
        if (log.isDebugEnabled())
            log.debug("Drop statistics [targets=" + targets + ']');

        for (StatisticsTarget target : targets) {
            String key = key2String(target.key());

            try {
                while (true) {
                    StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

                    validateDropRefresh(target, oldCfg);

                    Set<String> cols;
                    if (F.isEmpty(target.columns())) {
                        cols = oldCfg.columns().values().stream().map(StatisticsColumnConfiguration::name)
                            .collect(Collectors.toSet());
                    }
                    else
                        cols = Arrays.stream(target.columns()).collect(Collectors.toSet());

                    StatisticsObjectConfiguration newCfg = oldCfg.refresh(cols);

                    if (distrMetaStorage.compareAndSet(key, oldCfg, newCfg))
                        break;
                }
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteSQLException(
                    "Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
            }
        }
    }

    /**
     * Validate that drop/refresh target exists in specified configuration. For statistics refresh/drop operations.
     *
     * @param target Operation targer.
     * @param cfg Current statistics configuration.
     */
    private void validateDropRefresh(@NotNull StatisticsTarget target, @NotNull StatisticsObjectConfiguration cfg) {
        if (cfg == null || F.isEmpty(cfg.columns())) {
            throw new IgniteSQLException(
                "Statistic doesn't exist for [schema=" + target.schema() + ", obj=" + target.obj() + ']',
                IgniteQueryErrorCode.TABLE_NOT_FOUND
            );
        }

        if (!F.isEmpty(target.columns())) {
            for (String col : target.columns()) {
                if (!cfg.columns().containsKey(col)) {
                    throw new IgniteSQLException(
                        "Statistic doesn't exist for [" +
                            "schema=" + cfg.key().schema() +
                            ", obj=" + cfg.key().obj() +
                            ", col=" + col + ']',
                        IgniteQueryErrorCode.COLUMN_NOT_FOUND
                    );
                }
            }
        }
    }

    /**
     * Scan local statistic saved at the local metastorage, compare ones to statistic configuration.
     * The local statistics must be matched with configuration:
     * - re-collect old statistics;
     * - drop local statistics if ones dropped on cluster;
     * - collect new statistics if it possible.
     *
     * The method is called on change affinity assignment (end of PME).
     * @param cfg expected statistic configuration.
     * @param topVer topology version.
     * @return Set of local primary partitions.
     */
    private Set<Integer> checkLocalStatistics(StatisticsObjectConfiguration cfg, final AffinityTopologyVersion topVer) {
        try {
            GridH2Table tbl = schemaMgr.dataTable(cfg.key().schema(), cfg.key().obj());

            if (tbl == null) {
                // Drop tables handle by onDropTable
                return Collections.emptySet();
            }

            GridCacheContext cctx = tbl.cacheContext();

            if (cctx == null)
                return Collections.emptySet();

            AffinityTopologyVersion topVer0 = cctx.affinity().affinityReadyFuture(topVer).get();

            final Set<Integer> parts = cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer0);

            if (F.isEmpty(parts)) {
                // There is no data on the node for specified cache.
                // Remove oll data
                dropColumnsOnLocalStatistics(cfg, cfg.columns().keySet());

                return Collections.emptySet();
            }

            final Set<Integer> partsOwn = new HashSet<>(
                cctx.affinity().backupPartitions(cctx.localNodeId(), topVer0)
            );

            partsOwn.addAll(parts);

            if (log.isDebugEnabled())
                log.debug("Check local statistics [key=" + cfg + ", parts=" + parts + ']');

            Collection<ObjectPartitionStatisticsImpl> partStats = repo.getLocalPartitionsStatistics(cfg.key());

            Set<Integer> partsToRmv = new HashSet<>();
            Set<Integer> partsToCollect = new HashSet<>(parts);
            Map<String, StatisticsColumnConfiguration> colsToCollect = new HashMap<>();
            Set<String> colsToRmv = new HashSet<>();

            if (!F.isEmpty(partStats)) {
                for (ObjectPartitionStatisticsImpl pstat : partStats) {
                    if (!partsOwn.contains(pstat.partId()))
                        partsToRmv.add(pstat.partId());

                    boolean partExists = true;

                    for (StatisticsColumnConfiguration colCfg : cfg.columnsAll().values()) {
                        ColumnStatistics colStat = pstat.columnStatistics(colCfg.name());

                        if (colCfg.tombstone()) {
                            if (colStat != null)
                                colsToRmv.add(colCfg.name());
                        }
                        else {
                            if (colStat == null || colStat.version() < colCfg.version()) {
                                colsToCollect.put(colCfg.name(), colCfg);

                                partsToCollect.add(pstat.partId());

                                partExists = false;
                            }
                        }
                    }

                    if (partExists)
                        partsToCollect.remove(pstat.partId());
                }
            }

            if (!F.isEmpty(partsToRmv)) {
                if (log.isDebugEnabled()) {
                    log.debug("Remove local partitioned statistics [key=" + cfg.key() +
                        ", part=" + partsToRmv + ']');
                }

                partsToRmv.forEach(p -> {
                    assert !partsToCollect.contains(p);

                    repo.clearLocalPartitionStatistics(cfg.key(), p);
                });
            }

            if (!F.isEmpty(colsToRmv))
                dropColumnsOnLocalStatistics(cfg, colsToRmv);

            if (!F.isEmpty(partsToCollect))
                gatherLocalStatistics(cfg, tbl, parts, partsToCollect, colsToCollect);
            else {
                // All local statistics by partition are available.
                // Only refresh aggregated local statistics.
                gatherer.aggregateStatisticsAsync(cfg.key(), () -> aggregateLocalGathering(cfg.key(), parts));
            }

            return parts;
        }
        catch (Throwable ex) {
            log.error("Unexpected error on check local statistics", ex);

            return Collections.emptySet();
        }
    }

    /**
     * Match local statistic with changes of statistic configuration:
     * - update statistics;
     * - drop columns;
     * - add new columns to collect statistics.
     *
     * The method is called on change statistic configuration object at the distributed metastorage.
     */
    private void onChangeStatisticConfiguration(
        StatisticsObjectConfiguration oldCfg,
        StatisticsObjectConfiguration newCfg
    ) {
        synchronized (mux) {
            if (log.isDebugEnabled())
                log.debug("Statistic configuration changed [old=" + oldCfg + ", new=" + newCfg + ']');

            StatisticsObjectConfiguration.Diff diff = StatisticsObjectConfiguration.diff(oldCfg, newCfg);

            if (!F.isEmpty(diff.dropCols()))
                dropColumnsOnLocalStatistics(newCfg, diff.dropCols());

            if (!F.isEmpty(diff.updateCols())) {
                GridH2Table tbl = schemaMgr.dataTable(newCfg.key().schema(), newCfg.key().obj());

                // Drop table handles by dropTblLsnr.
                if (tbl == null)
                    return;

                GridCacheContext cctx = tbl.cacheContext();

                // Not affinity node (e.g. client node)
                if (cctx == null)
                    return;

                Set<Integer> parts = cctx.affinity().primaryPartitions(
                    cctx.localNodeId(), cctx.affinity().affinityTopologyVersion());

                gatherLocalStatistics(
                    newCfg,
                    tbl,
                    parts,
                    parts,
                    diff.updateCols()
                );
            }
        }
    }

    /**
     * Gather local statistics for specified object and partitions.
     *
     * @param cfg Statistics object configuration.
     * @param tbl Table.
     * @param partsToAggregate Set of partition ids to aggregate.
     * @param partsToCollect Set of partition ids to gather statistics from.
     * @param colsToCollect If specified - collect statistics only for this columns,
     *                      otherwise - collect to all columns from object configuration.
     */
    public void gatherLocalStatistics(
        StatisticsObjectConfiguration cfg,
        GridH2Table tbl,
        Set<Integer> partsToAggregate,
        Set<Integer> partsToCollect,
        Map<String, StatisticsColumnConfiguration> colsToCollect
    ) {
        if (F.isEmpty(colsToCollect))
            colsToCollect = cfg.columns();

        gatherer.gatherLocalObjectsStatisticsAsync(tbl, cfg, colsToCollect, partsToCollect);

        gatherer.aggregateStatisticsAsync(cfg.key(), () -> aggregateLocalGathering(cfg.key(), partsToAggregate));
    }

    /** */
    private void dropColumnsOnLocalStatistics(StatisticsObjectConfiguration cfg, Set<String> cols) {
        if (log.isDebugEnabled()) {
            log.debug("Remove local statistics [key=" + cfg.key() +
                ", columns=" + cols + ']');
        }

        LocalStatisticsGatheringContext gCtx = gatherer.gatheringInProgress(cfg.key());

        if (gCtx != null) {
            gCtx.futureAggregate().thenAccept((v) -> {
                repo.clearLocalStatistics(cfg.key(), cols);
                repo.clearLocalPartitionsStatistics(cfg.key(), cols);
            });
        }
        else {
            repo.clearLocalStatistics(cfg.key(), cols);
            repo.clearLocalPartitionsStatistics(cfg.key(), cols);
        }
    }

    /** */
    private ObjectStatisticsImpl aggregateLocalGathering(StatisticsKey key, Set<Integer> partsToAggregate) {
        synchronized (mux) {
            try {
                StatisticsObjectConfiguration cfg = distrMetaStorage.read(key2String(key));

                return repo.aggregatedLocalStatistics(partsToAggregate, cfg);
            }
            catch (Throwable e) {
                if (!X.hasCause(e, NodeStoppingException.class)) {
                    log.error("Error on aggregate statistic on finish local statistics collection" +
                        " [key=" + key + ", parts=" + partsToAggregate, e);
                }

                return null;
            }
        }
    }

    /**
     * Read statistics object configuration by key.
     *
     * @param key Statistics key to read configuration by.
     * @return Statistics object configuration of {@code null} if there are no such configuration.
     * @throws IgniteCheckedException In case of errors.
     */
    public StatisticsObjectConfiguration config(StatisticsKey key) throws IgniteCheckedException {
        return distrMetaStorage.read(key2String(key));
    }

    /**
     * Validate specified configuration: check that specified table exist and contains all specified columns.
     *
     * @param cfg Statistics object configuration to check.
     * @param tbl Corresponding GridH2Table (if exists).
     */
    private void validate(StatisticsObjectConfiguration cfg, GridH2Table tbl) {
        if (tbl == null) {
            throw new IgniteSQLException(
                "Table doesn't exist [schema=" + cfg.key().schema() + ", table=" + cfg.key().obj() + ']',
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        if (!F.isEmpty(cfg.columns())) {
            for (String col : cfg.columns().keySet()) {
                if (!tbl.doesColumnExist(col)) {
                    throw new IgniteSQLException(
                        "Column doesn't exist [schema=" + cfg.key().schema() +
                            ", table=" + cfg.key().obj() +
                            ", column=" + col + ']',
                        IgniteQueryErrorCode.COLUMN_NOT_FOUND);
                }
            }
        }
    }

    /**
     * Generate metastorage key by specified statistics key.
     *
     * @param key Statistics key.
     * @return Metastorage key.
     */
    private static String key2String(StatisticsKey key) {
        StringBuilder sb = new StringBuilder(STAT_OBJ_PREFIX);

        sb.append(key.schema()).append('.').append(key.obj());

        return sb.toString();
    }
}

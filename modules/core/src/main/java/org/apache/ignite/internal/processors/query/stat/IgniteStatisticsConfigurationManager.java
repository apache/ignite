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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnConfigurationViewWalker;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.AbstractSchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.view.ColumnConfigurationViewSupplier;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
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

    /** Statistic processor. */
    private final StatisticsProcessor statProc;

    /** */
    private final BusyExecutor mgmtBusyExecutor;

    /** Persistence enabled flag. */
    private final boolean persistence;

    /** Logger. */
    private final IgniteLogger log;

    /** Last ready topology version if {@code null} - used to skip updates of the distributed metastorage on start. */
    private volatile AffinityTopologyVersion topVer;

    /** Cluster state processor. */
    private final GridClusterStateProcessor cluster;

    /** Is server node flag. */
    private final boolean isServerNode;

    /** Active flag. */
    private volatile boolean active;

    /** Configuration change subscribers. */
    private final List<Consumer<StatisticsObjectConfiguration>> subscribers = new CopyOnWriteArrayList<>();

    /** Change statistics configuration listener to update particular object statistics. */
    private final DistributedMetastorageLifecycleListener distrMetaStoreLsnr =
        new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                distrMetaStorage = metastorage;

                distrMetaStorage.listen(
                    (metaKey) -> metaKey.startsWith(STAT_OBJ_PREFIX),
                    (k, oldV, newV) -> {
                        // Skip invoke on start node (see 'ReadableDistributedMetaStorage#listen' the second case)
                        // The update statistics on start node is handled by 'scanAndCheckLocalStatistic' method
                        // called on exchange done.
                        if (topVer == null)
                            return;

                        StatisticsObjectConfiguration newStatCfg = (StatisticsObjectConfiguration)newV;

                        for (Consumer<StatisticsObjectConfiguration> subscriber : subscribers)
                            subscriber.accept(newStatCfg);

                        mgmtBusyExecutor.execute(() -> {
                            try {
                                while (!updateLocalStatisticsAsync((StatisticsObjectConfiguration)newV).get())
                                    ; // No-op
                            }
                            catch (IgniteCheckedException e) {
                                log.warning("Unexpected error during statistics collection: " + e.getMessage(), e);
                            }
                        });
                    }
                );
            }
        };

    /** Schema change listener */
    private final SchemaChangeListener schemaLsnr = new AbstractSchemaChangeListener() {
        @Override public void onColumnsDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo,
            List<String> cols
        ) {
            if (!active)
                return;

            assert !F.isEmpty(cols);

            // Drop statistics after columns dropped.
            dropStatistics(
                Collections.singletonList(
                    new StatisticsTarget(schemaName, typeDesc.tableName(), cols.toArray(EMPTY_STRINGS))
                ),
                false
            );
        }

        @Override public void onSqlTypeDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            boolean destroy
        ) {
            if (!active || !destroy)
                return;

            String name = typeDesc.tableName();

            assert !F.isEmpty(schemaName) && !F.isEmpty(name) : schemaName + ":" + name;

            StatisticsKey key = new StatisticsKey(schemaName, name);

            try {
                StatisticsObjectConfiguration cfg = config(key);

                if (cfg != null && !F.isEmpty(cfg.columns()))
                    dropStatistics(Collections.singletonList(new StatisticsTarget(schemaName, name)), false);
            }
            catch (Throwable e) {
                if (!X.hasCause(e, NodeStoppingException.class))
                    throw new IgniteSQLException("Error on drop statistics for dropped table [key=" + key + ']', e);
            }
        }
    };

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     * @param subscriptionProcessor Subscription processor.
     * @param sysViewMgr System view manager.
     * @param cluster Cluster state processor.
     * @param statProc Staitistics processor.
     * @param persistence Persistence enabled flag.
     * @param mgmtPool Statistics management pool
     * @param stopping Stopping state supplier.
     * @param logSupplier Log supplier.
     * @param isServerNode Server node flag.
     */
    public IgniteStatisticsConfigurationManager(
        SchemaManager schemaMgr,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        GridSystemViewManager sysViewMgr,
        GridClusterStateProcessor cluster,
        StatisticsProcessor statProc,
        boolean persistence,
        IgniteThreadPoolExecutor mgmtPool,
        Supplier<Boolean> stopping,
        Function<Class<?>, IgniteLogger> logSupplier,
        boolean isServerNode
    ) {
        this.schemaMgr = schemaMgr;
        log = logSupplier.apply(IgniteStatisticsConfigurationManager.class);
        this.persistence = persistence;
        this.mgmtBusyExecutor = new BusyExecutor("configuration", mgmtPool, stopping, logSupplier);
        this.statProc = statProc;
        this.cluster = cluster;
        this.isServerNode = isServerNode;

        subscriptionProcessor.registerDistributedMetastorageListener(distrMetaStoreLsnr);

        if (isServerNode)
            subscriptionProcessor.registerSchemaChangeListener(schemaLsnr);

        ColumnConfigurationViewSupplier colCfgViewSupplier = new ColumnConfigurationViewSupplier(this,
            logSupplier);

        sysViewMgr.registerFiltrableView(STAT_CFG_VIEW_NAME, STAT_CFG_VIEW_DESCRIPTION,
            new StatisticsColumnConfigurationViewWalker(),
            colCfgViewSupplier::columnConfigurationViewSupplier,
            Function.identity());
    }

    /**
     * Update statistics after topology change, if necessary.
     *
     * @param fut Topology change future.
     */
    public void afterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        topVer = fut.topologyVersion();

        // Skip join/left client nodes.
        if (fut.exchangeType() != GridDhtPartitionsExchangeFuture.ExchangeType.ALL ||
            (persistence && cluster.clusterState().lastState() != ClusterState.ACTIVE))
            return;

        DiscoveryEvent evt = fut.firstEvent();

        // Skip create/destroy caches.
        if (evt.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
            DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

            if (msg instanceof DynamicCacheChangeBatch)
                return;
        }

        mgmtBusyExecutor.execute(this::updateAllLocalStatistics);
    }

    /**
     * Pass all necessary parameters to schedule statistics key update.
     *
     * @param cfg Statistics object configuration to update statistics by.
     */
    private IgniteInternalFuture<Boolean> updateLocalStatisticsAsync(StatisticsObjectConfiguration cfg) {
        TableDescriptor tbl = schemaMgr.table(cfg.key().schema(), cfg.key().obj());
        GridQueryTypeDescriptor typeDesc = tbl != null ? tbl.type() : null;
        GridCacheContextInfo<?, ?> cacheInfo = tbl != null ? tbl.cacheInfo() : null;
        GridCacheContext<?, ?> cctx = cacheInfo != null ? cacheInfo.cacheContext() : null;

        if (tbl == null || cfg.columns().isEmpty()) {
            // Can be drop table event, need to ensure that there is no stored data left for this table.
            if (log.isDebugEnabled()) {
                if (tbl == null)
                    log.debug("Can't find table by key " + cfg.key() + ". Check statistics empty.");
                else if (cfg == null)
                    log.debug("Tombstone configuration by key " + cfg.key() + ". Check statistics empty.");
            }

            // Ensure to clean local metastorage.
            LocalStatisticsGatheringContext ctx = new LocalStatisticsGatheringContext(false, typeDesc,
                cacheInfo, cfg, Collections.emptySet(), topVer);

            statProc.updateLocalStatistics(ctx);

            if (tbl == null && !cfg.columns().isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Removing config for non existing object " + cfg.key());

                return dropStatisticsAsync(Collections.singletonList(new StatisticsTarget(cfg.key())), false);
            }

            return new GridFinishedFuture<>(true);
        }

        if (cctx == null || !cctx.gate().enterIfNotStopped()) {
            if (log.isDebugEnabled())
                log.debug("Unable to lock table by key " + cfg.key() + ". Skipping statistics collection.");

            return new GridFinishedFuture<>(true);
        }

        try {
            AffinityTopologyVersion topVer0 = cctx.affinity().affinityReadyFuture(topVer).get();

            final Set<Integer> primParts = cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer0);

            LocalStatisticsGatheringContext ctx = new LocalStatisticsGatheringContext(false, typeDesc,
                cacheInfo, cfg, primParts, topVer0);

            statProc.updateLocalStatistics(ctx);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unexpected error during statistics collection: " + e.getMessage(), e);
        }
        finally {
            cctx.gate().leave();
        }

        return new GridFinishedFuture<>(true);
    }

    /**
     * Get statistics configurations for all objects.
     *
     * @return Collection of all statistics configuration.
     * @throws IgniteCheckedException In case of error.
     */
    public Collection<StatisticsObjectConfiguration> getAllConfig() throws IgniteCheckedException {
        List<StatisticsObjectConfiguration> res = new ArrayList<>();

        distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> res.add((StatisticsObjectConfiguration)v));

        return res;
    }

    /**
     * Start tracking configuration changes and do initial loading.
     */
    public void start() {
        if (log.isTraceEnabled())
            log.trace("Statistics configuration manager starting...");

        active = true;
        mgmtBusyExecutor.activate();

        if (log.isDebugEnabled())
            log.debug("Statistics configuration manager started.");

        if (distrMetaStorage != null && isServerNode)
            mgmtBusyExecutor.execute(this::updateAllLocalStatistics);
    }

    /**
     * Scan statistics configuration and update each key it contains.
     */
    public void updateAllLocalStatistics() {
        try {
            GridCompoundFuture<Boolean, Boolean> compoundFuture = new GridCompoundFuture<>(CU.boolReducer());

            distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> {
                StatisticsObjectConfiguration cfg = (StatisticsObjectConfiguration)v;

                compoundFuture.add(updateLocalStatisticsAsync(cfg));
            });

            compoundFuture.markInitialized();

            compoundFuture.listen(future -> {
                if (future.error() == null && !future.result())
                    mgmtBusyExecutor.execute(this::updateAllLocalStatistics);
            });
        }
        catch (IgniteCheckedException e) {
            log.warning("Unexpected statistics configuration processing error", e);
        }
    }

    /**
     * Stop tracking configuration changes.
     */
    public void stop() {
        if (log.isTraceEnabled())
            log.trace("Statistics configuration manager stopping...");

        active = false;
        mgmtBusyExecutor.deactivate();

        if (log.isDebugEnabled())
            log.debug("Statistics configuration manager stopped.");
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
            TableDescriptor tbl = schemaMgr.table(target.key().schema(), target.key().obj());

            validate(target, tbl);

            List<StatisticsColumnConfiguration> colCfgs;

            if (F.isEmpty(target.columns()))
                colCfgs = tbl.type().fields().keySet().stream()
                    .filter(col -> !QueryUtils.KEY_FIELD_NAME.equals(col) && !QueryUtils.VAL_FIELD_NAME.equals(col))
                    .map(col -> new StatisticsColumnConfiguration(col, null))
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
                throw new IgniteSQLException("Error on get or update statistic schema",
                    IgniteQueryErrorCode.UNKNOWN, ex);
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
        try {
            while (!dropStatisticsAsync(targets, validate).get())
                ; // No-op
        }
        catch (IgniteCheckedException ex) {
            if (ex.getCause() instanceof IgniteSQLException)
                throw (IgniteSQLException)ex.getCause();

            throw new IgniteSQLException("Error occurs while updating statistics schema",
                    IgniteQueryErrorCode.UNKNOWN, ex);
        }
    }

    /**
     * Drop local statistic for specified database objects on the cluster.
     * Remove local aggregated and partitioned statistics that are stored at the local metastorage.
     *
     * @param targets  DB objects to update statistics by.
     * @param validate if {@code true} - validate statistics existence, otherwise - just try to remove.
     */
    public IgniteInternalFuture<Boolean> dropStatisticsAsync(List<StatisticsTarget> targets, boolean validate) {
        if (log.isDebugEnabled())
            log.debug("Drop statistics [targets=" + targets + ']');

        GridFutureAdapter<Boolean> resultFuture = new GridFutureAdapter<>();
        IgniteInternalFuture<Boolean> chainFuture = new GridFinishedFuture<>(true);

        for (StatisticsTarget target : targets) {
            chainFuture = chainFuture.chainCompose(f -> {
                if (f.error() == null && f.result() == Boolean.TRUE)
                    return removeFromMetastore(target, validate);

                return f;
            });
        }

        chainFuture.listen(f -> {
            if (f.error() != null)
                resultFuture.onDone(f.error());
            else
                resultFuture.onDone(f.result() == null || f.result().booleanValue());
        });

        return resultFuture;
    }

    /**
     * Drop statistics target configuration from metastore.
     *
     * @param target Statistics target.
     * @param validate Validation flag.
     * @return Operation future.
     */
    private IgniteInternalFuture<Boolean> removeFromMetastore(StatisticsTarget target, boolean validate) {
        String key = key2String(target.key());

        try {
            StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

            if (validate)
                validateDropRefresh(target, oldCfg);

            if (oldCfg == null)
                return new GridFinishedFuture<>(null); //Stop future chaining. Other thread\node makes the progress.

            Set<String> dropColNames = (target.columns() == null) ? Collections.emptySet() :
                                               Arrays.stream(target.columns()).collect(Collectors.toSet());

            StatisticsObjectConfiguration newCfg = oldCfg.dropColumns(dropColNames);

            if (oldCfg.equals(newCfg))
                return new GridFinishedFuture<>(true); //Skip. Nothing to do.

            return distrMetaStorage.compareAndSetAsync(key, oldCfg, newCfg);
        }
        catch (Throwable ex) {
            return new GridFinishedFuture<>(ex);
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
    private void validateDropRefresh(@NotNull StatisticsTarget target, StatisticsObjectConfiguration cfg) {
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
     * @param tbl Corresponding table (if exists).
     */
    private void validate(StatisticsObjectConfiguration cfg, TableDescriptor tbl) {
        if (tbl == null) {
            throw new IgniteSQLException(
                "Table doesn't exist [schema=" + cfg.key().schema() + ", table=" + cfg.key().obj() + ']',
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        if (!F.isEmpty(cfg.columns())) {
            for (String col : cfg.columns().keySet()) {
                if (!tbl.type().fields().containsKey(col)) {
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

    /**
     * Subscribe to statistics configuration changed.
     *
     * @param subscriber Subscriber.
     */
    public void subscribe(Consumer<StatisticsObjectConfiguration> subscriber) {
        subscribers.add(subscriber);
    }
}

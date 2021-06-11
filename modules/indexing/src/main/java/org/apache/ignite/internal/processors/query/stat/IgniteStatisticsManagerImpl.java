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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedEnumProperty;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.NO_UPDATE;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.OFF;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.ON;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager {
    /** Size of statistics collection pool. */
    private static final int STATS_POOL_SIZE = 4;

    /** Default statistics usage state. */
    private static final StatisticsUsageState DEFAULT_STATISTICS_USAGE_STATE = ON;

    /** Interval to check statistics obsolescence in seconds. */
    private static final int OBSOLESCENCE_INTERVAL = 60;

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** SchemaManager */
    private final SchemaManager schemaMgr;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statsRepos;

    /** Ignite statistics helper. */
    private final IgniteStatisticsHelper helper;

    /** Statistics collector. */
    private final StatisticsGatherer gatherer;

    /** Statistics configuration manager. */
    private final IgniteStatisticsConfigurationManager statCfgMgr;

    /** Management pool. */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Gathering pool. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** Cluster wide statistics usage state. */
    private final DistributedEnumProperty<StatisticsUsageState> usageState = new DistributedEnumProperty<>(
        "statistics.usage.state", StatisticsUsageState::fromOrdinal, StatisticsUsageState::index, StatisticsUsageState.class);

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;

        helper = new IgniteStatisticsHelper(ctx.localNodeId(), schemaMgr, ctx::log);

        log = ctx.log(IgniteStatisticsManagerImpl.class);

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
            ctx.cache().context().database() : null;

        gatherPool = new IgniteThreadPoolExecutor("stat-gather",
            ctx.igniteInstanceName(),
            0,
            STATS_POOL_SIZE,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            ctx.uncaughtExceptionHandler()
        );

        mgmtPool = new IgniteThreadPoolExecutor("stat-mgmt",
            ctx.igniteInstanceName(),
            0,
            1,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            ctx.uncaughtExceptionHandler()
        );

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());

        IgniteStatisticsStore store;
        if (!storeData)
            store = new IgniteStatisticsDummyStoreImpl(ctx::log);
        else if (db == null)
            store = new IgniteStatisticsInMemoryStoreImpl(ctx::log);
        else
            store = new IgniteStatisticsPersistenceStoreImpl(ctx.internalSubscriptionProcessor(), db, ctx::log);

        statsRepos = new IgniteStatisticsRepository(store, ctx.systemView(), helper, ctx::log);

        gatherer = new StatisticsGatherer(
            statsRepos,
            gatherPool,
            ctx::log
        );

        statCfgMgr = new IgniteStatisticsConfigurationManager(
            schemaMgr,
            ctx.internalSubscriptionProcessor(),
            ctx.systemView(),
            ctx.state(),
            ctx.cache().context().exchange(),
            statsRepos,
            gatherer,
            mgmtPool,
            ctx::log
        );

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            usageState.addListener((name, oldVal, newVal) -> {
                if (log.isInfoEnabled())
                    log.info(String.format("Statistics usage state was changed from %s to %s", oldVal, newVal));

                if (oldVal == newVal)
                    return;

                switch (newVal) {
                    case OFF:
                        disableOperations();

                        break;
                    case ON:
                    case NO_UPDATE:
                        enableOperations();

                        break;
                }
            });

            dispatcher.registerProperty(usageState);
        });

        StatisticsUsageState currState = usageState();
        if (currState == ON || currState == NO_UPDATE)
            enableOperations();

        ctx.timeout().schedule(() -> {
            StatisticsUsageState state = usageState();
            if (state == ON && !ctx.isStopping()) {
                if (log.isTraceEnabled())
                    log.trace("Processing statistics obsolescence...");

                try {
                    processObsolescence();
                } catch (Throwable e) {
                    log.warning("Error while processing statistics obsolescence", e);
                }
            }

        }, OBSOLESCENCE_INTERVAL * 1000, OBSOLESCENCE_INTERVAL * 1000);
    }

    /**
     * Enable statistics operations.
     */
    private synchronized void enableOperations() {
        statsRepos.start();
        gatherer.start();
        statCfgMgr.start();
    }

    /**
     * Disable statistics operations.
     */
    private synchronized void disableOperations() {
        statCfgMgr.stop();
        gatherer.stop();
        statsRepos.stop();
    }

    /**
     * @return Statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(StatisticsKey key) {
        StatisticsUsageState currState = usageState();

        return (currState == ON || currState == NO_UPDATE) ? statsRepos.getLocalStatistics(key) : null;
    }

    /** {@inheritDoc} */
    @Override public void collectStatistics(StatisticsObjectConfiguration... targets) throws IgniteCheckedException {
        if (usageState() == OFF)
            throw new IgniteException("Can't gather statistics while statistics usage state is OFF.");

        statCfgMgr.updateStatistics(targets);
    }

    /** {@inheritDoc} */
    @Override public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        if (usageState() == OFF)
            throw new IgniteException("Can't drop statistics while statistics usage state is OFF.");

        statCfgMgr.dropStatistics(Arrays.asList(targets), true);
    }

    /** {@inheritDoc} */
    @Override public void refreshStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        if (usageState() == OFF)
            throw new IgniteException("Can't refresh statistics while statistics usage state is OFF.");

        statCfgMgr.refreshStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropAll() throws IgniteCheckedException {
        statCfgMgr.dropAll();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        disableOperations();

        if (gatherPool != null) {
            List<Runnable> unfinishedTasks = gatherPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection cancelled.", unfinishedTasks.size()));
        }

        if (mgmtPool != null) {
            List<Runnable> unfinishedTasks = mgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics configuration change handler cancelled.", unfinishedTasks.size()));
        }
    }

    /** */
    public IgniteStatisticsConfigurationManager statisticConfiguration() {
        return statCfgMgr;
    }

    /** {@inheritDoc} */
    @Override public void usageState(StatisticsUsageState state) throws IgniteCheckedException {
        try {
            usageState.propagate(state);
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to set usage state value due to " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public StatisticsUsageState usageState() {
        return usageState.getOrDefault(DEFAULT_STATISTICS_USAGE_STATE);
    }

    /** {@inheritDoc} */
    @Override public void onRowUpdated(String schemaName, String objName, int partId, byte[] keyBytes) {
        try {
            if (statCfgMgr.config(new StatisticsKey(schemaName, objName)) != null)
                statsRepos.addRowsModified(new StatisticsKey(schemaName, objName), partId, keyBytes);
        }
        catch (IgniteCheckedException e) {
            if (log.isInfoEnabled())
                log.info(String.format("Error while obsolescence key in %s.%s due to %s", schemaName, objName,
                    e.getMessage()));
        }
    }

    /**
     * Save dirty obsolescence info to local metastore. Check if statistics need to be refreshed and schedule it.
     */
    public synchronized void processObsolescence() {
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty = statsRepos.saveObsolescenceInfo();

        Map<StatisticsKey, List<Integer>> tasks = calculateObsolescenceRefreshTasks(dirty);

        if (!F.isEmpty(tasks))
            if (log.isTraceEnabled())
                log.trace(String.format("Refreshing statistics for %d targets", tasks.size()));

        for (Map.Entry<StatisticsKey, List<Integer>> objTask : tasks.entrySet()) {
            GridH2Table tbl = schemaMgr.dataTable(objTask.getKey().schema(), objTask.getKey().obj());

            if (tbl == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Got obsolescence statistics for unknown table %s", objTask.getKey()));

                continue;
            }

            StatisticsObjectConfiguration objCfg;
            try {
                objCfg = statCfgMgr.config(objTask.getKey());
            } catch (IgniteCheckedException e) {
                log.warning("Unable to load statistics object configuration from global metastore", e);
                continue;
            }

            if (objCfg == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Got obsolescence statistics for unknown configuration %s", objTask.getKey()));

                continue;
            }

            GridCacheContext cctx = tbl.cacheContext();

            Set<Integer> parts = cctx.affinity().primaryPartitions(
                cctx.localNodeId(), cctx.affinity().affinityTopologyVersion());

            statCfgMgr.gatherLocalStatistics(objCfg, tbl, parts, new HashSet<>(objTask.getValue()), null);
        }
    }

    /**
     * Calculate targets to refresh obsolescence statistics by map of dirty partitions.
     *
     * @param dirty Map of statistics key to list of it's dirty obsolescence info.
     * @return Map of statistics key to partition to refresh statistics.
     */
    private Map<StatisticsKey, List<Integer>> calculateObsolescenceRefreshTasks(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty
    ) {
        Map<StatisticsKey, List<Integer>> res = new HashMap<>();

        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : dirty.entrySet()) {
            StatisticsKey key = objObs.getKey();
            List<Integer> taskParts = new ArrayList<>();

            StatisticsObjectConfiguration cfg = null;
            try {
                cfg = statCfgMgr.config(key);
            }
            catch (IgniteCheckedException e) {
                log.warning("Unable to get configuration by key " + key + " due to " + e.getMessage()
                    + ". Some statistics can be outdated.");

                continue;
            }

            if (F.isEmpty(cfg.columns())) {
                statsRepos.removeObsolescenceInfo(key);
                continue;
            }

            StatisticsObjectConfiguration finalCfg = cfg;

            objObs.getValue().forEach((k, v) -> {
                ObjectPartitionStatisticsImpl partStat = statsRepos.getLocalPartitionStatistics(key, k);

                if (partStat == null || partStat.rowCount() == 0 ||
                    (double)v.modified() * 100 / partStat.rowCount() > finalCfg.maxPartitionObsolescencePercent())
                    taskParts.add(k);
            });

            if (!taskParts.isEmpty())
                res.put(key, taskParts);
        }

        return res;
    }
}

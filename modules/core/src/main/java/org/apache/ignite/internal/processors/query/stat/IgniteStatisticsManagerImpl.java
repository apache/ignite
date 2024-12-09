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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedEnumProperty;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
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

    /** Global statistics repository. */
    private final IgniteGlobalStatisticsManager globalStatsMgr;

    /** Ignite statistics helper. */
    private final IgniteStatisticsHelper helper;

    /** Statistics collector. */
    private final StatisticsProcessor statProc;

    /** Statistics configuration manager. */
    private final IgniteStatisticsConfigurationManager statCfgMgr;

    /** Management pool. */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Executor to do obsolescence management. */
    private final BusyExecutor obsolescenceBusyExecutor;

    /** Gathering pool. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** Cluster wide statistics usage state. */
    private final DistributedEnumProperty<StatisticsUsageState> usageState = new DistributedEnumProperty<>(
        "statistics.usage.state",
        "Statistics usage state. OFF - No statistics used, NO_UPDATE - Statistics used 'as is' without updates, " +
            "ON - Statistics used and updated after each changes (a default value).",
        StatisticsUsageState::fromOrdinal,
        StatisticsUsageState::index,
        StatisticsUsageState.class);

    /** Last known statistics usage state. */
    private volatile StatisticsUsageState lastUsageState = null;

    /** Started flag to prevent double start on change statistics usage state and activation and vice versa. */
    private volatile boolean started;

    /** Schedule to process obsolescence statistics. */
    private volatile GridTimeoutProcessor.CancelableTask obsolescenceSchedule;

    /** Exchange listener. */
    private final PartitionsExchangeAware exchAwareLsnr = new PartitionsExchangeAware() {
        @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
            if (fut.exchangeType() != GridDhtPartitionsExchangeFuture.ExchangeType.ALL)
                return;

            ClusterState newState = ctx.state().clusterState().state();

            if (newState.active() && !started)
                tryStart();

            if (!newState.active() && started)
                tryStop();

            if (started)
                statCfgMgr.afterTopologyUnlock(fut);
        }
    };

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        schemaMgr = ctx.query().schemaManager();

        boolean serverNode = !ctx.config().isClientMode();

        helper = new IgniteStatisticsHelper(ctx.localNodeId(), schemaMgr, ctx::log);

        log = ctx.log(IgniteStatisticsManagerImpl.class);

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
            ctx.cache().context().database() : null;

        gatherPool = (serverNode) ? new IgniteThreadPoolExecutor("stat-gather",
            ctx.igniteInstanceName(),
            0,
            STATS_POOL_SIZE,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            ctx.uncaughtExceptionHandler()
            ) : null;

        mgmtPool = new IgniteThreadPoolExecutor("stat-mgmt",
            ctx.igniteInstanceName(),
            0,
            1,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            ctx.uncaughtExceptionHandler()
        );

        obsolescenceBusyExecutor = new BusyExecutor("obsolescence", mgmtPool, ctx::isStopping, ctx::log);

        IgniteStatisticsStore store;

        if (!serverNode)
            store = new IgniteStatisticsDummyStoreImpl(ctx::log);
        else if (db == null)
            store = new IgniteStatisticsInMemoryStoreImpl(ctx::log);
        else
            store = new IgniteStatisticsPersistenceStoreImpl(ctx.internalSubscriptionProcessor(), db, ctx::log);

        statsRepos = new IgniteStatisticsRepository(store, ctx.systemView(), helper, ctx::log);

        statProc = serverNode ? new StatisticsProcessor(
            statsRepos,
            gatherPool,
            ctx::isStopping,
            ctx::log
        ) : null;

        statCfgMgr = new IgniteStatisticsConfigurationManager(
            schemaMgr,
            ctx.internalSubscriptionProcessor(),
            ctx.systemView(),
            ctx.state(),
            statProc,
            db != null,
            mgmtPool,
            ctx::isStopping,
            ctx::log,
            serverNode
        );

        globalStatsMgr = new IgniteGlobalStatisticsManager(
            this,
            ctx.systemView(),
            mgmtPool,
            ctx.discovery(),
            ctx.state(),
            ctx.cache().context().exchange(),
            helper,
            ctx.io(),
            ctx::log);

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            usageState.addListener((name, oldVal, newVal) -> {
                if (log.isInfoEnabled() && newVal != null) {
                    log.info(String.format("Statistics usage state was changed from %s to %s",
                        oldVal == null ? DEFAULT_STATISTICS_USAGE_STATE : oldVal, newVal));
                }

                lastUsageState = newVal;

                if (oldVal == newVal)
                    return;

                if (newVal == ON || newVal == NO_UPDATE)
                    tryStart();

                if (newVal == OFF)
                    tryStop();
            });

            dispatcher.registerProperty(usageState);
        });

        tryStart();

        if (serverNode)
            scheduleObsolescence(OBSOLESCENCE_INTERVAL);

        ctx.cache().context().exchange().registerExchangeAwareComponent(exchAwareLsnr);
    }

    /** */
    void scheduleObsolescence(int seconds) {
        assert seconds >= 1;

        if (obsolescenceSchedule != null)
            obsolescenceSchedule.close();

        obsolescenceSchedule = ctx.timeout().schedule(() -> obsolescenceBusyExecutor.execute(this::processObsolescence),
            seconds * 1000, seconds * 1000);
    }

    /**
     * Check all preconditions and stop if started and have reason to stop.
     */
    private synchronized void tryStop() {
        StatisticsUsageState statUsageState = usageState();

        if (!(ClusterState.ACTIVE == ctx.state().clusterState().state()
            && !ctx.isStopping()
            && (statUsageState == ON || statUsageState == NO_UPDATE))
            && started)
            stopX();
    }

    /**
     * Stop all statistics related components.
     */
    private void stopX() {
        if (log.isDebugEnabled())
            log.debug("Stopping statistics subsystem");

        globalStatsMgr.stop();
        statCfgMgr.stop();

        if (statProc != null)
            statProc.stop();

        statsRepos.stop();

        obsolescenceBusyExecutor.deactivate();
        started = false;
    }

    /**
     * Check all preconditions and start if stopped and all preconditions pass.
     */
    private synchronized void tryStart() {
        StatisticsUsageState statUsageState = usageState();

        if (ClusterState.ACTIVE == ctx.state().clusterState().state()
            && !ctx.isStopping()
            && (statUsageState == ON || statUsageState == NO_UPDATE)
            && !started)
            startX();
    }

    /**
     * Start all statistics related components.
     */
    private void startX() {
        if (log.isDebugEnabled())
            log.debug("Starting statistics subsystem...");

        obsolescenceBusyExecutor.activate();

        statsRepos.start();

        if (statProc != null)
            statProc.start();

        statCfgMgr.start();
        globalStatsMgr.start();

        started = true;
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

        return (currState == ON || currState == NO_UPDATE) ? statsRepos.getLocalStatistics(key, null) : null;
    }

    /**
     * Get local statitsics with specified topology version if exists.
     *
     * @param key Key to get statistics by.
     * @param topVer Required topology version.
     * @return Local object statistics or {@code null} if there are no statistics with requested topology version.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key, AffinityTopologyVersion topVer) {
        return statsRepos.getLocalStatistics(key, topVer);
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getGlobalStatistics(StatisticsKey key) {
        StatisticsUsageState currState = usageState();

        return (currState == ON || currState == NO_UPDATE) ? globalStatsMgr.getGlobalStatistics(key) : null;
    }

    /** {@inheritDoc} */
    @Override public void collectStatistics(StatisticsObjectConfiguration... targets) throws IgniteCheckedException {
        ensureActive("collect statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't gather statistics while statistics usage state is OFF.");

        statCfgMgr.updateStatistics(targets);
    }

    /** {@inheritDoc} */
    @Override public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        ensureActive("drop statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't drop statistics while statistics usage state is OFF.");

        statCfgMgr.dropStatistics(Arrays.asList(targets), true);
    }

    /** {@inheritDoc} */
    @Override public void refreshStatistics(StatisticsTarget... targets) {
        ensureActive("refresh statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't refresh statistics while statistics usage state is OFF.");

        statCfgMgr.refreshStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropAll() {
        ensureActive("drop all statistics");

        statCfgMgr.dropAll();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        stopX();

        if (obsolescenceSchedule != null)
            obsolescenceSchedule.close();

        if (gatherPool != null) {
            List<Runnable> unfinishedTasks = gatherPool.shutdownNow();

            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection cancelled.", unfinishedTasks.size()));
        }

        if (mgmtPool != null) {
            List<Runnable> unfinishedTasks = mgmtPool.shutdownNow();

            if (!unfinishedTasks.isEmpty()) {
                log.warning(String.format("%d statistics configuration change handler cancelled.",
                    unfinishedTasks.size()));
            }
        }
    }

    /**
     * @return Statistics configuration manager.
     */
    public IgniteStatisticsConfigurationManager statisticConfiguration() {
        return statCfgMgr;
    }

    /** {@inheritDoc} */
    @Override public void usageState(StatisticsUsageState state) {
        ensureActive("change usage state of statistics");

        try {
            usageState.propagate(state);
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to set usage state value due to " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public StatisticsUsageState usageState() {
        return (lastUsageState == null) ? DEFAULT_STATISTICS_USAGE_STATE : lastUsageState;
    }

    /** {@inheritDoc} */
    @Override public void onRowUpdated(String schemaName, String objName, int partId, byte[] keyBytes) {
        ObjectPartitionStatisticsObsolescence statObs = statsRepos.getObsolescence(
            new StatisticsKey(schemaName, objName), partId);

        if (statObs != null)
            statObs.onModified(keyBytes);
    }

    /**
     * Save dirty obsolescence info to local metastore. Check if statistics need to be refreshed and schedule it.
     *
     * 1) Get all dirty partition statistics.
     * 2) Make separate tasks for each key to avoid saving obsolescence info for removed partition (race).
     * 3) Check if partition should be recollected and add it to list in its tables task.
     * 4) Submit tasks. Actually obsolescence info will be stored during task processing.
     */
    public synchronized void processObsolescence() {
        StatisticsUsageState usageState = usageState();

        if (usageState != ON || ctx.isStopping()) {
            if (log.isDebugEnabled())
                log.debug("Skipping obsolescence processing.");

            return;
        }

        if (log.isTraceEnabled())
            log.trace("Process statistics obsolescence started.");

        List<StatisticsKey> keys = statsRepos.getObsolescenceKeys();

        if (F.isEmpty(keys)) {
            if (log.isTraceEnabled())
                log.trace("No obsolescence info found. Finish obsolescence processing.");

            return;
        }
        else {
            if (log.isTraceEnabled())
                log.trace(String.format("Scheduling obsolescence savings for %d targets", keys.size()));
        }

        for (StatisticsKey key : keys) {
            StatisticsObjectConfiguration cfg = null;

            try {
                cfg = statCfgMgr.config(key);
            }
            catch (IgniteCheckedException ignored) {
                // No-op.
            }

            Set<Integer> tasksParts = calculateObsolescencedPartitions(cfg, statsRepos.getObsolescence(key));

            TableDescriptor tbl = schemaMgr.table(key.schema(), key.obj());
            GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();

            if (tbl == null) {
                // Table can be removed earlier, but not already processed. Or somethink goes wrong. Try to reschedule.
                if (log.isDebugEnabled())
                    log.debug(String.format("Got obsolescence statistics for unknown table %s", key));
            }

            LocalStatisticsGatheringContext ctx = new LocalStatisticsGatheringContext(true,
                tbl.type(), cacheInfo, cfg, tasksParts, null);

            statProc.updateLocalStatistics(ctx);
        }
    }

    /**
     * Calculate targets to refresh obsolescence statistics by map of dirty partitions and actual per partition
     * statistics.
     *
     * @param cfg Statistics configuration
     * @param parts  list of it's obsolescence info paritions.
     * @return Map of statistics cfg to partition to refresh statistics.
     */
    private Set<Integer> calculateObsolescencedPartitions(
        StatisticsObjectConfiguration cfg,
        IntMap<ObjectPartitionStatisticsObsolescence> parts
    ) {
        Set<Integer> res = new HashSet<>();

        parts.forEach((k, v) -> {
            ObjectPartitionStatisticsImpl partStat = statsRepos.getLocalPartitionStatistics(cfg.key(), k);

            if (partStat == null || partStat.rowCount() == 0 ||
                (double)v.modified() * 100 / partStat.rowCount() > cfg.maxPartitionObsolescencePercent())
                res.add(k);
        });

        // Will add even empty list of partitions to recollect just to force obsolescence info to be stored.
        return res;
    }

    /**
     * Subscribe to all local statistics changes.
     *
     * @param subscriber Local statitics subscriber.
     */
    public void subscribeToLocalStatistics(Consumer<ObjectStatisticsEvent> subscriber) {
        statsRepos.subscribeToLocalStatistics(subscriber);
    }

    /**
     * Subscribe to all statistics configuration changed.
     *
     * @param subscriber Statistics configuration subscriber.
     */
    public void subscribeToStatisticsConfig(Consumer<StatisticsObjectConfiguration> subscriber) {
        statCfgMgr.subscribe(subscriber);
    }

    /**
     * Check that cluster is active.
     *
     * @param op Operation name.
     */
    public void ensureActive(String op) {
        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            throw new IgniteException(String.format(
                "Unable to perform %s due to cluster state [state=%s]", op, ctx.state().clusterState().state()));
    }
}

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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.task.GatherPartitionStatistics;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

/**
 * Process all tasks, related to statistics repository. Mostly - statistics collection,
 * invalidation (due to configuration, topology or obsolescence issues) and loads.
 * Input tasks should be scheduled throug management pool while gathering pool used to process heavy
 * operations in parallel.
 *
 * Manage gathering pool and it's jobs. To guarantee gracefull shutdown:
 * 1) Any job can be added into gatheringInProgress only in active state (check after adding)
 * 2) State can be disactivated only after cancelling all jobs and getting busyLock block
 * 3) Each job should do it's work in busyLock with periodically checking of it's cancellation status.
 */
public class StatisticsProcessor {
    /** Logger. */
    private final IgniteLogger log;

    /** Ignite statistics repository. */
    private final IgniteStatisticsRepository statRepo;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** (cacheGroupId -> gather context) */
    private final ConcurrentMap<StatisticsKey, LocalStatisticsGatheringContext> gatheringInProgress =
        new ConcurrentHashMap<>();

    /** Active flag (used to skip commands in inactive cluster.) */
    private volatile boolean active;

    /* Lock protection of started gathering during deactivation. */
    private static final GridBusyLock busyLock = new GridBusyLock();

    /**
     * Constructor.
     *
     * @param repo IgniteStatisticsRepository.
     * @param gatherPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsProcessor(
        IgniteStatisticsRepository repo,
        IgniteThreadPoolExecutor gatherPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.statRepo = repo;
        this.gatherPool = gatherPool;
        this.log = logSupplier.apply(StatisticsProcessor.class);
    }

    /**
     * Update statistics for the given key to actual state.
     * If byObsolescence and tbl is not {@code null} - does not clear any other partitions.
     * Should run throw management pool only.
     * 1) Replace previous gathering context if exist and needed (replace byObsolescence gathering with new one or
     * replace gathering with older configuration or topology version with new one).
     * 2) If byObsolescence and no table awailable - clean obsolescence and partition statistics for the given key.
     * 3) Submit tasks for each specified partition.
     * 4) after last task finish gathering - it starts aggregation.
     * 5) read all partitions & obsolescence from repo and
     * if byObsolescence = {@code true} - remove unnecessary one and aggregate by specified list
     * if byObsolescence = {@code false} - aggregate all presented in store (because it should contains only actual ones)
     * 5) save aggregated local statistics
     *
     * @param byObsolescence Update only obsolescence partitions.
     * @param tbl Table to update. If {@code null} - just clear all partitions and obsolescence from the repo
     * @param cfg Statistics configuration to use.
     * @param partsToProcess Partitions to update, if !byObsolescence - all primary partitions for the given topology.
     * @param topVer Topology version, can be {@code null} if tbl is null.
     */
    public void updateKeyAsync(
        boolean byObsolescence,
        GridH2Table tbl,
        StatisticsObjectConfiguration cfg,
        Set<Integer> partsToProcess,
        AffinityTopologyVersion topVer
    ) {
        if (!startJob("Updating key " + cfg.key()))
            return;

        try {
            if (log.isDebugEnabled()) {
                log.debug(String.format(
                    "Start statistics processing: byObsolescence=%b, cfg=%s, partToProcess = %s, topVer=%s",
                    byObsolescence, cfg, partsToProcess, topVer));
            }

            LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(byObsolescence, tbl, cfg,
                partsToProcess, topVer);
            LocalStatisticsGatheringContext registeredCtx = registerNewTask(newCtx);

            if (registeredCtx != null) {

                prepareTask(registeredCtx);

                if (registeredCtx.table() != null && !registeredCtx.remainingParts().isEmpty() &&
                    !registeredCtx.configuration().columns().isEmpty())
                    submitTasks(registeredCtx);
                else {
                    gatheringInProgress.remove(registeredCtx.configuration().key(), registeredCtx);

                    assert registeredCtx.remainingParts().isEmpty();
                    assert registeredCtx.finished().getCount() == 0;
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Gathered by key " + cfg.key() + " were skipped due to previous one.");
            }
        }
        finally {
            endJob();
        }
    }

    /**
     * Do preparation step before schedule any gathering.
     *
     * @param ctx Context to do preparations.
     */
    private void prepareTask(LocalStatisticsGatheringContext ctx) {
        try {
            if (ctx.byObsolescence())
                statRepo.saveObsolescenceInfo(ctx.configuration().key());

            if (ctx.table() == null || ctx.configuration().columns().isEmpty())
                statRepo.clearLocalPartitionsStatistics(ctx.configuration().key(), null);
        }
        catch (Throwable t) {
            ctx.future().cancel(true);
        }
        finally {
            // Prepare phase done
            ctx.finished().countDown();
        }
    }

    /**
     * Try to register new task.
     *
     * @param ctx Task to register.
     * @return Registered task.
     */
    private LocalStatisticsGatheringContext registerNewTask(LocalStatisticsGatheringContext ctx) {
        LocalStatisticsGatheringContext ctxToSubmit[] = new LocalStatisticsGatheringContext[1];
        LocalStatisticsGatheringContext ctxToAwait[] = new LocalStatisticsGatheringContext[1];

        gatheringInProgress.compute(ctx.configuration().key(), (k, v) -> {
            if (v == null) {
                // No old context - start new
                ctxToSubmit[0] = ctx;

                return ctx;
            }

            if ((v.byObsolescence() && ctx.byObsolescence()) ||
                (v.topologyVersion().before(ctx.topologyVersion()) ||
                    !checkStatisticsCfg(v.configuration(), ctx.configuration()))) {
                // Old context for older topology or config - cancel and start new
                v.future().cancel(true);
                ctxToAwait[0] = v;

                ctxToSubmit[0] = ctx;

                return ctx;
            }

            ctxToSubmit[0] = null;

            return v;
        });

        // Can't wait in map critical section (to allow task to try to remove itselves), but
        // have to wait here in busyLock to do gracefull shutdown.
        if (ctxToAwait[0] != null) {
            try {
                ctxToAwait[0].finished().await();
            }
            catch (InterruptedException e) {
                log.warning("Unable to wait statistics gathering task finished by key " +
                    ctx.configuration().key(), e);
            }
        }

        return ctxToSubmit[0];
    }

    /**
     * Test if statistics configuration is fit to all required versions and doesn't
     * contain any extra column configurations.
     *
     * @param existingCfg Statistics configuration to check.
     * @param targetCfg Target configuration.
     * @return {@code true} if it is, {@code false} otherwise.
     */
    private boolean checkStatisticsCfg(
        StatisticsObjectConfiguration existingCfg,
        StatisticsObjectConfiguration targetCfg
    ) {
        if (existingCfg == targetCfg)
            return true;

        if (existingCfg.columns().size() != targetCfg.columns().size())
            return false;

        for (Map.Entry<String, StatisticsColumnConfiguration> targetColCfgEntry : targetCfg.columns().entrySet()) {
            StatisticsColumnConfiguration existingColCfg = existingCfg.columns().get(targetColCfgEntry.getKey());

            if (existingColCfg == null || existingColCfg.version() < targetColCfgEntry.getValue().version())
                return false;
        }

        return true;
    }

    /**
     * Generate and subtim tasks into gathering pool for existing gathering context.
     *
     * @param ctx Context to generate tasks by.
     */
    private void submitTasks(LocalStatisticsGatheringContext ctx) {
        for (int part : ctx.remainingParts()) {
            final GatherPartitionStatistics task = new GatherPartitionStatistics(
                statRepo,
                ctx,
                part,
                log
            );

            submitTask(ctx, task);
        }
    }

    /**
     *
     * @param ctx
     */
    private void aggregateStatistics(LocalStatisticsGatheringContext ctx) {
        if (checkCancelled(ctx))
            return;

        StatisticsKey key = ctx.configuration().key();
        Collection<ObjectPartitionStatisticsImpl> allParts = statRepo.getLocalPartitionsStatistics(key);

        if (ctx.byObsolescence()) {
            if (ctx.configuration() == null)
                return;

            statRepo.aggregatedLocalStatistics(allParts, ctx.configuration());
        }
        else {
            Set<Integer> partsToRemove = new HashSet<>();
            Collection<ObjectPartitionStatisticsImpl> partsToAggregate = new ArrayList<>();

            for (ObjectPartitionStatisticsImpl partStat : allParts) {
                if (ctx.allParts() == null || !ctx.allParts().contains(partStat.partId()))
                    partsToRemove.add(partStat.partId());
                else
                    partsToAggregate.add(partStat);
            }

            if (!partsToRemove.isEmpty())
                statRepo.clearLocalPartitionIdsStatistics(ctx.configuration().key(), partsToRemove);

            if (!partsToAggregate.isEmpty())
                statRepo.aggregatedLocalStatistics(partsToAggregate, ctx.configuration());
        }
    }

    /**
     * Check if specified context cancelled. If so - log debug message and return {@code true}.
     *
     * @param ctx Gathering context to check.
     * @return {@code true} if context was cancelled, {@code false} - otherwise.z
     */
    private boolean checkCancelled(LocalStatisticsGatheringContext ctx) {
        if (ctx.future().isCancelled()) {
            if (log.isDebugEnabled())
                log.debug("Gathering context by key " + ctx.configuration().key() + " cancelled.");

            return true;
        }

        return false;
    }

    /**
     * Try to get busyLock and check active state. Return success flag.
     * If unsuccess - release busyLock.
     *
     * @param operation Text description of operation to log.
     * @return {@code true}
     */
    private boolean startJob(String operation) {
        if (!busyLock.enterBusy()) {
            if (log.isDebugEnabled())
                log.debug("Unable to start " + operation + " due to inactive state.");

            return false;
        }

        if (!active) {
            if (log.isDebugEnabled())
                log.debug("Unable to start " + operation + " due to inactive state.");

            busyLock.leaveBusy();

            return false;
        }

        return true;
    }

    /**
     * Just unlock busyLock.
     */
    private void endJob() {
        busyLock.leaveBusy();
    }

    /**
     * Mark partition task failed. If that was the last partition -
     * finalize ctx and remove it from gathering in progress.
     *
     * @param ctx Context to fishish partition in.
     * @param partId Partition id.
     */
    private void failPartTask(LocalStatisticsGatheringContext ctx, int partId) {
        // Partition skipped
        ctx.finished().countDown();

        // No need to gather rest partitions.
        ctx.future().cancel(true);

        if (log.isDebugEnabled())
            log.debug(String.format("Gathering failed for key %s.%d ", ctx.configuration().key(), partId));

        if (ctx.partitionNotAvailable(partId)) {
            gatheringInProgress.remove(ctx.configuration().key(), ctx);

            assert ctx.finished().getCount() == 0;

            if (log.isDebugEnabled())
                log.debug(String.format("Gathering removed for key %s", ctx.configuration().key()));
        }
    }

    /**
     * Submit partition gathering task.
     *
     * @param ctx Gathering context to track state.
     * @param task Gathering task to proceed.
     */
    private void submitTask(
        final LocalStatisticsGatheringContext ctx,
        final GatherPartitionStatistics task
    ) {
        gatherPool.submit(() -> {
            if (!startJob("Gathering partition statistics by key " + ctx.configuration().key())) {
                failPartTask(ctx, task.partition());

                return;
            }
            try {
                GatheredPartitionResult gatherRes = new GatheredPartitionResult();

                try {
                    gatherCtxPartition(ctx, task, gatherRes);

                    completePartitionStatistic(gatherRes.newPart, ctx, task.partition(), gatherRes.partStats);
                }
                catch (Throwable t) {
                    //failPartTask(ctx, task.partition());
                    gatherRes.partStats = null;

                    if (t instanceof GatherStatisticCancelException) {
                        if (log.isDebugEnabled()) {
                            log.debug("Collect statistics task was cancelled " +
                                "[key=" + ctx.configuration().key() + ", part=" + task.partition() + ']');
                        }
                    }
                    else
                        log.warning("Unexpected error on statistic gathering", t);
                }
                finally {
                    if (gatherRes.partStats == null)
                        failPartTask(ctx, task.partition());
                    else {
                        // Finish partition task
                        ctx.finished().countDown();

                        if (ctx.partitionDone(task.partition()))
                            gatheringInProgress.remove(ctx.configuration().key(), ctx);
                    }
                }
            }
            finally {
                endJob();
            }
        });
    }

    private void gatherCtxPartition(
        final LocalStatisticsGatheringContext ctx,
        final GatherPartitionStatistics task,
        GatheredPartitionResult result
    ) {
        if (!ctx.byObsolescence()) {
            // Try to use existing partition statistics instead of gather new one
            ObjectPartitionStatisticsImpl existingPartStat = statRepo.getLocalPartitionStatistics(
                ctx.configuration().key(), task.partition());

            if (existingPartStat != null && partStatFit(existingPartStat, ctx.configuration()))
                result.partStats = existingPartStat;
        }

        result.newPart = result.partStats == null;

        if (result.partStats == null)
            result.partStats = task.call();
    }

    /**
     * Check if specified object stistic fully meet specified statistics object configuration.
     *
     * @param stat Object statistics to test.
     * @param cfg Statistics object configuration to compare with.
     * @return {@code true} if specified statistics fully meet to specified configuration requiremenrs,
     *         {@code false} - otherwise.
     */
    private boolean partStatFit(ObjectStatisticsImpl stat, StatisticsObjectConfiguration cfg) {
        if (stat == null)
            return false;

        if (stat.columnsStatistics().size() != cfg.columns().size())
            return false;

        for (StatisticsColumnConfiguration colCfg : cfg.columns().values()) {
            ColumnStatistics colStat = stat.columnStatistics(colCfg.name());

            if (colStat == null || colCfg.version() > colStat.version())
                return false;
        }

        return true;
    }

    /**
     * Run task on busy lock.
     *
     * @param r Task to run.
     */
    public void busyRun(Runnable r) {
        if (!busyLock.enterBusy())
            return;

        try {
            if (!active)
                return;

            r.run();
        }
        finally {
            busyLock.leaveBusy();
        }

    }

    /**
     * Complete gathering of partition statistics: save to repository and try to complete whole task.
     *
     * @param newStat If {@code true} - partition statitsics was just gathered and need to be saved to repo.
     * @param ctx Gathering context.
     * @param partId Partition id.
     * @param partStat Collected statistics or {@code null} if it was impossible to gather current partition.
     */
    private void completePartitionStatistic(
        boolean newStat,
        LocalStatisticsGatheringContext ctx,
        int partId,
        ObjectPartitionStatisticsImpl partStat
    ) {
        if (ctx.future().isCancelled() || partStat == null)
            return;

        StatisticsKey key = ctx.configuration().key();

        if (newStat) {
            if (ctx.configuration().columns().size() == partStat.columnsStatistics().size())
                statRepo.refreshObsolescence(key, partId);

            statRepo.replaceLocalPartitionStatistics(key, partStat);
            //statRepo.saveLocalPartitionStatistics(key, partStat);

            if (log.isDebugEnabled())
                log.debug("Local partitioned statistic saved [stat=" + partStat + ']');
        }
        if (ctx.partitionDone(partId)) {
            if (log.isDebugEnabled())
                log.debug("Local partitions statistics successfully gathered by key " +
                    ctx.configuration().key());

            aggregateStatistics(ctx);
        }
    }

    /**
     * Get gathering context by key.
     *
     * @param key Statistics key.
     * @return Gathering in progress or {@code null} if there are no active gathering by specified key.
     */
    public LocalStatisticsGatheringContext gatheringInProgress(StatisticsKey key) {
        return gatheringInProgress.get(key);
    }

    /**
     * Start gathering.
     */
    public void start() {
        if (log.isDebugEnabled())
            log.debug("Statistics gathering started.");

        active = true;
    }

    /**
     * Stop gathering.
     */
    public void stop() {
        if (log.isTraceEnabled())
            log.trace(String.format("Statistics gathering stopping %d task...", gatheringInProgress.size()));

        active = false;

        cancelAllTasks();

        if (log.isDebugEnabled())
            log.debug("Statistics gathering stopped.");
    }

    /**
     * Cancel all currently running statistics gathering tasks.
     */
    public void cancelAllTasks() {
        gatheringInProgress.values().forEach(g -> g.future().cancel(true));
        // Can skip waiting for each task finished because of global busyLock.

        gatheringInProgress.clear();

        busyLock.block();
        busyLock.unblock();
    }

    /**
     * Get gathering pool.
     *
     * @return Gathering pool.
     */
    public IgniteThreadPoolExecutor gatheringPool() {
        return gatherPool;
    }

    /**
     * Gathering result.
     */
    private static class GatheredPartitionResult {
        /** Partition statistics was gathered, not just read. */
        public boolean newPart;

        /** Partition statistics. */
        public ObjectPartitionStatisticsImpl partStats;
    }
}

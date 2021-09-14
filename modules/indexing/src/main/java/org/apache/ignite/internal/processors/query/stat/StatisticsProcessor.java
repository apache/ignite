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
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.task.GatherPartitionStatistics;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.typedef.X;
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
     * If byObsolescence - does not clear any other partitions.
     * Should run throw management pool only.
     * 1) Replace previous gathering context if exist and needed (replace byObsolescence gathering with new one or
     * replace gathering with older configuration or topology version with new one)
     * 2) If byObsolescence and no table awailable - clean obsolescence and partition statistics for the given key
     * 3) Submit tasks for each specified partition
     * 4) after last task finish gathering - it start aggregation: read all partitions & obsolescence from repo and if !byObsolescence
     * - remove unnecessary one, aggregate all necessary (all for byObsolescence, all not deleted - for !byObsolescence)
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

        if (log.isDebugEnabled()) {
            log.debug(String.format("Start statistics processing: byObsolescence=%b, cfg=%s, topVer=%s",
                byObsolescence, cfg, topVer));
        }

        try {
            LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(byObsolescence, tbl, cfg,
                partsToProcess, topVer);
            LocalStatisticsGatheringContext ctxToSubmit[] = new LocalStatisticsGatheringContext[1];

            gatheringInProgress.compute(cfg.key(), (k, v) -> {
                if (v == null) {
                    // No old context - start new
                    ctxToSubmit[0] = newCtx;
                    return newCtx;
                }

                if ((v.byObsolescence() && byObsolescence) ||
                    (v.topologyVersion().before(topVer) || !checkStatisticsCfg(v.configuration(), cfg))) {
                    // Old context for older topology or config - cancel and start new
                    v.future().cancel(true);

                    try {
                        newCtx.finished().await();
                    }
                    catch (InterruptedException e) {
                        log.warning("Unable to wait statistics gathering task finished by key " + cfg.key(), e);
                    }

                    ctxToSubmit[0] = newCtx;
                    return newCtx;
                }

                ctxToSubmit[0] = null;
                return v;
            });

            if (ctxToSubmit[0] != null) {

                if (byObsolescence) {
//                    if (primaryPartitions
//                     (tbl == null || cfg.columns().isEmpty()))
                    // Double check that repository is clear
                    // (it's enough to just clear obsolescence with statRepo.removeObsolescenceInfo(cfg.key()); here)
                    if (tbl == null)
                        // No table - just clean store
                        statRepo.clearLocalPartitionsStatistics(cfg.key(), null);
                    else
                        statRepo.saveObsolescenceInfo(cfg.key());
                }

                if (tbl != null && !partsToProcess.isEmpty())
                    submitTasks(ctxToSubmit[0]);
            }

            if (log.isDebugEnabled())
                log.debug("Statitics processing finished.");
        }
        finally {
            endJob();
        }
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

//    /**
//     * Schedule supplied statistics aggregation and return task context.
//     *
//     * @param key Statistics key to aggregate statistics by.
//     * @param aggregate Statistics to aggregate supplier.
//     * @return Task context.
//     */
//    public LocalStatisticsGatheringContext aggregateStatisticsAsync(
//        final StatisticsKey key,
//        Supplier<ObjectStatisticsImpl> aggregate
//    ) {
//        final LocalStatisticsGatheringContext ctx = new LocalStatisticsGatheringContext(Collections.emptySet());
//
//        // Only refresh local aggregates.
//        ctx.futureGather().complete(true);
//
//        LocalStatisticsGatheringContext inProgressCtx = gatheringInProgress.putIfAbsent(key, ctx);
//
//        if (inProgressCtx == null) {
//            CompletableFuture<ObjectStatisticsImpl> f = CompletableFuture.supplyAsync(
//                () -> {
//                    boolean entered;
//                    if (!(entered = busyLock.enterBusy()) || !active) {
//                        if (entered)
//                            busyLock.leaveBusy();
//
//                        if (log.isDebugEnabled())
//                            log.debug("Can't aggregate statistics by key " + key + " due to inactive state.");
//
//                        gatheringInProgress.remove(key, ctx);
//                        ctx.cancel();
//
//                        return null;
//                    }
//
//                    try {
//                        return aggregate.get();
//                    }
//                    finally {
//                        busyLock.leaveBusy();
//                    }
//
//            }, gatherPool);
//
//            f.handle((stat, ex) -> {
//                if (ex == null)
//                    ctx.futureAggregate().complete(stat);
//                else
//                    ctx.futureAggregate().completeExceptionally(ex);
//
//                gatheringInProgress.remove(key, ctx);
//
//                return null;
//            });
//
//            return ctx;
//        }
//        else {
//            inProgressCtx.futureGather().thenAcceptAsync((complete) -> {
//                if (complete) {
//                    if (busyLock.enterBusy()) {
//                        try {
//                            if (!active) {
//                                inProgressCtx.cancel();
//
//                                return;
//                            }
//
//                            ObjectStatisticsImpl stat = aggregate.get();
//
//                            inProgressCtx.futureAggregate().complete(stat);
//
//                            if (log.isDebugEnabled())
//                                log.debug("Local statistics for key " + key + " aggregated succesfully");
//                        }
//                        finally {
//                            busyLock.leaveBusy();
//                        }
//                    }
//                    else
//                        inProgressCtx.futureAggregate().cancel(true);
//                }
//                else
//                    inProgressCtx.futureAggregate().complete(null);
//
//                gatheringInProgress.remove(key, inProgressCtx);
//            }, gatherPool);
//
//            return inProgressCtx;
//        }
//    }

//    /**
//     * Collect statistic per partition for specified object.
//     *
//     * @param tbl Table to gather statistics by.
//     * @param cfg Statistics configuration.
//     * @param parts Partitions to gather.
//     * @return Operation context.
//     */
//    public LocalStatisticsGatheringContext gatherLocalObjectsStatisticsAsync(
//        GridH2Table tbl,
//        StatisticsObjectConfiguration cfg,
//        Set<Integer> parts
//    ) {
//        if (!startJob("statistics gathering by key" + cfg.key()))
//            return null;
//
//        try {
//            final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(tbl, cfg, parts);
//
//            LocalStatisticsGatheringContext oldCtx = gatheringInProgress.put(cfg.key(), newCtx);
//
//            if (oldCtx != null) {
//                if (log.isDebugEnabled())
//                    log.debug("Cancel previous statistic gathering for [key=" + cfg.key() + ']');
//
//                oldCtx.future().cancel(true);
//            }
//
//            for (int part : parts) {
//                final GatherPartitionStatistics task = new GatherPartitionStatistics(
//                    statRepo,
//                    newCtx,
//                    part,
//                    log
//                );
//
//                submitTask(newCtx, task);
//            }
//
//            return newCtx;
//
//        }
//        finally {
//            endJob();
//        }
//
//        StatisticsKey key = new StatisticsKey(tbl.getSchema().getName(), tbl.getName());
//
//        if (log.isDebugEnabled()) {
//            log.debug("Start statistics gathering [key=" + key +
//                ", cfg=" + cfg +
//                ", parts=" + parts + ']');
//        }
//
//        final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(tbl, cfg, parts);
//
//        LocalStatisticsGatheringContext oldCtx = gatheringInProgress.put(key, newCtx);
//
//        if (oldCtx != null) {
//            if (log.isDebugEnabled())
//                log.debug("Cancel previous statistic gathering for [key=" + key + ']');
//
//            oldCtx.cancel();
//        }
//
//        if (!active) {
//            newCtx.cancel();
//            gatheringInProgress.remove(key);
//
//            if (log.isDebugEnabled())
//                log.debug("Reject aggregation by key " + key + " due to inactive state.");
//        }
//
//        for (int part : parts) {
//            final GatherPartitionStatistics task = new GatherPartitionStatistics(
//                statRepo,
//                newCtx,
//                part,
//                log
//            );
//
//            submitTask(newCtx, task);
//        }
//
//        return newCtx;
//    }

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
            if (!startJob("Gathering partition statistics by key " + ctx.configuration().key()))
                return;

            try {
                ObjectPartitionStatisticsImpl partStat = null;

                if (!ctx.byObsolescence()) {
                    // Try to use existing partition statistics instead of gather new one
                    ObjectPartitionStatisticsImpl existingPartStat = statRepo.getLocalPartitionStatistics(
                        ctx.configuration().key(), task.partition());

                    if (existingPartStat != null && partStatFit(existingPartStat, ctx.configuration()))
                        partStat = existingPartStat;
                }
                boolean newStat = partStat == null;

                if (partStat == null)
                    partStat = task.call();

                completePartitionStatistic(newStat, ctx, task.partition(), partStat);
            }
            catch (Throwable t) {
                if (t instanceof GatherStatisticCancelException) {
                    if (log.isDebugEnabled()) {
                        log.debug("Collect statistics task was cancelled " +
                            "[key=" + ctx.configuration().key() + ", part=" + task.partition() + ']');
                    }
                }
                else {
                    log.error("Unexpected error on statistic gathering", t);

                    ctx.future().cancel(true);
                }
            }
            finally {
                ctx.finished().countDown();
                endJob();
            }
        });
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
     * @param part Partition id.
     * @param partStat Collected statistics or {@code null} if it was impossible to gather current partition.
     */
    private void completePartitionStatistic(
        boolean newStat,
        LocalStatisticsGatheringContext ctx,
        int part,
        ObjectPartitionStatisticsImpl partStat
    ) {
        if (ctx.future().isCancelled())
            return;

        StatisticsKey key = ctx.configuration().key();

        try {
            if (partStat == null)
                ctx.partitionNotAvailable(part);
            else {
                if (newStat) {
                    if (ctx.configuration().columns().size() == partStat.columnsStatistics().size())
                        statRepo.refreshObsolescence(key, part);

                    statRepo.saveLocalPartitionStatistics(key, partStat);

                    if (log.isDebugEnabled())
                        log.debug("Local partitioned statistic saved [stat=" + partStat + ']');
                }

                if (ctx.partitionDone(part)) {
                    if (log.isDebugEnabled())
                        log.debug("Local partitions statistics successfully gathered by key " + key);

                    aggregateStatistics(ctx);
                }
            }
        }
        catch (Throwable ex) {
            if (!X.hasCause(ex, NodeStoppingException.class))
                log.error("Unexpected error on statistic save", ex);
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
}

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
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.task.GatherPartitionStatistics;
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
    private final BusyExecutor gatheringBusyExecutor;

    /** (cacheGroupId -> gather context) */
    private final ConcurrentMap<StatisticsKey, LocalStatisticsGatheringContext> gatheringInProgress =
        new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param repo IgniteStatisticsRepository.
     * @param gatherPool Thread pool to gather statistics in.
     * @param stopping External stopping state supplier.
     * @param logSupplier Log supplier function.
     */
    public StatisticsProcessor(
        IgniteStatisticsRepository repo,
        IgniteThreadPoolExecutor gatherPool,
        Supplier<Boolean> stopping,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.statRepo = repo;
        this.gatheringBusyExecutor = new BusyExecutor("gathering", gatherPool, stopping, logSupplier);
        this.log = logSupplier.apply(StatisticsProcessor.class);
    }

    /**
     * Update statistics for the given key to actual state.
     * If byObsolescence and tbl is not {@code null} - does not clear any other partitions.
     *
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
     * @param ctx Statistics Gathering context.
     */
    public void updateLocalStatistics(LocalStatisticsGatheringContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug(String.format(
                "Start statistics processing: forceRecollect=%b, cfg=%s, partToProcess = %s, topVer=%s",
                ctx.forceRecollect(), ctx.configuration(), ctx.allParts(), ctx.topologyVersion()));
        }

        if (registerNewTask(ctx)) {
            try {
                if (ctx.forceRecollect())
                    // To save all obsolescence info even if there is no partitions to recollect.
                    statRepo.saveObsolescenceInfo(ctx.configuration().key());

                if (ctx.table() == null || ctx.configuration() == null || ctx.configuration().columns().isEmpty()) {
                    statRepo.clearLocalPartitionsStatistics(ctx.configuration().key(), null);
                    ctx.future().complete(null);

                    return;
                }

                if (ctx.remainingParts().isEmpty())
                    ctx.future().complete(null);
                else
                    submitTasks(ctx);
            }
            catch (Throwable t) {
                // Submit tasks can't fire an error, so no need to cancel and wait for tasks finished here.
                ctx.future().completeExceptionally(t);
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Gathered by key " + ctx.configuration().key() + " were skipped due to previous one.");
        }
    }

    /**
     * Try to register new task. Returned task will remove itself from gatheringInProgress after completion.
     * If there are some other task for the given key - operation will be scheduled right after it if necessary
     * (current task have newer configuration or topology).
     *
     * @param ctx Task to register.
     * @return {@code true} if task was actually pushed as Gathering in progress task, {@code false} - otherwise.
     */
    private boolean registerNewTask(LocalStatisticsGatheringContext ctx) {
        boolean res[] = new boolean[1];

        gatheringInProgress.compute(ctx.configuration().key(), (k, v) -> {
            if (v == null) {
                // No old context - start new
                res[0] = true;

                ctx.future().whenComplete((r, t) -> {
                    if (t != null) {
                        if (t instanceof CancellationException || t instanceof NodeStoppingException) {
                            if (log.isDebugEnabled())
                                log.debug("Got " + t.getClass() + " exception during statistics collection by key "
                                    + ctx.configuration().key() + "." );
                        }
                        else
                            log.warning("Unexpected error during statistics collection by key " +
                                ctx.configuration().key() + " . " + t.getMessage(), t);
                    }

                    // Anyway - try to remove itself from gathering map.
                    gatheringInProgress.remove(ctx.configuration().key(), ctx);
                });

                return ctx;
            }

            // If there are key - check if we can cancel it.
            if (v.topologyVersion() == null ||
                (ctx.topologyVersion() != null && v.topologyVersion().compareTo(ctx.topologyVersion()) < 0) ||
                v.configuration().compareTo(ctx.configuration()) < 0) {
                // Old context for older topology or config - cancel and start new
                v.cancel();

                v.future().whenComplete((r, t) -> {
                    // Will be executed before original, so have to try to cancel previous context to add new one.
                    gatheringInProgress.remove(ctx.configuration().key(), v);

                    boolean rescheduled = gatheringBusyExecutor.busyRun(() -> updateLocalStatistics(ctx));

                    if (!rescheduled && log.isDebugEnabled()) {
                        log.debug("Unable to reschedule statistics task by key " + ctx.configuration().key()
                            + " due to inactive state.");
                    }

                });

                res[0] = false;

                return v;
            }

            res[0] = false;

            return v;
        });

        return res[0];
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

            gatheringBusyExecutor.submit(new CancellableTask() {
                    /** {@inheritDoc} */
                    @Override public void run() {
                        processPartitionTask(task);
                    }

                    /** {@inheritDoc} */
                    @Override public void cancel() {
                        task.context().cancel();
                    }
                })
                .thenAccept(success -> {
                    if (!success) {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Gathering failed for key %s.%d ", ctx.configuration().key(),
                                task.partition()));
                        }

                        ctx.partitionNotAvailable(task.partition());
                    }
                });
        }
    }

    /**
     * Aggregate partition statistics to local one.
     * @param ctx Context to use in aggregation.
     */
    private void aggregateStatistics(LocalStatisticsGatheringContext ctx) {
        if (ctx.cancelled())
            return;

        StatisticsKey key = ctx.configuration().key();
        Collection<ObjectPartitionStatisticsImpl> allParts = statRepo.getLocalPartitionsStatistics(key);

        if (ctx.forceRecollect())
            statRepo.aggregatedLocalStatistics(allParts, ctx.configuration(), ctx.topologyVersion());
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
                statRepo.clearLocalPartitionsStatistics(ctx.configuration().key(), partsToRemove);

            if (!partsToAggregate.isEmpty())
                statRepo.aggregatedLocalStatistics(partsToAggregate, ctx.configuration(), ctx.topologyVersion());
        }
    }

    /**
     * Process partition: call gather task, mark partition done, aggregate and complete future if needed.
     * @param task Partition gathering task to process.
     */
    private void processPartitionTask(final GatherPartitionStatistics task) {
        LocalStatisticsGatheringContext ctx = task.context();

        try {
            task.call();

            if (ctx.partitionDone(task.partition())) {
                aggregateStatistics(ctx);

                if (log.isDebugEnabled()) {
                    log.debug("Local partitions statistics successfully gathered by key " +
                        ctx.configuration().key());
                }

                ctx.future().complete(null);
            }
        }
        catch (Throwable t) {
            ctx.partitionNotAvailable(task.partition());

            if (t instanceof GatherStatisticCancelException) {
                if (log.isDebugEnabled()) {
                    log.debug("Collect statistics task was cancelled " +
                        "[key=" + ctx.configuration().key() + ", part=" + task.partition() + ']');
                }
            }
            else if (t.getCause() instanceof NodeStoppingException) {
                if (log.isDebugEnabled())
                    log.debug("Node stopping during statistics collection on " +
                        "[key=" + ctx.configuration().key() + ", part=" + task.partition() + ']');
            }
            else
                log.warning("Unexpected error on statistic gathering", t);
        }
    }

    /**
     * Check if specified object stistic fully meet specified statistics object configuration.
     *
     * @param stat Object statistics to test.
     * @param cfg Statistics object configuration to compare with.
     * @return {@code true} if specified statistics fully meet to specified configuration requiremenrs,
     *         {@code false} - otherwise.
     */
    private boolean partStatSuitToConfiguration(ObjectStatisticsImpl stat, StatisticsObjectConfiguration cfg) {
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
     * Start gathering.
     */
    public void start() {
        if (log.isDebugEnabled())
            log.debug("Statistics gathering started.");

        gatheringBusyExecutor.activate();
    }

    /**
     * Stop gathering.
     */
    public void stop() {
        if (log.isTraceEnabled())
            log.trace(String.format("Statistics gathering stopping %d task...", gatheringInProgress.size()));

        // Can skip waiting for each task finished because of global busyLock.
        gatheringBusyExecutor.deactivate();

        if (log.isDebugEnabled())
            log.debug("Statistics gathering stopped.");
    }
}

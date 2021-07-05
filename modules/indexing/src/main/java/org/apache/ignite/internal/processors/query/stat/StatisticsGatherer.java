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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.task.GatherPartitionStatistics;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.h2.table.Column;

/**
 * Implementation of statistic collector.
 */
public class StatisticsGatherer {
    /** Logger. */
    private final IgniteLogger log;

    /** Ignite statistics repository. */
    private final IgniteStatisticsRepository statRepo;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** (cacheGroupId -> gather context) */
    private final ConcurrentMap<StatisticsKey, LocalStatisticsGatheringContext> gatheringInProgress = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param repo IgniteStatisticsRepository.
     * @param gatherPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsGatherer(
        IgniteStatisticsRepository repo,
        IgniteThreadPoolExecutor gatherPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatherer.class);
        this.statRepo = repo;
        this.gatherPool = gatherPool;
    }

    /**
     * Schedule supplied statistics aggregation and return task context.
     *
     * @param key Statistics key to aggregate statistics by.
     * @param aggregate Statistics to aggregate supplier.
     * @return Task context.
     */
    public LocalStatisticsGatheringContext aggregateStatisticsAsync(
        final StatisticsKey key,
        Supplier<ObjectStatisticsImpl> aggregate
    ) {
        final LocalStatisticsGatheringContext ctx = new LocalStatisticsGatheringContext(Collections.emptySet());

        // Only refresh local aggregates.
        ctx.futureGather().complete(true);

        LocalStatisticsGatheringContext inProgressCtx = gatheringInProgress.putIfAbsent(key, ctx);

        if (inProgressCtx == null) {
            CompletableFuture<ObjectStatisticsImpl> f = CompletableFuture.supplyAsync(aggregate, gatherPool);

            f.handle((stat, ex) -> {
                if (ex == null)
                    ctx.futureAggregate().complete(stat);
                else
                    ctx.futureAggregate().completeExceptionally(ex);

                gatheringInProgress.remove(key, ctx);

                return null;
            });

            return ctx;
        }
        else {
            inProgressCtx.futureGather().thenAccept((complete) -> {
                if (complete) {
                    ObjectStatisticsImpl stat = aggregate.get();

                    inProgressCtx.futureAggregate().complete(stat);
                }
                else
                    inProgressCtx.futureAggregate().complete(null);
            });

            return inProgressCtx;
        }
    }

    /**
     * Collect statistic per partition for specified object.
     *
     * @param tbl Table to gather statistics by.
     * @param cfg Statistics configuration.
     * @param colCfgs Column to gathering configuration.
     * @param parts Partitions to gather.
     * @return Operation context.
     */
    public LocalStatisticsGatheringContext gatherLocalObjectsStatisticsAsync(
        GridH2Table tbl,
        StatisticsObjectConfiguration cfg,
        Map<String, StatisticsColumnConfiguration> colCfgs,
        Set<Integer> parts
    ) {
        StatisticsKey key = new StatisticsKey(tbl.getSchema().getName(), tbl.getName());
        Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), colCfgs.keySet());

        if (log.isDebugEnabled()) {
            log.debug("Start statistics gathering [key=" + key +
                ", cols=" + Arrays.toString(cols) +
                ", cfgs=" + colCfgs +
                ", parts=" + parts + ']');
        }

        final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(parts);

        LocalStatisticsGatheringContext oldCtx = gatheringInProgress.put(key, newCtx);

        if (oldCtx != null) {
            if (log.isDebugEnabled())
                log.debug("Cancel previous statistic gathering for [key=" + key + ']');

            oldCtx.futureGather().cancel(false);
        }

        for (int part : parts) {
            final GatherPartitionStatistics task = new GatherPartitionStatistics(
                newCtx,
                tbl,
                cols,
                colCfgs,
                part,
                log
            );

            submitTask(tbl, cfg, key, newCtx, task);
        }

        return newCtx;
    }

    /** */
    private void submitTask(
        final GridH2Table tbl,
        final StatisticsObjectConfiguration cfg,
        final StatisticsKey key,
        final LocalStatisticsGatheringContext ctx,
        final GatherPartitionStatistics task
    ) {
        CompletableFuture<ObjectPartitionStatisticsImpl> f = CompletableFuture.supplyAsync(task::call, gatherPool);

        f.thenAccept((partStat) -> {
            completePartitionStatistic(tbl, cfg, key, ctx, task.partition(), partStat);
        });

        f.exceptionally((ex) -> {
            if (ex instanceof GatherStatisticCancelException) {
                if (log.isDebugEnabled()) {
                    log.debug("Collect statistics task was cancelled " +
                        "[key=" + key + ", part=" + task.partition() + ']');
                }
            }
            else {
                log.error("Unexpected error on statistic gathering", ex);

                ctx.futureGather().obtrudeException(ex);
            }

            return null;
        });
    }

    /**
     * Complete gathering of partition statistics: save to repository and try to complete whole task.
     *
     * @param tbl Table to gather statistics by.
     * @param cfg Statistics configuration.
     * @param key Key to gather statistics by.
     * @param ctx Task context.
     * @param part Partition id.
     * @param partStat Collected statistics or {@code null} if it was impossible to gather current partition.
     */
    private void completePartitionStatistic(
        GridH2Table tbl,
        StatisticsObjectConfiguration cfg,
        StatisticsKey key,
        LocalStatisticsGatheringContext ctx,
        int part,
        ObjectPartitionStatisticsImpl partStat
    ) {
        try {
            if (partStat == null)
                ctx.partitionNotAvailable(part);
            else {
                statRepo.saveLocalPartitionStatistics(
                    new StatisticsKey(tbl.getSchema().getName(), tbl.getName()),
                    partStat
                );

                if (cfg.columns().size() == partStat.columnsStatistics().size())
                    statRepo.refreshObsolescence(key, part);

                if (log.isDebugEnabled())
                    log.debug("Local partitioned statistic saved [stat=" + partStat + ']');

                ctx.partitionDone(part);
            }

            if (ctx.futureGather().isDone())
                gatheringInProgress.remove(key, ctx);
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
    }

    /**
     * Stop gathering.
     */
    public void stop() {
        if (log.isTraceEnabled())
            log.trace(String.format("Statistics gathering stopping %d task...", gatheringInProgress.size()));

        gatheringInProgress.values().forEach(ctx -> ctx.futureGather().cancel(true));

        gatheringInProgress.clear();

        if (log.isDebugEnabled())
            log.debug("Statistics gathering stopped.");
    }
}

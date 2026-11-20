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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsCacheSizeTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry.STATISTICS_CACHE_SIZE_TASK_FEATURE;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;

/** Calcite statistics wrapper. */
public class IgniteStatisticsImpl implements Statistic {
    /** Minimum interval between row count refresh attempts, in milliseconds. */
    static final long STATS_REFRESH_INTERVAL = 2_000;

    /** */
    private static final long LOCAL_CACHE_SIZE_TIMEOUT = 1_000;

    /** Fallback estimate until a row count measurement succeeds. */
    static final long DFLT_ROW_CNT = 1_000;

    /** */
    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    /** */
    private final AtomicBoolean updateInProgress = new AtomicBoolean();

    /** Time the last refresh attempt finished, or zero if none has finished. */
    volatile long lastRefreshTs;

    /** */
    private volatile long primaryRowCnt = DFLT_ROW_CNT;

    /** Internal statistics implementation. */
    private final ObjectStatisticsImpl statistics;

    /** Table descriptor. */
    private final CacheTableDescriptor desc;

    /** Waiters share only the first estimate. Later refreshes can serve the previous value to other readers. */
    private final CompletableFuture<Void> firstRowCntFut;

    /**
     * Constructor.
     *
     * @param statistics Internal object statistics.
     */
    public IgniteStatisticsImpl(ObjectStatisticsImpl statistics) {
        this.statistics = statistics;
        desc = null;
        firstRowCntFut = COMPLETED_FUTURE;
    }

    /**
     * Constructor.
     *
     * @param desc Table descriptor.
     */
    public IgniteStatisticsImpl(CacheTableDescriptor desc) {
        statistics = null;
        this.desc = desc;
        firstRowCntFut = desc == null ? COMPLETED_FUTURE : new CompletableFuture<>();
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount() {
        long rows;

        if (statistics != null)
            rows = statistics.rowCount();
        else if (desc != null)
            rows = getRowCountApproximation();
        else
            rows = DFLT_ROW_CNT;

        return (double)rows;
    }

    /** {@inheritDoc} */
    @Override public boolean isKey(ImmutableBitSet cols) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public List<ImmutableBitSet> getKeys() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public List<RelCollation> getCollations() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution getDistribution() {
        return null;
    }

    /**
     * Get column statistics.
     *
     * @param colName Column name.
     * @return Column statistics or {@code null} if there are no statistics for specified column.
     */
    public @Nullable ColumnStatistics getColumnStatistics(String colName) {
        return (statistics == null) ? null : statistics.columnStatistics(colName);
    }

    /**
     * Refreshes table stats if they are possibly outdated, must be called only in client mode.
     */
    private void refreshStatsIfNeededEx() {
        if (!firstRowCntFut.isDone())
            firstRefreshRowCount();
        else
            subsequentRefreshRowCount();
    }

    /** */
    private long getRowCountApproximation() {
        refreshStatsIfNeededEx();

        return primaryRowCnt;
    }

    /** */
    private static @Nullable Long localCacheSize(@Nullable GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        if (cctx == null || !cctx.kernalContext().rollingUpgrade().features().isActive(STATISTICS_CACHE_SIZE_TASK_FEATURE))
            return null;

        Collection<ClusterNode> nodes = new ArrayList<>(cctx.grid().cluster().forDataNodes(cctx.name()).nodes());

        if (nodes.isEmpty())
            return null;

        IgniteInternalFuture<Long> fut = cctx.kernalContext().task().execute(
            new StatisticsCacheSizeTask(cctx.name()),
            null,
            options(nodes).withTimeout(LOCAL_CACHE_SIZE_TIMEOUT).withFailoverDisabled()
        );

        return fut.get();
    }

    /** */
    private void firstRefreshRowCount() {
        if (updateInProgress.compareAndSet(false, true)) {
            try {
                if (!firstRowCntFut.isDone())
                    refreshRowCount();
            }
            finally {
                firstRowCntFut.complete(null);

                updateInProgress.set(false);
            }
        }
        else
            firstRowCntFut.join();
    }

    /** */
    private void subsequentRefreshRowCount() {
        if (updateInProgress.compareAndSet(false, true)) {
            try {
                if (U.currentTimeMillis() - lastRefreshTs < STATS_REFRESH_INTERVAL)
                    return;

                refreshRowCount();
            }
            finally {
                updateInProgress.set(false);
            }
        }
    }

    /** Refreshes the cached row count, retaining the previous estimate if the measurement fails. */
    private void refreshRowCount() {
        try {
            Long size = localCacheSize(desc.cacheInfo().cacheContext());

            if (size != null)
                primaryRowCnt = size;
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }
        finally {
            lastRefreshTs = U.currentTimeMillis();
        }
    }
}

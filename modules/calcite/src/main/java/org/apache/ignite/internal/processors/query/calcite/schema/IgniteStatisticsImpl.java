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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;

/** Calcite statistics wrapper. */
public class IgniteStatisticsImpl implements Statistic {
    /** */
    private static final int STATS_CLI_UPDATE_THRESHOLD = 200;

    /** */
    AtomicInteger cliReqCnt = new AtomicInteger();

    /** */
    private volatile long primaryRowCnt;

    /** Internal statistics implementation. */
    private final ObjectStatisticsImpl statistics;

    /** Table descriptor. */
    private final CacheTableDescriptor desc;

    /**
     * Constructor.
     *
     * @param statistics Internal object statistics.
     */
    public IgniteStatisticsImpl(ObjectStatisticsImpl statistics) {
        this.statistics = statistics;
        desc = null;
    }

    /**
     * Constructor.
     *
     * @param desc Table descriptor.
     */
    public IgniteStatisticsImpl(CacheTableDescriptor desc) {
        statistics = null;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount() {
        long rows;

        if (statistics != null)
            rows = statistics.rowCount();
        else if (desc != null)
            rows = getRowCountApproximation();
        else
            rows = 1000;

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
    public ColumnStatistics getColumnStatistics(String colName) {
        return (statistics == null) ? null : statistics.columnStatistics(colName);
    }

    /**
     * Refreshes table stats if they are possibly outdated, must be called only in client mode.
     */
    private void refreshStatsIfNeededEx() {
        if (cliReqCnt.getAndIncrement() % STATS_CLI_UPDATE_THRESHOLD == 0) {
            try {
                primaryRowCnt = desc.cacheInfo().cacheContext().cache().size(new CachePeekMode[] {CachePeekMode.PRIMARY});
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }
        }
    }

    /** */
    private long getRowCountApproximation() {
        refreshStatsIfNeededEx();

        return primaryRowCnt;
    }
}

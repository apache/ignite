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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;

/** Calcite statistics wrapper. */
public class IgniteStatisticsImpl implements Statistic {
    /** Internal statistics implementation. */
    private final ObjectStatisticsImpl statistics;

    /** Grid table. */
    private final GridH2Table tbl;

    /**
     * Constructor.
     *
     * @param statistics Internal object statistics.
     */
    public IgniteStatisticsImpl(ObjectStatisticsImpl statistics) {
        this.statistics = statistics;
        tbl = null;
    }

    /**
     * Constructor.
     *
     * @param tbl Base grid table.
     */
    public IgniteStatisticsImpl(GridH2Table tbl) {
        statistics = null;
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount() {
        long rows;

        if (statistics != null)
            rows = statistics.rowCount();
        else if (tbl != null)
            rows = tbl.getRowCountApproximationNoCheck();
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
}

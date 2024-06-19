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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.SystemViewScan;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation for system views.
 */
public class SystemViewTableImpl extends AbstractTable implements IgniteTable {
    /** Default row count approximation. */
    private static final Double DEFAULT_ROW_COUNT_APPROXIMATION = 1000d;

    /** */
    private final SystemViewTableDescriptorImpl<?> desc;

    /** */
    private final Map<String, IgniteIndex> indexes;

    /** */
    private final Statistic statistic;

    /**
     * @param desc Table descriptor.
     */
    public SystemViewTableImpl(SystemViewTableDescriptorImpl<?> desc) {
        this.desc = desc;
        statistic = new StatisticsImpl();

        if (desc.isFiltrable()) {
            IgniteIndex idx = new SystemViewIndexImpl(this);

            indexes = Collections.singletonMap(idx.name(), idx);
        }
        else
            indexes = Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory)typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return statistic;
    }

    /** {@inheritDoc} */
    @Override public SystemViewTableDescriptorImpl<?> descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogicalTableScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns,
        @Nullable List<RelHint> hints
    ) {
        return IgniteLogicalTableScan.create(cluster, cluster.traitSet(), relOptTbl, hints, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet usedColumns
    ) {
        return new SystemViewScan<>(execCtx, desc, null, usedColumns);
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        return desc.colocationGroup(ctx);
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgniteIndex> indexes() {
        return indexes;
    }

    /** {@inheritDoc} */
    @Override public void addIndex(IgniteIndex idxTbl) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(String idxName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aCls) {
        if (aCls.isInstance(desc))
            return aCls.cast(desc);

        return super.unwrap(aCls);
    }

    /** {@inheritDoc} */
    @Override public boolean isModifiable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void markIndexRebuildInProgress(boolean mark) {
        throw new AssertionError("Index rebuild in progress was marked for system view");
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexRebuildInProgress() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return desc.name();
    }

    /** {@inheritDoc} */
    @Override public void authorize(Operation op) {
        // No-op.
    }

    /** */
    private static class StatisticsImpl implements Statistic {
        /** {@inheritDoc} */
        @Override public Double getRowCount() {
            return DEFAULT_ROW_COUNT_APPROXIMATION;
        }

        /** {@inheritDoc} */
        @Override public List<RelCollation> getCollations() {
            return ImmutableList.of(); // Required not null.
        }
    }
}

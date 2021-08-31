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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.TableScan;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements IgniteTable {
    /** */
    private final TableDescriptor desc;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final Map<String, IgniteIndex> indexes = new ConcurrentHashMap<>();

    /** */
    private volatile GridH2Table tbl;

    /**
     * @param ctx Kernal context.
     * @param desc Table descriptor.
     */
    public IgniteTableImpl(GridKernalContext ctx, TableDescriptor desc) {
        this.ctx = ctx;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory)typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        IgniteH2Indexing idx = (IgniteH2Indexing)ctx.query().getIndexing();

        final String tblName = desc.typeDescription().tableName();
        final String schemaName = desc.typeDescription().schemaName();

        ObjectStatisticsImpl statistics = (ObjectStatisticsImpl)idx.statsManager().getLocalStatistics(
            new StatisticsKey(schemaName, tblName));

        if (statistics != null)
            return new IgniteStatisticsImpl(statistics);

        if (tbl == null)
            tbl = idx.schemaManager().dataTable(schemaName, tblName);

        return new IgniteStatisticsImpl(tbl);
    }

    /** {@inheritDoc} */
    @Override public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogicalTableScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalTableScan.create(cluster, cluster.traitSet(), relOptTbl, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogicalIndexScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalIndexScan.create(cluster, cluster.traitSet(), relOptTbl, idxName, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup group,
        Predicate<Row> filter,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet usedColumns) {
        UUID localNodeId = execCtx.planningContext().localNodeId();
        if (group.nodeIds().contains(localNodeId))
            return new TableScan<>(execCtx, desc, group.partitions(localNodeId), filter, rowTransformer, usedColumns);

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
        return desc.colocationGroup(ctx);
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgniteIndex> indexes() {
        return Collections.unmodifiableMap(indexes);
    }

    /** {@inheritDoc} */
    @Override public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }

    /** {@inheritDoc} */
    @Override public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aCls) {
        if (aCls.isInstance(desc))
            return aCls.cast(desc);

        return super.unwrap(aCls);
    }

    /**
     * Start cache context for lazy caches.
     */
    public void ensureCacheStarted() {
        if (desc.cacheContext() == null) {
            try {
                ctx.cache().dynamicStartCache(null, desc.cacheInfo().config().getName(), null,
                    false, true, true).get();
            }
            catch (IgniteCheckedException ex) {
                throw U.convertException(ex);
            }
        }
    }
}

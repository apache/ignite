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

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.AffinityFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable, ProjectableFilterableTable {
    private final String name;
    private final String cacheName;
    private final RowType rowType;
    private final Object identityKey;
    private final RelCollation collation;
    private final HashMap<String, IgniteTable> indexes = new HashMap<>(4);
    private final String sql;

    public IgniteTable(String name, String cacheName, RowType rowType, RelCollation collation, String sql, Object identityKey) {
        this.name = name;
        this.cacheName = cacheName;
        this.rowType = rowType;
        this.collation = collation;
        this.sql = sql;
        this.identityKey = identityKey;
    }

    public void addIndex(IgniteTable idx) {
        indexes.put(idx.name(), idx);
    }

    public String sql() {
        return sql;
    }

    public HashMap<String, IgniteTable> indexes() {
        return indexes;
    }

    /**
     * @return Table name;
     */
    public String name() {
        return name;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType.asRelDataType(typeFactory);
    }

    public RowType igniteRowType() {
        return rowType;
    }

    @Override public Statistic getStatistic() {
        return new TableStatistics();
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replaceIf(DistributionTraitDef.INSTANCE, this::getDistribution);

        if (collation != null)
            traitSet = traitSet.replaceIf(RelCollationTraitDef.INSTANCE, () -> collation);

        return new IgniteTableScan(cluster, traitSet, relOptTable, null, null);
    }

    public IgniteDistribution getDistribution() {
        Object key = identityKey();

        if (key == null)
            return IgniteDistributions.broadcast();

        return IgniteDistributions.hash(rowType.distributionKeys(), new AffinityFactory(CU.cacheId(cacheName), key));
    }

    protected Object identityKey() {
        return identityKey;
    }

    public FragmentInfo fragmentInfo(PlannerContext ctx) {
        return new FragmentInfo(ctx.mapForCache(CU.cacheId(cacheName), ctx.topologyVersion()));
    }

    @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        return null; // TODO: CODE: implement.
    }

    private class TableStatistics implements Statistic {
        @Override public Double getRowCount() {
            return null;
        }

        @Override public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override public List<RelReferentialConstraint> getReferentialConstraints() {
            return ImmutableList.of();
        }

        @Override public List<RelCollation> getCollations() {
            return ImmutableList.of();
        }

        @Override public RelDistribution getDistribution() {
            return IgniteTable.this.getDistribution();
        }
    }
}

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
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable, ScannableTable {
    /** */
    private final String tableName;

    /** */
    private final String cacheName;

    /** */
    private final RowType rowType;

    /** */
    private final Object identityKey;

    /**
     * @param tableName Table name.
     * @param cacheName Cache name.
     * @param rowType Row type.
     * @param identityKey Affinity identity key.
     */
    public IgniteTable(String tableName, String cacheName, RowType rowType, Object identityKey) {
        this.tableName = tableName;
        this.cacheName = cacheName;
        this.rowType = rowType;
        this.identityKey = identityKey;
    }

    /**
     * @return Table name;
     */
    public String tableName() {
        return tableName;
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

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return new TableStatistics();
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

        return new LogicalTableScan(cluster, traitSet, relOptTable);
    }

    /**
     * @return Table distribution trait.
     */
    public IgniteDistribution distribution() {
        Object key = identityKey();

        if (key == null)
            return IgniteDistributions.broadcast();

        return IgniteDistributions.hash(rowType.distributionKeys(), new DistributionFunction.AffinityDistribution(CU.cacheId(cacheName), key));
    }

    /**
     * @return Affinity identity key.
     */
    protected Object identityKey() {
        return identityKey;
    }

    /**
     * @param ctx Planner context.
     * @return Fragment meta information.
     */
    public FragmentInfo fragmentInfo(PlannerContext ctx) {
        return new FragmentInfo(ctx.mapForCache(CU.cacheId(cacheName)));
    }

    /** {@inheritDoc} */
    @Override public Enumerable<Object[]> scan(DataContext root) {
        throw new AssertionError(); // TODO
    }

    /** */
    private class TableStatistics implements Statistic {
        /** {@inheritDoc} */
        @Override public Double getRowCount() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isKey(ImmutableBitSet columns) {
            return false;
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
        @Override public RelDistribution getDistribution() {
            return distribution();
        }
    }
}

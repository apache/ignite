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
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.TableScan;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable, ScannableTable {
    /** */
    private final List<String> fullName;

    /** */
    private final TableDescriptor desc;

    /**
     * @param fullName Table full name.
     */
    public IgniteTable(List<String> fullName, TableDescriptor desc) {
        assert !F.isEmpty(fullName);
        this.fullName = ImmutableList.copyOf(fullName);
        this.desc = desc;
    }

    /**
     * @return Table name.
     */
    public String name() {
        return fullName.get(fullName.size() - 1);
    }

    /**
     * @return Table full name.
     */
    public List<String> fullName() {
        return fullName;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return desc.apply(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return new StatisticsImpl();
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

        return new IgniteTableScan(cluster, traitSet, relOptTable);
    }

    /**
     * @return Table distribution trait.
     */
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /**
     * @param ctx Planner context.
     * @return Fragment meta information.
     */
    public FragmentInfo fragmentInfo(IgniteCalciteContext ctx) {
        return new FragmentInfo(ctx.mapForCache(desc.cacheId()));
    }

    /** {@inheritDoc} */
    @Override public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(new TableScan((ExecutionContext) root, desc));
    }

    /** */
    private class StatisticsImpl implements Statistic {
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
            return desc.collations();
        }

        /** {@inheritDoc} */
        @Override public RelDistribution getDistribution() {
            return desc.distribution();
        }
    }
}

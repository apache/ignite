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

import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable {
    private final String tableName;
    private final String cacheName;
    private final Function<RelDataTypeFactory, RelDataType> rowType;
    private final Statistic statistic;


    public IgniteTable(String tableName, String cacheName,
        Function<RelDataTypeFactory, RelDataType> rowType, @Nullable Statistic statistic) {
        this.tableName = tableName;
        this.cacheName = cacheName;
        this.rowType = rowType;

        this.statistic = statistic == null ? Statistics.UNKNOWN : statistic;
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
    @Override public Statistic getStatistic() {
        return statistic;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType.apply(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSet()
                .replace(IgniteRel.LOGICAL_CONVENTION)
                .replaceIf(RelDistributionTraitDef.INSTANCE, () -> getStatistic().getDistribution())
                .replaceIfs(RelCollationTraitDef.INSTANCE, () -> getStatistic().getCollations());
        return new IgniteLogicalTableScan(cluster, traitSet, relOptTable);
    }
}

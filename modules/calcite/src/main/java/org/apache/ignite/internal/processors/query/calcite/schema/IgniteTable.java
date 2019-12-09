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

import java.util.HashMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable {
    private final String name;
    private final String cacheName;
    private final RowType rowType;
    private final RelCollation collation;
    private final HashMap<String, IgniteTable> indexes = new HashMap<>(4);
    private final String sql;

    public IgniteTable(String name, String cacheName, RowType rowType, RelCollation collation, String sql) {
        this.name = name;
        this.cacheName = cacheName;
        this.rowType = rowType;
        this.collation = collation;
        this.sql = sql;
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

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        PlannerContext ctx = Commons.plannerContext(cluster.getPlanner().getContext());
        RelTraitSet traitSet = cluster.traitSet().replace(IgniteRel.IGNITE_CONVENTION)
            .replaceIf(DistributionTraitDef.INSTANCE, () -> distributionTrait(ctx))
            .replace(collation);
        return new IgniteTableScan(cluster, traitSet, relOptTable);
    }

    public DistributionTrait distributionTrait(PlannerContext context) {
        return Commons.plannerContext(context).distributionTrait(CU.cacheId(cacheName), rowType);
    }

    public FragmentInfo fragmentInfo(PlannerContext ctx) {
        PlannerContext ctx0 = Commons.plannerContext(ctx);

        return new FragmentInfo(ctx0.mapForCache(CU.cacheId(cacheName), ctx0.topologyVersion()));
    }
}

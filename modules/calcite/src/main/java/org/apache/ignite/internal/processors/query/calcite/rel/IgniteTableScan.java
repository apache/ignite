/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteTableScan extends TableScan implements IgniteRel {
    private final List<RexNode> filters;
    private final ImmutableIntList projects;

    public IgniteTableScan(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        @Nullable List<RexNode> filters,
        @Nullable ImmutableIntList projects) {
        super(cluster, traitSet, table);

        if (filters != null)
            System.out.println("!");

        // TODO check if mapping is trivial


        this.filters = filters;
        this.projects = projects;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost currentCost = super.computeSelfCost(planner, mq);


        if (!F.isEmpty(filters)) {
            System.out.println("!");
//            RexNode finalFilter = RexUtil.removeCast() getCluster().getRexBuilder();
//
//            mq.getSelectivity(this, filters);
        }

        if (!F.isEmpty(filters) && table.getQualifiedName().get(1).contains("IDX"))
            return currentCost.multiplyBy(0.1);

        return currentCost;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return this;
    }

    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public List<RexNode> filters() {
        return filters;
    }

    public ImmutableIntList projects() {
        return projects;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("filters", filters)
            .item("projects", projects);
    }

    @Override public RelDataType deriveRowType() {
        if (projects == null)
            return super.deriveRowType();

        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();

        for (int project : projects)
            builder.add(fieldList.get(project));

        return builder.build();
    }
}

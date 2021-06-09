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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;

/**
 * Relational operator that returns the contents of a table.
 */
public class IgniteTableSpool extends AbstractIgniteSpool implements IgniteRel {
    /** */
    public IgniteTableSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        Spool.Type readType,
        RelNode input
    ) {
        super(cluster, traits, readType, input);
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableSpool(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getEnum("readType", Spool.Type.class),
            input.getInput()
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableSpool(cluster, getTraitSet(), readType, inputs.get(0));
    }

    /** {@inheritDoc} */
    @Override protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteTableSpool(getCluster(), traitSet, readType, input);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCnt * bytesPerRow;
        double cpuCost = rowCnt * IgniteCost.ROW_PASS_THROUGH_COST;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        return costFactory.makeCost(rowCnt, cpuCost, 0, totalBytes, 0);
    }
}

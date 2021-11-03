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

package org.apache.ignite.internal.processors.query.calcite.rel.agg;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class IgniteReduceHashAggregate extends IgniteReduceAggregateBase implements IgniteHashAggregateBase {
    /**
     *
     */
    public IgniteReduceHashAggregate(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RelDataType rowType
    ) {
        super(cluster, traits, input, groupSet, groupSets, aggCalls, rowType);

        assert RelOptUtil.areRowTypesEqual(input.getRowType(),
                IgniteMapHashAggregate.rowType(Commons.typeFactory(cluster), !aggCalls.isEmpty()), true);
    }

    /**
     *
     */
    public IgniteReduceHashAggregate(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteReduceHashAggregate(getCluster(), traitSet, sole(inputs), groupSet, groupSets, aggCalls, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteReduceHashAggregate(cluster, getTraitSet(), sole(inputs),
                groupSet, groupSets, aggCalls, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double rows = mq.getRowCount(getInput());

        double mem = 0d;
        if (aggCalls.isEmpty()) {
            mem = groupSet.cardinality() * IgniteCost.AVERAGE_FIELD_SIZE;
        } else {
            for (AggregateCall aggCall : aggCalls) {
                if (aggCall.isDistinct()) {
                    mem += IgniteCost.AGG_CALL_MEM_COST * rows;
                } else {
                    mem += IgniteCost.AGG_CALL_MEM_COST;
                }
            }
        }

        return costFactory.makeCost(
                rows,
                rows * IgniteCost.ROW_PASS_THROUGH_COST,
                0,
                mem,
                0
        );
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        return List.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
                List.of(inTraits.get(0).replace(RelCollations.EMPTY))));
    }
}

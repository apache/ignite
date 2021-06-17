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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 *
 */
public abstract class IgniteAggregate extends Aggregate implements IgniteRel {
    /** {@inheritDoc} */
    protected IgniteAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    protected IgniteAggregate(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        Double groupsCnt = mq.getDistinctRowCount(getInput(), groupSet, null);

        if (groupsCnt != null)
            return groupsCnt;

        // Estimation of the groups count is not available.
        // Use heuristic estimation for result rows count.
        return super.estimateRowCount(mq);
    }

    /** */
    public double estimateMemoryForGroup(RelMetadataQuery mq) {
        double mem = groupSet.cardinality() * IgniteCost.AVERAGE_FIELD_SIZE;

        if (!aggCalls.isEmpty()) {
            double grps = estimateRowCount(mq);
            double rows = input.estimateRowCount(mq);

            for (AggregateCall aggCall : aggCalls) {
                if (aggCall.isDistinct())
                    mem += IgniteCost.AGG_CALL_MEM_COST * rows / grps;
                else
                    mem += IgniteCost.AGG_CALL_MEM_COST;
            }
        }

        return mem;
    }

    /** */
    public RelOptCost computeSelfCostHash(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double inRows = mq.getRowCount(getInput());
        double groups = estimateRowCount(mq);

        return costFactory.makeCost(
            inRows,
            inRows * IgniteCost.ROW_PASS_THROUGH_COST,
            0,
            groups * estimateMemoryForGroup(mq),
            0
        );
    }

    /** */
    public RelOptCost computeSelfCostSort(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double inRows = mq.getRowCount(getInput());

        return costFactory.makeCost(
            inRows,
            inRows * IgniteCost.ROW_PASS_THROUGH_COST,
            0,
            estimateMemoryForGroup(mq),
            0
        );
    }
}

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
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 *
 */
public class IgniteReduceSortAggregate extends IgniteReduceAggregateBase implements IgniteSortAggregateBase {
    /** Collation. */
    private final RelCollation collation;

    /**
     *
     */
    public IgniteReduceSortAggregate(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RelDataType rowType,
            RelCollation collation
    ) {
        super(cluster, traits, input, groupSet, groupSets, aggCalls, rowType);

        assert Objects.nonNull(collation);
        assert !collation.isDefault();

        this.collation = collation;
    }

    /**
     *
     */
    public IgniteReduceSortAggregate(RelInput input) {
        super(input);
        collation = input.getCollation();

        assert Objects.nonNull(collation);
        assert !collation.isDefault();
    }

    /** {@inheritDoc} */
    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteReduceSortAggregate(
                getCluster(),
                traitSet,
                sole(inputs),
                groupSet,
                groupSets,
                aggCalls,
                rowType,
                TraitUtils.collation(traitSet)
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteReduceSortAggregate(
                cluster,
                getTraitSet().replace(collation),
                sole(inputs),
                groupSet,
                groupSets,
                aggCalls,
                rowType,
                collation
        );
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("collation", collation);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double rows = mq.getRowCount(getInput());

        return costFactory.makeCost(
                rows,
                rows * IgniteCost.ROW_PASS_THROUGH_COST,
                0
        );
    }
}

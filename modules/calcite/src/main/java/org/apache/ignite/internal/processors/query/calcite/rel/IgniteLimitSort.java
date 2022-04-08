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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit.FETCH_IS_PARAM_FACTOR;
import static org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit.doubleFromRex;

/**
 * Ignite sort operator.
 */
public class IgniteLimitSort extends Sort implements IgniteRel {
    /** Order fetch limit. */
    private RexNode limit;

    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param child Input node.
     * @param collation Collation.
     */
    public IgniteLimitSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode limit
    ) {
        super(cluster, traits, child, collation);

//        traitSet = getTraitSet().replace(RelCollations.EMPTY);

        this.limit = limit;
    }

    /** */
    public IgniteLimitSort(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override public Sort copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        RexNode offset,
        RexNode fetch
    ) {
        assert offset == null && fetch == null;

        return new IgniteLimitSort(getCluster(), traitSet, newInput, newCollation, limit);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (TraitUtils.distribution(required) != IgniteDistributions.single())
            return null;

        RelCollation requiredCollation = TraitUtils.collation(required);
        RelCollation relCollation = TraitUtils.collation(traitSet);

        if (relCollation.satisfies(requiredCollation))
            required = required.replace(relCollation);
        else if (!requiredCollation.satisfies(relCollation))
            return null;

        return Pair.of(required, ImmutableList.of(required));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        if (!TraitUtils.collation(childTraits).satisfies(TraitUtils.collation(traitSet)))
            return null;

        return Pair.of(childTraits, ImmutableList.of(childTraits));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double inputRowCount = mq.getRowCount(getInput());

        double lim = fetch != null ? doubleFromRex(fetch, inputRowCount * FETCH_IS_PARAM_FACTOR) : inputRowCount;
//        double off = offset != null ? doubleFromRex(offset, inputRowCount * OFFSET_IS_PARAM_FACTOR) : 0;

//        return Math.min(lim, inputRowCount - off);
        return Math.min(lim, inputRowCount);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double inputRowCount = mq.getRowCount(getInput());

        double lim = fetch != null ? doubleFromRex(fetch, inputRowCount * FETCH_IS_PARAM_FACTOR) : inputRowCount;

        double rows = Math.min(mq.getRowCount(getInput()), lim);

        double cpuCost = rows * IgniteCost.ROW_PASS_THROUGH_COST + Util.nLogM(1, rows) * IgniteCost.ROW_COMPARISON_COST;
        double memory = rows * getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        RelOptCost cost = costFactory.makeCost(rows, cpuCost, 0, memory, 0);

        return cost;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteLimitSort(cluster, getTraitSet(), sole(inputs), collation, limit);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return false;
    }
}

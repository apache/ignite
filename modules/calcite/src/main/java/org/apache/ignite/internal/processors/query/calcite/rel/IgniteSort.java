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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.FETCH_IS_PARAM_FACTOR;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.OFFSET_IS_PARAM_FACTOR;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.doubleFromRex;

/**
 * Ignite sort operator.
 */
public class IgniteSort extends Sort implements IgniteRel {
    /** */
    private final boolean enforcer;

    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param child Input node.
     * @param collation Collation.
     * @param offset Offset.
     * @param fetch Limit.
     * @param enforcer Enforcer flag.
     */
    public IgniteSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        @Nullable RexNode offset,
        @Nullable RexNode fetch,
        boolean enforcer
    ) {
        super(cluster, traits, child, collation, offset, fetch);

        this.enforcer = enforcer;
    }

    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param child Input node.
     * @param collation Collation.
     * @param enforcer Enforcer flag.
     */
    public IgniteSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        boolean enforcer
    ) {
        this(cluster, traits, child, collation, null, null, enforcer);
    }

    /** */
    public IgniteSort(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));

        // No need to enforce anything on ready, fragmented and sent plan.
        enforcer = false;
    }

    /** {@inheritDoc} */
    @Override public Sort copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        @Nullable RexNode offset,
        @Nullable RexNode fetch
    ) {
        return new IgniteSort(getCluster(), traitSet, newInput, traitSet.getCollation(), offset, fetch, enforcer);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (isEnforcer() || required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        RelCollation requiredCollation = TraitUtils.collation(required);
        RelCollation relCollation = traitSet.getCollation();

        if (!requiredCollation.satisfies(relCollation))
            return null;

        return Pair.of(required, ImmutableList.of(required.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (isEnforcer() || childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        return Pair.of(childTraits.replace(collation()), ImmutableList.of(childTraits));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return memRows(mq.getRowCount(getInput()));
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double inputRows = mq.getRowCount(getInput());

        double memRows = memRows(inputRows);

        double cpuCost = inputRows * IgniteCost.ROW_PASS_THROUGH_COST + Util.nLogM(inputRows, memRows)
            * IgniteCost.ROW_COMPARISON_COST;
        double memory = memRows * getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        RelOptCost cost = costFactory.makeCost(inputRows, cpuCost, 0, memory, 0);

        // Distributed sorting is more preferable than sorting on the single node.
        if (TraitUtils.distribution(traitSet).satisfies(IgniteDistributions.single()))
            cost = cost.plus(costFactory.makeTinyCost());

        return cost;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSort(cluster, getTraitSet(), sole(inputs), collation, offset, fetch, enforcer);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return enforcer;
    }

    /** Rows number to keep in memory and sort. */
    private double memRows(double inputRows) {
        double fetch = this.fetch != null ? doubleFromRex(this.fetch, inputRows * FETCH_IS_PARAM_FACTOR)
            : inputRows;
        double offset = this.offset != null ? doubleFromRex(this.offset, inputRows * OFFSET_IS_PARAM_FACTOR)
            : 0;

        return Math.min(inputRows, fetch + offset);
    }
}

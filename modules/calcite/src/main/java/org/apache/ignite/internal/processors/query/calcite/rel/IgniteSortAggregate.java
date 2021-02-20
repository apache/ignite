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
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 *
 */
public class IgniteSortAggregate extends IgniteAggregateBase {
    /** Collation. */
    private final RelCollation collation;

    /** {@inheritDoc} */
    public IgniteSortAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);

        assert !TraitUtils.collation(traitSet).isDefault();

        collation = TraitUtils.collation(traitSet);
    }

    /** {@inheritDoc} */
    public IgniteSortAggregate(RelInput input) {
        super(input);

        collation = input.getCollation();

        assert Objects.nonNull(collation);
        assert !collation.isDefault();
    }

    /** {@inheritDoc} */
    @Override public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new IgniteSortAggregate(getCluster(), traitSet.replace(collation), input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSortAggregate(cluster, getTraitSet(), sole(inputs),
            getGroupSet(), getGroupSets(), getAggCallList());
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("collation", collation);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(groupSet.asList()));

        return Pair.of(nodeTraits.replace(collation),
            ImmutableList.of(inputTraits.get(0).replace(collation)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelCollation requiredCollation = RelCollations.of(ImmutableIntList.copyOf(groupSet.asList()));
        RelCollation inputCollation = TraitUtils.collation(inputTraits.get(0));

        if (!inputCollation.satisfies(requiredCollation))
            return ImmutableList.of();

        return ImmutableList.of(Pair.of(nodeTraits.replace(requiredCollation), inputTraits));
    }

    /** {@inheritDoc} */
    @Override protected RelNode createMapAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new IgniteMapSortAggregate(getCluster(), traits, input, groupSet, groupSets, aggCalls, collation);
    }

    /** {@inheritDoc} */
    @Override protected RelNode createReduceAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls,
        RelDataType rowType) {
        return new IgniteReduceSortAggregate(getCluster(), traits, input, groupSet, groupSets,
            aggCalls, getRowType(), collation);
    }

    /** {@inheritDoc} */
    @Override public RelCollation collation() {
        assert collation.equals(super.collation());

        return collation;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return computeSelfCostSort(planner, mq);
    }
}

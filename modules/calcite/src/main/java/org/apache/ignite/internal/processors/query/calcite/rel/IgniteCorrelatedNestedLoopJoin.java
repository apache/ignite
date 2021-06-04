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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.maxPrefix;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public class IgniteCorrelatedNestedLoopJoin extends AbstractIgniteJoin {
    /**
     * Creates a Join.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesSet Set variables that are set by the LHS and used by the RHS and are not available to nodes
     * above this Join in the tree
     */
    public IgniteCorrelatedNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
        RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    /** */
    public IgniteCorrelatedNestedLoopJoin(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class));
    }

    /** {@inheritDoc} */
    @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
        return new IgniteCorrelatedNestedLoopJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);
        RelCollation leftCollation = TraitUtils.collation(left), rightCollation = TraitUtils.collation(right);

        List<Integer> newRightCollationFields = maxPrefix(rightCollation.getKeys(), joinInfo.leftKeys);

        if (nullOrEmpty(newRightCollationFields))
            return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY), inputTraits));

        // We preserve left edge collation only if batch size == 1
        if (variablesSet.size() == 1)
            nodeTraits = nodeTraits.replace(leftCollation);
        else
            nodeTraits = nodeTraits.replace(RelCollations.EMPTY);

        return ImmutableList.of(Pair.of(nodeTraits, inputTraits));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        // Correlated nested loop requires rewindable right edge.
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RewindabilityTrait rewindability = TraitUtils.rewindability(left);

        return ImmutableList.of(Pair.of(nodeTraits.replace(rewindability),
            ImmutableList.of(left, right.replace(RewindabilityTrait.REWINDABLE))));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        // We preserve left edge collation only if batch size == 1
        if (variablesSet.size() == 1) {
            Pair<RelTraitSet, List<RelTraitSet>> baseTraits = super.passThroughCollation(nodeTraits, inputTraits);

            return Pair.of(
                baseTraits.getKey(),
                ImmutableList.of(
                    baseTraits.getValue().get(0),
                    baseTraits.getValue().get(1)
                )
            );
        }

        return Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            ImmutableList.of(left.replace(RelCollations.EMPTY), right));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Correlated nested loop requires rewindable right edge.
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return Pair.of(nodeTraits.replace(rewindability),
            ImmutableList.of(left.replace(rewindability), right.replace(RewindabilityTrait.REWINDABLE)));
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double leftCount = mq.getRowCount(getLeft());

        if (Double.isInfinite(leftCount))
            return costFactory.makeInfiniteCost();

        double rightCount = mq.getRowCount(getRight());

        if (Double.isInfinite(rightCount))
            return costFactory.makeInfiniteCost();

        double rows = leftCount * rightCount;

        return costFactory.makeCost(rows,
            rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        CorrelationTrait nodeCorr = TraitUtils.correlation(nodeTraits);

        Set<CorrelationId> selfCorrIds = new HashSet<>(CorrelationTrait.correlations(variablesSet).correlationIds());
        selfCorrIds.addAll(nodeCorr.correlationIds());

        return Pair.of(nodeTraits,
            ImmutableList.of(
                inTraits.get(0).replace(nodeCorr),
                inTraits.get(1).replace(CorrelationTrait.correlations(selfCorrIds))
            )
        );
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        Set<CorrelationId> rightCorrIds = TraitUtils.correlation(inTraits.get(1)).correlationIds();

        if (!rightCorrIds.containsAll(variablesSet))
            return ImmutableList.of();

        Set<CorrelationId> corrIds = new HashSet<>(rightCorrIds);

        // Left + right
        corrIds.addAll(TraitUtils.correlation(inTraits.get(0)).correlationIds());

        corrIds.removeAll(variablesSet);

        return ImmutableList.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(corrIds)), inTraits));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteCorrelatedNestedLoopJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
            getVariablesSet(), getJoinType());
    }

    /** */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("correlationVariables", getVariablesSet());
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        // condition selectivity already counted within the external filter
        return super.estimateRowCount(mq) / mq.getSelectivity(this, getCondition());
    }
}

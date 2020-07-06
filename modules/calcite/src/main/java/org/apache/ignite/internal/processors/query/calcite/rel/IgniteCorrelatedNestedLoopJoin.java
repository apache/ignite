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
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public class IgniteCorrelatedNestedLoopJoin extends AbstractIgniteNestedLoopJoin {
    /**
     * Creates a Join.
     *
     * @param cluster          Cluster
     * @param traitSet         Trait set
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     * @param joinType         Join type
     * @param variablesSet     Set variables that are set by the
     *                         LHS and used by the RHS and are not available to
     *                         nodes above this Join in the tree
     */
    public IgniteCorrelatedNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
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
    @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new IgniteCorrelatedNestedLoopJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        ImmutableList.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableList.builder();

        RewindabilityTrait leftRewindability = TraitUtils.rewindability(left);

        RelTraitSet outTraits, leftTraits, rightTraits;

        outTraits = nodeTraits.replace(RewindabilityTrait.ONE_WAY);
        leftTraits = left.replace(RewindabilityTrait.ONE_WAY);
        rightTraits = right.replace(RewindabilityTrait.REWINDABLE);

        b.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

        if (leftRewindability.rewindable()) {
            outTraits = nodeTraits.replace(RewindabilityTrait.REWINDABLE);
            leftTraits = left.replace(RewindabilityTrait.REWINDABLE);
            rightTraits = right.replace(RewindabilityTrait.REWINDABLE);

            b.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
        }

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
            RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

            RelTraitSet outTraits, leftTraits, rightTraits;

            RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

            outTraits = nodeTraits.replace(rewindability);
            leftTraits = left.replace(rewindability);
            rightTraits = right.replace(RewindabilityTrait.REWINDABLE);

            return ImmutableList.of(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Give it some penalty
        return super.computeSelfCost(planner, mq).multiplyBy(5);
    }
}

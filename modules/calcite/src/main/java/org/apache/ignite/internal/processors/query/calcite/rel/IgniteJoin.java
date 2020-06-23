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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public class IgniteJoin extends Join implements TraitsAwareIgniteRel {
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
    public IgniteJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    /** */
    public IgniteJoin(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf((List<CorrelationId>)input.get("variablesSet")),
            input.getEnum("joinType", JoinRelType.class));
    }

    /** {@inheritDoc} */
    @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new IgniteJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet.asList(), pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES);
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, left = pair.right.get(0), right = pair.right.get(1);

            RelTraitSet outTraits, leftTraits, rightTraits;

            RelCollation collation = TraitUtils.collation(left);

            if (joinType == RIGHT || joinType == JoinRelType.FULL) {
                for (RelFieldCollation field : collation.getFieldCollations()) {
                    if (RelFieldCollation.NullDirection.LAST != field.nullDirection) {
                        collation = RelCollations.EMPTY;
                        break;
                    }
                }
            }

            outTraits = out.replace(collation);
            leftTraits = left.replace(collation);
            rightTraits = right.replace(RelCollations.EMPTY);

            traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, left = pair.right.get(0), right = pair.right.get(1);

            RewindabilityTrait leftRewindability = TraitUtils.rewindability(left);
            RewindabilityTrait rightRewindability = TraitUtils.rewindability(right);

            RelTraitSet outTraits, leftTraits, rightTraits;

            outTraits = out.replace(RewindabilityTrait.ONE_WAY);
            leftTraits = left.replace(RewindabilityTrait.ONE_WAY);
            rightTraits = right.replace(RewindabilityTrait.ONE_WAY);

            traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

            if (leftRewindability.rewindable() && rightRewindability.rewindable()) {
                outTraits = out.replace(RewindabilityTrait.REWINDABLE);
                leftTraits = left.replace(RewindabilityTrait.REWINDABLE);
                rightTraits = right.replace(RewindabilityTrait.REWINDABLE);

                traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
            }
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, left = pair.right.get(0), right = pair.right.get(1);

            IgniteDistribution leftDistr = TraitUtils.distribution(left);
            IgniteDistribution rightDistr = TraitUtils.distribution(right);

            RelTraitSet outTraits, leftTraits, rightTraits;

            outTraits = out.replace(single());
            leftTraits = left.replace(single());
            rightTraits = right.replace(single());

            traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

            outTraits = out.replace(broadcast());
            leftTraits = left.replace(broadcast());
            rightTraits = right.replace(broadcast());

            traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

            if (joinType == LEFT || joinType == RIGHT || (joinType == INNER && !F.isEmpty(joinInfo.pairs()))) {
                Set<DistributionFunction> functions = new HashSet<>();

                if (leftDistr.getType() == HASH_DISTRIBUTED
                    && Objects.equals(joinInfo.leftKeys, leftDistr.getKeys()))
                    functions.add(leftDistr.function());

                if (rightDistr.getType() == HASH_DISTRIBUTED
                    && Objects.equals(joinInfo.rightKeys, rightDistr.getKeys()))
                    functions.add(rightDistr.function());

                functions.add(DistributionFunction.HashDistribution.INSTANCE);

                for (DistributionFunction factory : functions) {
                    outTraits = out.replace(hash(joinInfo.leftKeys, factory));
                    leftTraits = left.replace(hash(joinInfo.leftKeys, factory));
                    rightTraits = right.replace(hash(joinInfo.rightKeys, factory));

                    traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                    if (joinType == INNER || joinType == LEFT) {
                        leftTraits = left.replace(hash(joinInfo.leftKeys, factory));
                        rightTraits = right.replace(broadcast());

                        traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }

                    if (joinType == INNER || joinType == RIGHT) {
                        leftTraits = left.replace(broadcast());
                        rightTraits = right.replace(hash(joinInfo.rightKeys, factory));

                        traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }
                }
            }
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, left = pair.right.get(0), right = pair.right.get(1);

            RelTraitSet outTraits, leftTraits, rightTraits;

            RelCollation collation = TraitUtils.collation(out);

            if (joinType == RIGHT || joinType == JoinRelType.FULL) {
                for (RelFieldCollation field : collation.getFieldCollations()) {
                    if (RelFieldCollation.NullDirection.LAST != field.nullDirection) {
                        collation = RelCollations.EMPTY;
                        break;
                    }
                }
            }

            outTraits = out.replace(collation);
            leftTraits = left.replace(collation);
            rightTraits = right.replace(RelCollations.EMPTY);

            traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, left = pair.right.get(0), right = pair.right.get(1);

            RelTraitSet outTraits, leftTraits, rightTraits;

            RewindabilityTrait rewindability = TraitUtils.rewindability(out);

            outTraits = out.replace(rewindability);
            leftTraits = left.replace(rewindability);
            rightTraits = right.replace(rewindability);

            traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, left = pair.right.get(0), right = pair.right.get(1);

            RelTraitSet outTraits, leftTraits, rightTraits;

            IgniteDistribution distribution = TraitUtils.distribution(out);

            RelDistribution.Type distrType = distribution.getType();
            switch (distrType) {
                case BROADCAST_DISTRIBUTED:
                case SINGLETON:
                    outTraits = out.replace(distribution);
                    leftTraits = left.replace(distribution);
                    rightTraits = right.replace(distribution);

                    traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                    break;
                case HASH_DISTRIBUTED:
                case RANDOM_DISTRIBUTED:
                    if (joinType != LEFT && joinType != RIGHT && (joinType != INNER || F.isEmpty(joinInfo.pairs()))) {
                        outTraits = out.replace(single());
                        leftTraits = left.replace(single());
                        rightTraits = right.replace(single());

                        traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                        break;
                    }

                    DistributionFunction function = distrType == HASH_DISTRIBUTED
                        ? distribution.function()
                        : DistributionFunction.HashDistribution.INSTANCE;

                    IgniteDistribution outDistr = hash(joinInfo.leftKeys, function);

                    if (distrType == HASH_DISTRIBUTED && !outDistr.satisfies(distribution)) {
                        outTraits = out.replace(single());
                        leftTraits = left.replace(single());
                        rightTraits = right.replace(single());

                        traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                        break;
                    }

                    outTraits = out.replace(outDistr);
                    leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                    rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                    traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                    if (joinType == INNER || joinType == LEFT) {
                        leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                        rightTraits = right.replace(broadcast());

                        traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }

                    if (joinType == INNER || joinType == RIGHT) {
                        leftTraits = left.replace(broadcast());
                        rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                        traits0.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }

                    break;

                 default:
                    break;
            }
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost cost = super.computeSelfCost(planner, mq);

        return cost;
    }
}

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public class IgniteJoin extends Join implements IgniteRel {
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
    @Override public RelNode passThrough(RelTraitSet required) {
        required = fixTraits(required);

        Pair<RelCollation, RelCollation> inCollations = inCollations(Commons.collation(required));
        if (inCollations == null)
            return passThrough(required.replace(RelCollations.EMPTY));

        IgniteDistribution toDistr = Commons.distribution(required);

        Set<Pair<RelTraitSet, List<RelTraitSet>>> traits = new HashSet<>();

        RelOptCluster cluster = getCluster();
        RelTraitSet outTraits, leftTraits, rightTraits;

        RelDistribution.Type distrType = toDistr.getType();
        switch (distrType) {
            case BROADCAST_DISTRIBUTED:
            case SINGLETON:
                outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(toDistr)
                    .replace(Commons.collation(required));

                leftTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(toDistr)
                    .replace(inCollations.left);

                rightTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(toDistr)
                    .replace(inCollations.right);

                traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                break;
            case HASH_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
                if (joinType != LEFT && joinType != RIGHT && (joinType != INNER || F.isEmpty(joinInfo.pairs())))
                    return passThrough(required.replace(IgniteDistributions.single()));

                DistributionFunction function = distrType == HASH_DISTRIBUTED
                    ? toDistr.function()
                    : DistributionFunction.HashDistribution.INSTANCE;

                IgniteDistribution outDistr = hash(joinInfo.leftKeys, function);

                if (distrType == HASH_DISTRIBUTED && !outDistr.satisfies(toDistr))
                    return passThrough(required.replace(IgniteDistributions.single()));

                outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(outDistr)
                    .replace(Commons.collation(required));

                leftTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(hash(joinInfo.leftKeys, function))
                    .replace(inCollations.left);
                rightTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(hash(joinInfo.rightKeys, function))
                    .replace(inCollations.right);

                traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                if (joinType == INNER || joinType == LEFT) {
                    leftTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                        .replace(hash(joinInfo.leftKeys, function))
                        .replace(inCollations.left);
                    rightTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                        .replace(broadcast())
                        .replace(inCollations.right);

                    traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                }

                if (joinType == INNER || joinType == RIGHT) {
                    leftTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                        .replace(broadcast())
                        .replace(inCollations.left);
                    rightTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                        .replace(hash(joinInfo.rightKeys, function))
                        .replace(inCollations.right);

                    traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                }

             default:
                break;
        }

        List<RelNode> nodes = createNodes(traits);

        RelOptPlanner planner = getCluster().getPlanner();
        for (int i = 1; i < nodes.size(); i++)
            planner.register(nodes.get(i), this);

        return F.first(nodes);
    }

    /** {@inheritDoc} */
    @Override public List<RelNode> derive(List<List<RelTraitSet>> inTraits) {
        Set<Pair<RelTraitSet, List<RelTraitSet>>> traits = new HashSet<>();

        RelOptCluster cluster = getCluster();

        for (Pair<RelTraitSet, RelTraitSet> inTraits0 : inputTraits(inTraits)) {
            RelCollation leftCollation = Commons.collation(inTraits0.left);
            RelCollation rightCollation = Commons.collation(inTraits0.right);

            IgniteDistribution leftDistr = Commons.distribution(inTraits0.left);
            IgniteDistribution rightDistr = Commons.distribution(inTraits0.right);

            RelCollation outCollation = outCollation(leftCollation, rightCollation);

            RelTraitSet outTraits, leftTraits, rightTraits;

            outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(outCollation)
                    .replace(single());

            leftTraits = inTraits0.left.replace(single());
            rightTraits = inTraits0.right.replace(single());

            traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

            outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(outCollation)
                .replace(broadcast());

            leftTraits = inTraits0.left.replace(broadcast());
            rightTraits = inTraits0.right.replace(broadcast());

            traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

            if (joinType == LEFT || joinType == RIGHT || (joinType == INNER && !F.isEmpty(joinInfo.pairs()))) {
                Set<DistributionFunction> functions = new HashSet<>();

                if (leftDistr.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                    && Objects.equals(joinInfo.leftKeys, leftDistr.getKeys()))
                    functions.add(leftDistr.function());

                if (rightDistr.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                    && Objects.equals(joinInfo.rightKeys, rightDistr.getKeys()))
                    functions.add(rightDistr.function());

                functions.add(DistributionFunction.HashDistribution.INSTANCE);

                for (DistributionFunction factory : functions) {
                    outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                        .replace(outCollation)
                        .replace(hash(joinInfo.leftKeys, factory));

                    leftTraits = inTraits0.left.replace(hash(joinInfo.leftKeys, factory));
                    rightTraits = inTraits0.right.replace(hash(joinInfo.rightKeys, factory));

                    traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                    if (joinType == INNER || joinType == LEFT) {
                        leftTraits = inTraits0.left.replace(hash(joinInfo.leftKeys, factory));
                        rightTraits = inTraits0.right.replace(broadcast());

                        traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }

                    if (joinType == INNER || joinType == RIGHT) {
                        leftTraits = inTraits0.left.replace(broadcast());
                        rightTraits = inTraits0.right.replace(hash(joinInfo.rightKeys, factory));

                        traits.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }
                }
            }
        }

        return createNodes(traits);
    }

    /** {@inheritDoc} */
    @Override public DeriveMode getDeriveMode() {
        return DeriveMode.OMAKASE;
    }

    /** */
    private RelCollation outCollation(RelCollation left, RelCollation right) {
        switch (joinType) {
            case SEMI:
            case ANTI:
            case INNER:
            case LEFT:
                return left;
            case RIGHT:
            case FULL:
                for (RelFieldCollation field : left.getFieldCollations()) {
                    if (RelFieldCollation.NullDirection.LAST != field.nullDirection)
                        return RelCollations.EMPTY;
                }

                return left;
        }

        return RelCollations.EMPTY;
    }

    /** */
    private Pair<RelCollation, RelCollation> inCollations(RelCollation out) {
        switch (joinType) {
            case SEMI:
            case ANTI:
            case INNER:
            case LEFT:
                return Pair.of(out, RelCollations.EMPTY);
            case RIGHT:
            case FULL:
                for (RelFieldCollation field : out.getFieldCollations()) {
                    if (RelFieldCollation.NullDirection.LAST != field.nullDirection)
                        return null;
                }

                return Pair.of(out, RelCollations.EMPTY);
        }

        return null;
    }

    /** */
    private Collection<Pair<RelTraitSet, RelTraitSet>> inputTraits(List<List<RelTraitSet>> inputTraits) {
        assert !F.isEmpty(inputTraits) && inputTraits.size() == 2;
        int size = inputTraits.get(0).size() * inputTraits.get(1).size();
        Set<Pair<RelTraitSet, RelTraitSet>> pairs = U.newHashSet(size);
        for (RelTraitSet left : inputTraits.get(0)) {
            for (RelTraitSet right : inputTraits.get(1))
                pairs.add(Pair.of(fixTraits(left), fixTraits(right)));
        }
        return pairs;
    }

    /** */
    private List<RelNode> createNodes(Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        List<RelNode> res = new ArrayList<>(traits.size());
        for (Pair<RelTraitSet, List<RelTraitSet>> p : traits) {
            int size = getInputs().size();
            assert size == p.right.size();

            List<RelNode> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                list.add(RelOptRule.convert(getInput(i), p.right.get(i)));

            res.add(copy(p.left, list));
        }

        return res;
    }
}

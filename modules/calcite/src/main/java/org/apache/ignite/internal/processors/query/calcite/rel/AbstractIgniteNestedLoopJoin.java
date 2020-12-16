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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdRowCount.joinRowCount;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** */
public abstract class AbstractIgniteNestedLoopJoin extends Join implements TraitsAwareIgniteRel {
    /** */
    protected AbstractIgniteNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
        RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public abstract Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
        JoinRelType joinType, boolean semiJoinDone);

    /** {@inheritDoc} */
    @Override public abstract <T> T accept(IgniteRelVisitor<T> visitor);

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("variablesSet", Commons.transform(variablesSet.asList(), CorrelationId::getId), pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // We preserve left collation since it's translated into a nested loop join with an outer loop
        // over a left edge. The code below checks and projects left collation on an output row type.

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RelTraitSet outTraits, leftTraits, rightTraits;

        RelCollation collation = TraitUtils.collation(left);

        // If nulls are possible at left we has to check whether NullDirection.LAST flag is set on sorted fields.
        // TODO set NullDirection.LAST for insufficient fields instead of erasing collation.
        if (joinType == RIGHT || joinType == JoinRelType.FULL) {
            for (RelFieldCollation field : collation.getFieldCollations()) {
                if (RelFieldCollation.NullDirection.LAST != field.nullDirection) {
                    collation = RelCollations.EMPTY;
                    break;
                }
            }
        }

        outTraits = nodeTraits.replace(collation);
        leftTraits = left.replace(collation);
        rightTraits = right.replace(RelCollations.EMPTY);

        return ImmutableList.of(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The node is rewindable only if both sources are rewindable.

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RewindabilityTrait leftRewindability = TraitUtils.rewindability(left);
        RewindabilityTrait rightRewindability = TraitUtils.rewindability(right);

        RelTraitSet outTraits, leftTraits, rightTraits;

        if (leftRewindability.rewindable() && rightRewindability.rewindable()) {
            outTraits = nodeTraits.replace(RewindabilityTrait.REWINDABLE);
            leftTraits = left.replace(RewindabilityTrait.REWINDABLE);
            rightTraits = right.replace(RewindabilityTrait.REWINDABLE);
        }
        else {
            outTraits = nodeTraits.replace(RewindabilityTrait.ONE_WAY);
            leftTraits = left.replace(RewindabilityTrait.ONE_WAY);
            rightTraits = right.replace(RewindabilityTrait.ONE_WAY);
        }

        return ImmutableList.of(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Tere are several rules:
        // 1) any join is possible on broadcast or single distribution
        // 2) hash distributed join is possible when join keys equal to source distribution keys
        // 3) hash and broadcast distributed tables can be joined when join keys equal to hash
        //    distributed table distribution keys and:
        //      3.1) it's a left join and a hash distributed table is at left
        //      3.2) it's a right join and a hash distributed table is at right
        //      3.3) it's an inner join, this case a hash distributed table may be at any side

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>();

        IgniteDistribution leftDistr = TraitUtils.distribution(left);
        IgniteDistribution rightDistr = TraitUtils.distribution(right);

        RelTraitSet outTraits, leftTraits, rightTraits;

        if (leftDistr == broadcast() || rightDistr == broadcast()) {
            outTraits = nodeTraits.replace(broadcast());
            leftTraits = left.replace(broadcast());
            rightTraits = right.replace(broadcast());

            res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
        }

        if (leftDistr == single() || rightDistr == single()) {
            outTraits = nodeTraits.replace(single());
            leftTraits = left.replace(single());
            rightTraits = right.replace(single());

            res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
        }

        if (!F.isEmpty(joinInfo.pairs())) {
            Set<DistributionFunction> functions = new HashSet<>();

            if (leftDistr.getType() == HASH_DISTRIBUTED
                && Objects.equals(joinInfo.leftKeys, leftDistr.getKeys()))
                functions.add(leftDistr.function());

            if (rightDistr.getType() == HASH_DISTRIBUTED
                && Objects.equals(joinInfo.rightKeys, rightDistr.getKeys()))
                functions.add(rightDistr.function());

            functions.add(DistributionFunction.hash());

            for (DistributionFunction function : functions) {
                leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                // TODO distribution multitrait support
                outTraits = nodeTraits.replace(hash(joinInfo.leftKeys, function));
                res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                outTraits = nodeTraits.replace(hash(joinInfo.rightKeys, function));
                res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                if (joinType == INNER || joinType == LEFT) {
                    outTraits = nodeTraits.replace(hash(joinInfo.leftKeys, function));
                    leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                    rightTraits = right.replace(broadcast());

                    res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                }

                if (joinType == INNER || joinType == RIGHT) {
                    outTraits = nodeTraits.replace(hash(joinInfo.rightKeys, function));
                    leftTraits = left.replace(broadcast());
                    rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                    res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                }
            }
        }

        if (!res.isEmpty())
            return res;

        return ImmutableList.of(Pair.of(nodeTraits.replace(single()),
            ImmutableList.of(left.replace(single()), right.replace(single()))));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // We preserve left collation since it's translated into a nested loop join with an outer loop
        // over a left edge. The code below checks whether a desired collation is possible and requires
        // appropriate collation from the left edge.

        RelCollation collation = TraitUtils.collation(nodeTraits);

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        if (collation.equals(RelCollations.EMPTY))
            return Pair.of(nodeTraits,
                ImmutableList.of(left.replace(RelCollations.EMPTY), right.replace(RelCollations.EMPTY)));

        if (!projectsLeft(collation))
            collation = RelCollations.EMPTY;
        else if (joinType == RIGHT || joinType == JoinRelType.FULL) {
            for (RelFieldCollation field : collation.getFieldCollations()) {
                if (RelFieldCollation.NullDirection.LAST != field.nullDirection) {
                    collation = RelCollations.EMPTY;
                    break;
                }
            }
        }

        return Pair.of(nodeTraits.replace(collation),
            ImmutableList.of(left.replace(collation), right.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Tere are several rules:
        // 1) any join is possible on broadcast or single distribution
        // 2) hash distributed join is possible when join keys equal to source distribution keys
        // 3) hash and broadcast distributed tables can be joined when join keys equal to hash
        //    distributed table distribution keys and:
        //      3.1) it's a left join and a hash distributed table is at left
        //      3.2) it's a right join and a hash distributed table is at right
        //      3.3) it's an inner join, this case a hash distributed table may be at any side

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        RelDistribution.Type distrType = distribution.getType();
        switch (distrType) {
            case BROADCAST_DISTRIBUTED:
            case SINGLETON:
                return Pair.of(nodeTraits, Commons.transform(inputTraits, t -> t.replace(distribution)));

            case HASH_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
                // Such join may be replaced as a cross join with a filter uppon it.
                // It's impossible to get random or hash distribution from a cross join.
                if (F.isEmpty(joinInfo.pairs()))
                    break;

                // We cannot provide random distribution without unique constrain on join keys,
                // so, we require hash distribution (wich satisfies random distribution) instead.
                DistributionFunction function = distrType == HASH_DISTRIBUTED
                    ? distribution.function()
                    : DistributionFunction.hash();

                IgniteDistribution outDistr = hash(joinInfo.leftKeys, function);

                if (distrType != HASH_DISTRIBUTED || outDistr.satisfies(distribution)) {
                    return Pair.of(nodeTraits.replace(outDistr),
                        ImmutableList.of(left.replace(outDistr), right.replace(hash(joinInfo.rightKeys, function))));
                }

            default:
                // NO-OP
        }

        return Pair.of(nodeTraits.replace(single()), Commons.transform(inputTraits, t -> t.replace(single())));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return Util.first(joinRowCount(mq, this), 1D);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);

        // Joins can be flipped, and for many algorithms, both versions are viable
        // and have the same cost. To make the results stable between versions of
        // the planner, make one of the versions slightly more expensive.
        if (joinType == RIGHT)
            rowCount = RelMdUtil.addEpsilon(rowCount);

        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);

        if (Double.isInfinite(leftRowCount))
            rowCount = leftRowCount;
        if (Double.isInfinite(rightRowCount))
            rowCount = rightRowCount;

        if (!Double.isInfinite(leftRowCount) && !Double.isInfinite(rightRowCount) && leftRowCount > rightRowCount)
            rowCount = RelMdUtil.addEpsilon(rowCount);

        RelDistribution.Type type = distribution().getType();

        if (type == SINGLETON)
            rowCount = RelMdUtil.addEpsilon(rowCount);

        if (type == BROADCAST_DISTRIBUTED)
            rowCount = RelMdUtil.addEpsilon(RelMdUtil.addEpsilon(rowCount));

        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    /** */
    protected boolean projectsLeft(RelCollation collation) {
        int leftFieldCount = getLeft().getRowType().getFieldCount();
        for (int field : RelCollations.ordinals(collation)) {
            if (field >= leftFieldCount)
                return false;
        }
        return true;
    }
}

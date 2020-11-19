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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.util.NumberUtil.multiply;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** */
public class IgniteMergeJoin extends Join implements TraitsAwareIgniteRel {
    /** */
    public IgniteMergeJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
        RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    /** */
    public IgniteMergeJoin(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class));
    }

    /** {@inheritDoc} */
    @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
        JoinRelType joinType, boolean semiJoinDone) {
        return new IgniteMergeJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMergeJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(), getVariablesSet(), getJoinType());
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf(
                "variablesSet",
                Commons.transform(variablesSet.asList(), CorrelationId::getId),
                pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES
            );
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);
        RelCollation leftCollation = TraitUtils.collation(left), rightCollation = TraitUtils.collation(right);

        List<Integer> newLeftCollation, newRightCollation;

        if (isPrefix(leftCollation.getKeys(), joinInfo.leftKeys)) { // preserve left collation
            newLeftCollation = new ArrayList<>(leftCollation.getKeys());

            Map<Integer, Integer> leftToRight = joinInfo.pairs().stream()
                .collect(Collectors.toMap(p -> p.source, p -> p.target));

            newRightCollation = newLeftCollation.stream().map(leftToRight::get).collect(Collectors.toList());
        }
        else if (isPrefix(rightCollation.getKeys(), joinInfo.rightKeys)) { // preserve right collation
            newRightCollation = new ArrayList<>(rightCollation.getKeys());

            Map<Integer, Integer> rightToLeft = joinInfo.pairs().stream()
                .collect(Collectors.toMap(p -> p.target, p -> p.source));

            newLeftCollation = newRightCollation.stream().map(rightToLeft::get).collect(Collectors.toList());
        }
        else { // generate new collations
            // TODO: generate permutations when there will be multitraits

            newLeftCollation = new ArrayList<>(joinInfo.leftKeys);
            newRightCollation = new ArrayList<>(joinInfo.rightKeys);
        }

        leftCollation = createCollation(newLeftCollation);
        rightCollation = createCollation(newRightCollation);

        return ImmutableList.of(
            Pair.of(
                nodeTraits.replace(leftCollation),
                ImmutableList.of(
                    left.replace(leftCollation),
                    right.replace(rightCollation)
                )
            )
        );
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
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
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
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
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelCollation collation = TraitUtils.collation(nodeTraits);
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        int rightOff = this.left.getRowType().getFieldCount();

        Map<Integer, Integer> rightToLeft = joinInfo.pairs().stream()
            .collect(Collectors.toMap(p -> p.target, p -> p.source));

        List<Integer> collationLeftPrj = new ArrayList<>();

        for (Integer c : collation.getKeys()) {
            collationLeftPrj.add(
                c >= rightOff ? rightToLeft.get(c - rightOff) : c
            );
        }

        boolean preserveNodeCollation = false;

        List<Integer> newLeftCollation, newRightCollation;

        Map<Integer, Integer> leftToRight = joinInfo.pairs().stream()
            .collect(Collectors.toMap(p -> p.source, p -> p.target));

        if (isPrefix(collationLeftPrj, joinInfo.leftKeys)) { // preserve collation
            newLeftCollation = new ArrayList<>();
            newRightCollation = new ArrayList<>();

            int ind = 0;
            for (Integer c : collation.getKeys()) {
                if (c < rightOff) {
                    newLeftCollation.add(c);

                    if (ind < joinInfo.leftKeys.size())
                        newRightCollation.add(leftToRight.get(c));
                }
                else {
                    c -= rightOff;
                    newRightCollation.add(c);

                    if (ind < joinInfo.leftKeys.size())
                        newLeftCollation.add(rightToLeft.get(c));
                }

                ind++;
            }

            preserveNodeCollation = true;
        }
        else { // generate new collations
            newLeftCollation = maxPrefix(collationLeftPrj, joinInfo.leftKeys);

            Set<Integer> tail = new HashSet<>(joinInfo.leftKeys);

            tail.removeAll(newLeftCollation);

            // TODO: generate permutations when there will be multitraits
            newLeftCollation.addAll(tail);

            newRightCollation = newLeftCollation.stream().map(leftToRight::get).collect(Collectors.toList());
        }

        RelCollation leftCollation = createCollation(newLeftCollation);
        RelCollation rightCollation = createCollation(newRightCollation);

        return ImmutableList.of(
            Pair.of(
                nodeTraits.replace(preserveNodeCollation ? collation : leftCollation),
                ImmutableList.of(
                    left.replace(leftCollation),
                    right.replace(rightCollation)
                )
            )
        );
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        // The node is rewindable only if both sources are rewindable.

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return ImmutableList.of(Pair.of(nodeTraits.replace(rewindability),
            ImmutableList.of(left.replace(rewindability), right.replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
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

        RelTraitSet outTraits, leftTraits, rightTraits;

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        RelDistribution.Type distrType = distribution.getType();
        switch (distrType) {
            case BROADCAST_DISTRIBUTED:
            case SINGLETON:
                outTraits = nodeTraits.replace(distribution);
                leftTraits = left.replace(distribution);
                rightTraits = right.replace(distribution);

                res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                break;
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

                IgniteDistribution outDistr; // TODO distribution multitrait support

                outDistr = hash(joinInfo.leftKeys, function);

                if (distrType != HASH_DISTRIBUTED || outDistr.satisfies(distribution)) {
                    outTraits = nodeTraits.replace(outDistr);
                    leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                    rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                    res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                    if (joinType == INNER || joinType == LEFT) {
                        outTraits = nodeTraits.replace(outDistr);
                        leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                        rightTraits = right.replace(broadcast());

                        res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }
                }

                outDistr = hash(joinInfo.rightKeys, function);

                if (distrType != HASH_DISTRIBUTED || outDistr.satisfies(distribution)) {
                    outTraits = nodeTraits.replace(outDistr);
                    leftTraits = left.replace(hash(joinInfo.leftKeys, function));
                    rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                    res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

                    if (joinType == INNER || joinType == RIGHT) {
                        outTraits = nodeTraits.replace(outDistr);
                        leftTraits = left.replace(broadcast());
                        rightTraits = right.replace(hash(joinInfo.rightKeys, function));

                        res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));
                    }
                }

                break;

            default:
                break;
        }

        if (!res.isEmpty())
            return res;

        return ImmutableList.of(Pair.of(nodeTraits.replace(single()),
            ImmutableList.of(left.replace(single()), right.replace(single()))));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        @Nullable Double result = null;
        boolean finished = false;
        if (!getJoinType().projectsRight()) {
          // Create a RexNode representing the selectivity of the
          // semijoin filter and pass it to getSelectivity
          RexNode semiJoinSelectivity =
              RelMdUtil.makeSemiJoinSelectivityRexNode(mq, this);

            result = multiply(mq.getSelectivity(getLeft(), semiJoinSelectivity),
                mq.getRowCount(getLeft()));
        }
        else { // Row count estimates of 0 will be rounded up to 1.
               // So, use maxRowCount where the product is very small.
            final Double left1 = mq.getRowCount(getLeft());
            final Double right1 = mq.getRowCount(getRight());
            if (left1 != null && right1 != null) {
                if (left1 <= 1D || right1 <= 1D) {
                    Double max = mq.getMaxRowCount(this);
                    if (max != null && max <= 1D) {
                        result = max;
                        finished = true;
                    }
                }
                if (!finished) {
                    JoinInfo joinInfo1 = analyzeCondition();
                    ImmutableIntList leftKeys = joinInfo1.leftKeys;
                    ImmutableIntList rightKeys = joinInfo1.rightKeys;
                    double selectivity = mq.getSelectivity(this, getCondition());
                    if (F.isEmpty(leftKeys) || F.isEmpty(rightKeys)) {
                        result = left1 * right1 * selectivity;
                    }
                    else {
                        double leftDistinct = Util.first(
                            mq.getDistinctRowCount(getLeft(), ImmutableBitSet.of(leftKeys), null), left1);
                        double rightDistinct = Util.first(
                            mq.getDistinctRowCount(getRight(), ImmutableBitSet.of(rightKeys), null), right1);
                        double leftCardinality = leftDistinct / left1;
                        double rightCardinality = rightDistinct / right1;
                        double rowsCount = (Math.min(left1, right1) / (leftCardinality * rightCardinality)) * selectivity;
                        JoinRelType type = getJoinType();
                        if (type == LEFT)
                            rowsCount += left1;
                        else if (type == RIGHT)
                            rowsCount += right1;
                        else if (type == JoinRelType.FULL)
                            rowsCount += left1 + right1;
                        result = rowsCount;
                    }
                }
            }
        }

        return Util.first(result, 1D);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);

        // Joins can be flipped, and for many algorithms, both versions are viable
        // and have the same cost. To make the results stable between versions of
        // the planner, make one of the versions slightly more expensive.
        switch (joinType) {
            case SEMI:
            case ANTI:
                // SEMI and ANTI join cannot be flipped
                break;
            case RIGHT:
                rowCount = RelMdUtil.addEpsilon(rowCount);
                break;
            default:
                if (RelNodes.COMPARATOR.compare(left, right) > 0)
                    rowCount = RelMdUtil.addEpsilon(rowCount);
        }

        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);

        if (Double.isInfinite(leftRowCount))
            rowCount = leftRowCount;
        if (Double.isInfinite(rightRowCount))
            rowCount = rightRowCount;

        RelDistribution.Type type = distribution().getType();

        if (type == BROADCAST_DISTRIBUTED || type == SINGLETON)
            rowCount = RelMdUtil.addEpsilon(rowCount);

        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    private static <T> List<T> maxPrefix(List<T> seq, Collection<T> elems) {
        List<T> res = new ArrayList<>();

        Set<T> elems0 = new HashSet<>(elems);

        for (T e : seq) {
            if (!elems0.remove(e))
                break;

            res.add(e);
        }

        return res;
    }

    private static <T> boolean isPrefix(List<T> seq, Collection<T> elems) {
        Set<T> elems0 = new HashSet<>(elems);

        if (seq.size() < elems0.size())
            return false;

        for (T e : seq) {
            if (!elems0.remove(e))
                return false;

            if (elems0.isEmpty())
                break;
        }

        return true;
    }

    /**
     * Creates collations from provided keys.
     *
     * @param keys The keys to create collation from.
     * @return New collation.
     */
    private static RelCollation createCollation(List<Integer> keys) {
        return RelCollations.of(
            keys.stream().map(RelFieldCollation::new).collect(Collectors.toList())
        );
    }
}

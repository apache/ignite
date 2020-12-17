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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public class IgniteMergeJoin extends AbstractIgniteJoin {
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
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
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

        return Pair.of(
            nodeTraits.replace(preserveNodeCollation ? collation : leftCollation),
            ImmutableList.of(
                left.replace(leftCollation),
                right.replace(rightCollation)
            )
        );
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

        double rows = leftCount + rightCount;

        return costFactory.makeCost(rows,
            rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /**
     * Returns the longest possible prefix of {@code seq} that could be form from provided {@code elems}.
     *
     * @param seq Sequence.
     * @param elems Elems.
     * @return The longest possible prefix of {@code seq}.
     */
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

    /**
     * Checks if there is a such permutation of all {@code elems} that is prefix of
     * provided {@code seq}.
     *
     * @param seq Sequence.
     * @param elems Elems.
     * @return {@code true} if there is a permutation of all {@code elems} that is prefix of {@code seq}.
     */
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

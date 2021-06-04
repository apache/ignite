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
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.createCollation;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.isPrefix;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.maxPrefix;

/** */
public class IgniteMergeJoin extends AbstractIgniteJoin {
    /**
     * Collation of a left child. Keep it here to restore after deserialization.
     */
    private final RelCollation leftCollation;

    /**
     * Collation of a right child. Keep it here to restore after deserialization.
     */
    private final RelCollation rightCollation;

    /** */
    public IgniteMergeJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType
    ) {
        this(cluster, traitSet, left, right, condition, variablesSet, joinType,
            left.getTraitSet().getCollation(), right.getTraitSet().getCollation());
    }

    /** */
    public IgniteMergeJoin(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class),
            null,//((RelInputEx)input).getCollation("leftCollation"),
            null//((RelInputEx)input).getCollation("rightCollation")
        );
    }

    /** */
    private IgniteMergeJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType,
        RelCollation leftCollation,
        RelCollation rightCollation
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);

        this.leftCollation = leftCollation;
        this.rightCollation = rightCollation;
    }

    /** {@inheritDoc} */
    @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
        JoinRelType joinType, boolean semiJoinDone) {
        return new IgniteMergeJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType,
            leftCollation, rightCollation);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMergeJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
            getVariablesSet(), getJoinType(), leftCollation, rightCollation);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);
        RelCollation leftCollation = TraitUtils.collation(left), rightCollation = TraitUtils.collation(right);

        if (isPrefix(leftCollation.getKeys(), joinInfo.leftKeys)) // preserve left collation
            rightCollation = leftCollation.apply(buildProjectionMapping(true));

        else if (isPrefix(rightCollation.getKeys(), joinInfo.rightKeys))// preserve right collation
            leftCollation = rightCollation.apply(buildProjectionMapping(false));

        else { // generate new collations
            leftCollation = RelCollations.of(joinInfo.leftKeys);
            rightCollation = RelCollations.of(joinInfo.rightKeys);
        }

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

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("leftCollation", leftCollation)
            .item("rightCollation", rightCollation);
    }

    /**
     * @return Collation of a left child.
     */
    public RelCollation leftCollation() {
        return leftCollation;
    }

    /**
     * @return Collation of a right child.
     */
    public RelCollation rightCollation() {
        return rightCollation;
    }
}

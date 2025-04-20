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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.calcite.rel.RelCollations.EMPTY;
import static org.apache.calcite.rel.RelCollations.containsOrderless;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;

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
            ((RelInputEx)input).getCollation("leftCollation"),
            ((RelInputEx)input).getCollation("rightCollation")
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
            left.getTraitSet().getCollation(), right.getTraitSet().getCollation());
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

        if (containsOrderless(leftCollation, joinInfo.leftKeys)) // preserve left collation
            rightCollation = leftCollation.apply(buildTransposeMapping(true));

        else if (containsOrderless(rightCollation, joinInfo.rightKeys)) // preserve right collation
            leftCollation = rightCollation.apply(buildTransposeMapping(false));

        else { // generate new collations
            leftCollation = RelCollations.of(joinInfo.leftKeys);
            rightCollation = RelCollations.of(joinInfo.rightKeys);
        }

        RelCollation desiredCollation = leftCollation;

        if (joinType == RIGHT || joinType == FULL)
            desiredCollation = RelCollations.EMPTY;

        return ImmutableList.of(
            Pair.of(
                nodeTraits.replace(desiredCollation),
                ImmutableList.of(
                    left.replace(leftCollation),
                    right.replace(rightCollation)
                )
            )
        );
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
        RelTraitSet required,
        List<RelTraitSet> inputTraits
    ) {
        RelCollation collation = TraitUtils.collation(required);
        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        if (joinType == FULL)
            return defaultCollationPair(required, left, right);

        int leftInputFieldCnt = this.left.getRowType().getFieldCount();

        List<Integer> reqKeys = RelCollations.ordinals(collation);
        List<Integer> leftKeys = joinInfo.leftKeys.toIntegerList();
        List<Integer> rightKeys = joinInfo.rightKeys.incr(leftInputFieldCnt).toIntegerList();

        ImmutableBitSet reqKeySet = ImmutableBitSet.of(reqKeys);
        ImmutableBitSet leftKeySet = ImmutableBitSet.of(joinInfo.leftKeys);
        ImmutableBitSet rightKeySet = ImmutableBitSet.of(rightKeys);

        RelCollation nodeCollation;
        RelCollation leftCollation;
        RelCollation rightCollation;

        if (reqKeySet.equals(leftKeySet)) {
            if (joinType == RIGHT)
                return defaultCollationPair(required, left, right);

            nodeCollation = collation;
            leftCollation = collation;
            rightCollation = collation.apply(buildTransposeMapping(true));
        }
        else if (containsOrderless(leftKeys, collation)) {
            if (joinType == RIGHT)
                return defaultCollationPair(required, left, right);

            // if sort keys are subset of left join keys, we can extend collations to make sure all join
            // keys are sorted.
            nodeCollation = collation;
            leftCollation = extendCollation(collation, leftKeys);
            rightCollation = leftCollation.apply(buildTransposeMapping(true));
        }
        else if (containsOrderless(collation, leftKeys) && reqKeys.stream().allMatch(i -> i < leftInputFieldCnt)) {
            if (joinType == RIGHT)
                return defaultCollationPair(required, left, right);

            // if sort keys are superset of left join keys, and left join keys is prefix of sort keys
            // (order not matter), also sort keys are all from left join input.
            nodeCollation = collation;
            leftCollation = collation;
            rightCollation = leftCollation.apply(buildTransposeMapping(true));
        }
        else if (reqKeySet.equals(rightKeySet)) {
            if (joinType == LEFT)
                return defaultCollationPair(required, left, right);

            nodeCollation = collation;
            rightCollation = RelCollations.shift(collation, -leftInputFieldCnt);
            leftCollation = rightCollation.apply(buildTransposeMapping(false));
        }
        else if (containsOrderless(rightKeys, collation)) {
            if (joinType == LEFT)
                return defaultCollationPair(required, left, right);

            nodeCollation = collation;
            rightCollation = RelCollations.shift(extendCollation(collation, rightKeys), -leftInputFieldCnt);
            leftCollation = rightCollation.apply(buildTransposeMapping(false));
        }
        else
            return defaultCollationPair(required, left, right);

        return Pair.of(
            required.replace(nodeCollation),
            ImmutableList.of(
                left.replace(leftCollation),
                right.replace(rightCollation)
            )
        );
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double leftCnt = mq.getRowCount(getLeft());

        if (Double.isInfinite(leftCnt))
            return costFactory.makeInfiniteCost();

        double rightCnt = mq.getRowCount(getRight());

        if (Double.isInfinite(rightCnt))
            return costFactory.makeInfiniteCost();

        double rows = leftCnt + rightCnt;

        RelOptCost res = costFactory.makeCost(rows,
            rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);


        if (getLeft() instanceof IgniteExchange && getRight() instanceof IgniteExchange)
            System.err.println("TEST | Merge rows: " + estimateRowCount(mq) + ", left: " + leftCnt + ", right: " + rightCnt);

        if (getLeft() instanceof IgniteExchange && getRight() instanceof IgniteNestedLoopJoin)
            System.err.println("TEST | Merge rows: " + estimateRowCount(mq) + ", left: " + leftCnt + ", right: " + rightCnt);

        if (getLeft() instanceof IgniteSort && getRight() instanceof IgniteSort)
            System.err.println("TEST | Merge rows: " + estimateRowCount(mq) + ", left: " + leftCnt + ", right: " + rightCnt);

        //res = costFactory.makeZeroCost();
        //IgniteMdCumulativeCost.printCost(this, res, mq, leftCnt, rightCnt);

        return res;
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

    /** Creates pair with default collation for parent and simple yet sufficient collation for children nodes. */
    private Pair<RelTraitSet, List<RelTraitSet>> defaultCollationPair(
        RelTraitSet nodeTraits,
        RelTraitSet leftInputTraits,
        RelTraitSet rightInputTraits
    ) {
        return Pair.of(
            nodeTraits.replace(EMPTY),
            ImmutableList.of(
                leftInputTraits.replace(RelCollations.of(joinInfo.leftKeys)),
                rightInputTraits.replace(RelCollations.of(joinInfo.rightKeys))
            )
        );
    }

    /**
     * This function extends collation by appending new collation fields defined on keys.
     */
    private static RelCollation extendCollation(RelCollation collation, List<Integer> keys) {
        List<RelFieldCollation> fieldsForNewCollation = new ArrayList<>(keys.size());
        fieldsForNewCollation.addAll(collation.getFieldCollations());

        ImmutableBitSet keysBitset = ImmutableBitSet.of(keys);
        ImmutableBitSet colKeysBitset = ImmutableBitSet.of(collation.getKeys());
        ImmutableBitSet exceptBitset = keysBitset.except(colKeysBitset);

        for (Integer i : exceptBitset)
            fieldsForNewCollation.add(new RelFieldCollation(i));

        return RelCollations.of(fieldsForNewCollation);
    }
}

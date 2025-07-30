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
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public class IgniteHashJoin extends AbstractIgniteJoin {
    /** */
    public IgniteHashJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    /** Constructor. */
    public IgniteHashJoin(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            Set.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class)
        );
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double leftRowCnt = mq.getRowCount(getLeft());

        double rightRowCnt = mq.getRowCount(getRight());

        if (Double.isInfinite(leftRowCnt) || Double.isInfinite(rightRowCnt))
            return planner.getCostFactory().makeInfiniteCost();

        double rowCnt = leftRowCnt + rightRowCnt;

        int rightKeysSize = joinInfo.rightKeys.size();

        double rightSize = rightRowCnt * IgniteCost.AVERAGE_FIELD_SIZE * getRight().getRowType().getFieldCount();

        double distRightRows = Util.first(mq.getDistinctRowCount(right, ImmutableBitSet.of(joinInfo.rightKeys), null), 0.9 * rightRowCnt);

        rightSize += distRightRows * rightKeysSize * IgniteCost.AVERAGE_FIELD_SIZE;

        return costFactory.makeCost(rowCnt, rowCnt * IgniteCost.HASH_LOOKUP_COST, 0, rightSize, 0);
    }

    /** {@inheritDoc} */
    @Override public Join copy(
        RelTraitSet traitSet,
        RexNode condition,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone
    ) {
        return new IgniteHashJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteHashJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
            getVariablesSet(), getJoinType());
    }
}

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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;

/** */
public class CorrelatedNestedLoopJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    /** */
    public static final RelOptRule INSTANCE = new CorrelatedNestedLoopJoinConverterRule(1);

    /** */
    public static final RelOptRule INSTANCE_BATCHED = new CorrelatedNestedLoopJoinConverterRule(100);

    /** */
    private final int batchSize;

    /** */
    public CorrelatedNestedLoopJoinConverterRule(int batchSize) {
        super(LogicalJoin.class, "CorrelatedNestedLoopConverter");

        assert batchSize >= 0;

        this.batchSize = batchSize;
    }

    /** {@inheritDoc} */
    @Override public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.INNER; // TODO LEFT, SEMI, ANTI
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        final int leftFieldCount = rel.getLeft().getRowType().getFieldCount();
        final RelOptCluster cluster = rel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelBuilder relBuilder = relBuilderFactory.create(rel.getCluster(), null);

        final Set<CorrelationId> correlationIds = new HashSet<>();
        final ArrayList<RexNode> corrVar = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            CorrelationId correlationId = cluster.createCorrel();
            correlationIds.add(correlationId);
            corrVar.add(rexBuilder.makeCorrel(rel.getLeft().getRowType(), correlationId));
        }

        // Generate first condition
        final RexNode condition = rel.getCondition().accept(new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef input) {
                int field = input.getIndex();
                if (field >= leftFieldCount)
                    return rexBuilder.makeInputRef(input.getType(), input.getIndex() - leftFieldCount);

                return rexBuilder.makeFieldAccess(corrVar.get(0), field);
            }
        });

        List<RexNode> conditionList = new ArrayList<>();
        conditionList.add(condition);

        // Add batchSize-1 other conditions
        for (int i = 1; i < batchSize; i++) {
            final int corrIndex = i;
            final RexNode condition2 = condition.accept(new RexShuttle() {
                @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                    return corrVar.get(corrIndex);
                }
            });
            conditionList.add(condition2);
        }

        // Push a filter with batchSize disjunctions
        relBuilder.push(rel.getRight()).filter(relBuilder.or(conditionList));
        RelNode right = relBuilder.build();

        JoinRelType joinType = rel.getJoinType();

        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(RewindabilityTrait.REWINDABLE);

        RelNode left = convert(rel.getLeft(), leftInTraits);
        right = convert(right, rightInTraits);

        return new IgniteCorrelatedNestedLoopJoin(cluster, outTraits, left, right, rel.getCondition(), correlationIds, joinType);
    }
}

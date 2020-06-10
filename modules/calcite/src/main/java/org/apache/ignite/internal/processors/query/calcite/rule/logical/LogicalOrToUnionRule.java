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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

/**
 * Converts OR to UNION ALL.
 */
public class LogicalOrToUnionRule extends RelOptRule {
    /** Instance. */
    public static final RelOptRule INSTANCE = new LogicalOrToUnionRule(LogicalFilter.class, "LogicalOrToUnionRule");

    /**
     * Constructor.
     *
     * @param clazz Class of relational expression to match.
     * @param desc  Description, or null to guess description
     */
    private LogicalOrToUnionRule(Class<LogicalFilter> clazz, String desc) {
        super(
            operand(clazz, any()),
            RelFactories.LOGICAL_BUILDER, desc);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalFilter rel = call.rel(0);
        final RelOptCluster cluster = rel.getCluster();

        RexNode dnf = RexUtil.toDnf(cluster.getRexBuilder(), rel.getCondition());

        if (!dnf.isA(SqlKind.OR))
            return;

        List<RexNode> operands = RelOptUtil.disjunctions(dnf);

        if (operands.size() != 2)
            return;

        RelNode input = rel.getInput(0);

        call.transformTo(rel, ImmutableMap.of(
            createUnionAll(cluster, input, operands.get(0), operands.get(1)), rel,
            createUnionAll(cluster, input, operands.get(1), operands.get(0)), rel
        ));
    }

    /**
     * Creates 'UnionAll' for conditions.
     *
     * @param cluster The cluster UnionAll expression will belongs to.
     * @param input Input.
     * @param op1 First filter condition.
     * @param op2 Second filter condition.
     * @return UnionAll expression.
     */
    private RelNode createUnionAll(RelOptCluster cluster, RelNode input, RexNode op1, RexNode op2) {
        final RelBuilder builder = relBuilderFactory.create(cluster, null);
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        builder.push(input).filter(op1);
        builder.push(input).filter(
            builder.and(op2,
                // LNNVL is used here. We must treat 'null' values as valid.
                rexBuilder.makeCall(op1.getType(), SqlStdOperatorTable.CASE,
                    ImmutableList.of(
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, op1),
                        RexUtil.not(op1),
                        rexBuilder.makeLiteral(true))
                )
            )
        );

        return builder.union(true).build();
    }
}

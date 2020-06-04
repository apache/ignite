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

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.calcite.rule.RuleUtils;

/**
 *
 */
public class LogicalOrToUnionRule extends RelOptRule {
    /** Instance. */
    public static final RelOptRule INSTANCE = new LogicalOrToUnionRule();

    /**
     * Constructor.
     */
    public LogicalOrToUnionRule() {
        super(
            operand(LogicalFilter.class, any()),
            RelFactories.LOGICAL_BUILDER, null);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalFilter rel = call.rel(0);
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

        RexNode dnf = RexUtil.toDnf(rexBuilder, rel.getCondition());

        if (!dnf.isA(SqlKind.OR))
            return;

        List<RexNode> operands = ((RexCall)dnf).getOperands();

        if (operands.size() != 2)
            return; // TODO: allow more than 2 operands.

        RelNode input = rel.getInput(0);

        LogicalUnion union1 =
            LogicalUnion.create(Arrays.asList(
                LogicalFilter.create(input, operands.get(0)),
                LogicalFilter.create(input, RexUtil.andNot(rexBuilder, operands.get(1), operands.get(0)))), true);

        LogicalUnion union2 =
            LogicalUnion.create(Arrays.asList(
                LogicalFilter.create(input, operands.get(1)),
                LogicalFilter.create(input, RexUtil.andNot(rexBuilder, operands.get(0), operands.get(1)))), true);
//
//        RuleUtils.transformTo(call, Arrays.asList(RelOptUtil.createCastRel(union1, rel.getRowType(), false),
//            RelOptUtil.createCastRel(union2, rel.getRowType(), false)));

        RuleUtils.transformTo(call, RelOptUtil.createCastRel(union1, rel.getRowType(), false));
    }
}

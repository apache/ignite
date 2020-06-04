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

import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.calcite.tools.RelBuilder;

/**
 *
 */
public class LogicalOrToUnionRule extends RelOptRule {
    /** Instance. */
    public static final RelOptRule INSTANCE = new LogicalOrToUnionRule();

    /**
     * Constructor.
     */
    private LogicalOrToUnionRule() {
        super(
            operand(LogicalFilter.class, any()),
            RelFactories.LOGICAL_BUILDER, null);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalFilter rel = call.rel(0);
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode dnf = RexUtil.toDnf(rexBuilder, rel.getCondition());

        if (!dnf.isA(SqlKind.OR))
            return;

        List<RexNode> operands = RelOptUtil.disjunctions(dnf);
        RelBuilder builder = relBuilderFactory.create(rel.getCluster(), null);
        for (RexNode operand : operands) {
            List<RexNode> others = operands.stream()
                .filter(op -> op != operand)
                .collect(Collectors.toList());

            builder.push(rel.getInput()).filter(RexUtil.andNot(rexBuilder, operand, others));
        }

        RelNode union = builder.union(true).build();
        call.transformTo(union);
    }
}

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
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

/**
 * Converts OR to UNION ALL.
 */
public class LogicalOrToUnionRule extends RelRule<LogicalOrToUnionRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private LogicalOrToUnionRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalFilter rel = call.rel(0);
        final RelOptCluster cluster = rel.getCluster();

        RexNode dnf = RexUtil.toDnf(cluster.getRexBuilder(), rel.getCondition());

        if (!dnf.isA(SqlKind.OR))
            return;

        List<RexNode> operands = RelOptUtil.disjunctions(dnf);

        if (operands.size() != 2 || RexUtil.find(SqlKind.IS_NULL).anyContain(operands))
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
        RelBuilder relBldr = relBuilderFactory.create(cluster, null);

        return relBldr
            .push(input).filter(op1)
            .push(input).filter(
                relBldr.and(op2,
                    relBldr.or(relBldr.isNull(op1), relBldr.not(op1))))
            .union(true)
            .build();
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = RelRule.Config.EMPTY
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .withDescription("LogicalOrToUnionRule")
            .as(Config.class)
            .withOperandFor(LogicalFilter.class);

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Filter> filterClass) {
            return withOperandSupplier(o -> o.operand(filterClass).anyInputs())
                .as(Config.class);
        }

        /** {@inheritDoc} */
        @Override default LogicalOrToUnionRule toRule() {
            return new LogicalOrToUnionRule(this);
        }
    }
}

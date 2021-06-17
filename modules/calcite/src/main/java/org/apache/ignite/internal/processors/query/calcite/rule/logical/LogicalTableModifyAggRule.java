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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class LogicalTableModifyAggRule extends RelRule<LogicalTableModifyAggRule.Config> {

    /** Instance. */
    public static final RelOptRule INSTANCE = LogicalTableModifyAggRule.Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private LogicalTableModifyAggRule(LogicalTableModifyAggRule.Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalTableModify rel = call.rel(0);
        final RelOptCluster cluster = rel.getCluster();
        final RelBuilder relBuilder = relBuilderFactory.create(cluster, null);

        relBuilder.push(rel);

        relBuilder.aggregate(relBuilder.groupKey(),
                relBuilder.aggregateCall(SqlStdOperatorTable.SUM0, relBuilder.field(0)).as("ROWCOUNT"));

        call.transformTo(relBuilder.build());
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        LogicalTableModifyAggRule.Config DEFAULT = RelRule.Config.EMPTY
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withDescription("LogicalTableModifyAggRule")
                .as(LogicalTableModifyAggRule.Config.class)
                .withOperandFor(LogicalTableModify.class);

        /** Defines an operand tree for the given classes. */
        default LogicalTableModifyAggRule.Config withOperandFor(Class<? extends LogicalTableModify> modifyClass) {
            return withOperandSupplier(o -> o.operand(modifyClass).anyInputs())
                    .as(LogicalTableModifyAggRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override default LogicalTableModifyAggRule toRule() {
            return new LogicalTableModifyAggRule(this);
        }
    }
}

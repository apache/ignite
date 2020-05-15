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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;

public class FilterJoinRule {
    /** Rule that pushes predicates from a Filter into the Join below them. */
    public static final FilterIntoJoinRule FILTER_ON_JOIN = new FilterIntoJoinRule();

    /** Rule that pushes predicates in a Join into the inputs to the join. */
    public static final JoinConditionPushRule JOIN = new JoinConditionPushRule();

    /** Rule that pushes down expressions in "equal" join condition. */
    public static final JoinPushExpressionsRule PUSH_JOIN_CONDITION =
        new JoinPushExpressionsRule(LogicalJoin.class, RelFactories.LOGICAL_BUILDER);

    /**
     * Rule that tries to push filter expressions into a join condition and into the inputs of the join.
     */
    public static class FilterIntoJoinRule extends org.apache.calcite.rel.rules.FilterJoinRule {
        /**
         * Creates a converter.
         */
        public FilterIntoJoinRule() {
            super(
                operand(LogicalFilter.class,
                    operand(LogicalJoin.class, RelOptRule.any())),
                "FilterJoinRule:filter", true, RelFactories.LOGICAL_BUILDER,
                org.apache.calcite.rel.rules.FilterJoinRule.TRUE_PREDICATE);
        }

        /** {@inheritDoc} */
        @Override public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            Join join = call.rel(1);
            perform(call, filter, join);
        }
    }

    /** Rule that pushes parts of the join condition to its inputs. */
    public static class JoinConditionPushRule extends org.apache.calcite.rel.rules.FilterJoinRule {
        /**
         * Creates a converter.
         */
        public JoinConditionPushRule() {
            super(RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
                "FilterJoinRule:no-filter", true, RelFactories.LOGICAL_BUILDER,
                org.apache.calcite.rel.rules.FilterJoinRule.TRUE_PREDICATE);
        }

        /** {@inheritDoc} */
        @Override public void onMatch(RelOptRuleCall call) {
            Join join = call.rel(0);
            perform(call, null, join);
        }
    }
}

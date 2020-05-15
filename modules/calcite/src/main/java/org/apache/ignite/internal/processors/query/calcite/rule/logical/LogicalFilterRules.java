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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;

/**
 *
 */
public final class LogicalFilterRules {
    /** */
    public static final RelOptRule FILTER_MERGE_RULE = new LogicalFilterMergeRule();

    /** */
    public static final RelOptRule FILTER_PROJECT_TRANSPOSE_RULE =
        new FilterProjectTransposeRule(LogicalFilter.class, LogicalProject.class, true, true,
            RelFactories.LOGICAL_BUILDER);

    /**
     *
     */
    private static class LogicalFilterMergeRule extends RelOptRule {
        /** */
        public LogicalFilterMergeRule() {
            super(
                operand(LogicalFilter.class,
                    operand(LogicalFilter.class, any())),
                RelFactories.LOGICAL_BUILDER, null);
        }

        /** {@inheritDoc} */
        @Override public void onMatch(RelOptRuleCall call) {
            FilterMergeRule.INSTANCE.onMatch(call);
        }
    }

    /** */
    private LogicalFilterRules() {
        // No-op.
    }
}

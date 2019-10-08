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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalFilter;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RelOp;

/**
 *
 */
public class IgniteFilterRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new IgniteFilterRule();

    private IgniteFilterRule() {
        super(Commons.any(LogicalFilter.class, RelNode.class), RelFactories.LOGICAL_BUILDER, "IgniteFilterRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RelNode input = filter.getInput();

        final RelTraitSet traitSet = input.getTraitSet().replace(IgniteRel.LOGICAL_CONVENTION);

        RelNode converted = convert(input, traitSet);

        RelOp<LogicalFilter, Boolean> transformOp = Commons.transformSubset(call, converted, this::newFilter);

        if (!transformOp.go(filter))
            call.transformTo(newFilter(filter, converted));
    }

    public IgniteLogicalFilter newFilter(LogicalFilter filter, RelNode input) {
        RelTraitSet traits = filter.getTraitSet()
            .replace(IgniteRel.LOGICAL_CONVENTION)
            .replace(IgniteMdDistribution.filter(filter.getCluster().getMetadataQuery(), input, filter.getCondition()));

        return new IgniteLogicalFilter(filter.getCluster(), traits, input, filter.getCondition(), filter.getVariablesSet());
    }
}

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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;

/**
 *
 */
public class FilterConverterRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new FilterConverterRule();

    /** */
    public FilterConverterRule() {
        super(operand(LogicalFilter.class, any()));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalFilter rel = call.rel(0);

        RelOptCluster cluster = rel.getCluster();
        RelNode input = convert(rel.getInput(), IgniteConvention.INSTANCE);
        RelTraitSet traits = rel.getTraitSet()
            .replace(IgniteConvention.INSTANCE);

        RuleUtils.transformTo(call,
            new IgniteFilter(cluster, traits, input, rel.getCondition()));
    }
}

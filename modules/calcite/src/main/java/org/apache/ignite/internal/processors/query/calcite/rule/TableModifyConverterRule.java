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
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;

/**
 *
 */
public class TableModifyConverterRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new TableModifyConverterRule();

    /**
     * Creates a ConverterRule.
     */
    public TableModifyConverterRule() {
        super(operand(LogicalTableModify.class, any()));
    }

    @Override public void onMatch(RelOptRuleCall call) {
        LogicalTableModify rel = call.rel(0);

        RelOptCluster cluster = rel.getCluster();

        RelNode input = convert(rel.getInput(), IgniteConvention.INSTANCE);

        input = RuleUtils.changeTraits(input, IgniteDistributions.single());

        RelTraitSet traits = rel.getTraitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(IgniteDistributions.single());

        RuleUtils.transformTo(call,
            new IgniteTableModify(cluster, traits, rel.getTable(), rel.getCatalogReader(), input,
            rel.getOperation(), rel.getUpdateColumnList(), rel.getSourceExpressionList(), rel.isFlattened()));
    }
}

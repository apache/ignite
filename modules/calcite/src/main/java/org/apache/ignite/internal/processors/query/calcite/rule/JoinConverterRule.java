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
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;

/**
 * Ignite Join converter.
 */
public class JoinConverterRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new JoinConverterRule();

    /**
     * Creates a converter.
     */
    public JoinConverterRule() {
        super(operand(LogicalJoin.class, any()));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalJoin rel = call.rel(0);

        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = cluster.traitSet()
            .replace(IgniteConvention.INSTANCE);
        RelNode left = convert(rel.getLeft(), traits);
        RelNode right = convert(rel.getRight(), traits);

        RuleUtils.transformTo(call,
            new IgniteJoin(cluster, traits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType()));
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;

/**
 *
 */
public class JoinTraitsPropagationRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new JoinTraitsPropagationRule();

    public JoinTraitsPropagationRule() {
        super(operand(IgniteJoin.class, operand(RelSubset.class, any())));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteJoin proto = call.rel(0);

        RelNode left = proto.getLeft();
        RelNode right = proto.getRight();

        RelOptCluster cluster = proto.getCluster();
        RexNode condition = proto.getCondition();
        Set<CorrelationId> variablesSet = proto.getVariablesSet();
        JoinRelType joinType = proto.getJoinType();

        List<IgniteDistributions.BiSuggestion> suggests = IgniteDistributions.suggestJoin(
            left, right, proto.analyzeCondition(), joinType);

        List<RelNode> newRels = new ArrayList<>(suggests.size());

        for (IgniteDistributions.BiSuggestion suggest : suggests) {
            RelTraitSet traits = proto.getTraitSet().replace(suggest.out());

            RelNode left0 = RuleUtils.changeTraits(left, suggest.left());
            RelNode right0 = RuleUtils.changeTraits(right, suggest.right());

            newRels.add(new IgniteJoin(cluster, traits, left0, right0,
                condition, variablesSet, joinType));
        }

        RuleUtils.transformTo(call, newRels);
    }
}

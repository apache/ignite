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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
        super(RuleUtils.traitPropagationOperand(IgniteJoin.class));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteJoin rel = call.rel(0);

        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = call.getMetadataQuery();

        RexNode condition = rel.getCondition();
        Set<CorrelationId> variablesSet = rel.getVariablesSet();
        JoinRelType joinType = rel.getJoinType();

        List<IgniteDistributions.BiSuggestion> suggestions = IgniteDistributions.suggestJoin(
            mq, left, right, rel.analyzeCondition(), joinType);

        List<RelNode> newRels = new ArrayList<>(suggestions.size());

        for (IgniteDistributions.BiSuggestion suggestion : suggestions) {
            RelTraitSet traits = rel.getTraitSet().replace(suggestion.out());

            RelNode left0 = convert(left, suggestion.left());
            RelNode right0 = convert(right, suggestion.right());

            newRels.add(new IgniteJoin(cluster, traits, left0, right0,
                condition, variablesSet, joinType));
        }

        RuleUtils.transformTo(call, newRels);
    }
}

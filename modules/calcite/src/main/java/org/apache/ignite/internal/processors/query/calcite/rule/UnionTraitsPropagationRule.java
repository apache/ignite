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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.Suggestion;

/**
 *
 */
public class UnionTraitsPropagationRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new UnionTraitsPropagationRule();

    /** */
    public UnionTraitsPropagationRule() {
        super(RuleUtils.traitPropagationOperand(IgniteUnionAll.class));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteUnionAll rel = call.rel(0);
        List<RelNode> inputs = rel.getInputs();

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = call.getMetadataQuery();

        List<Suggestion> suggestions =
            IgniteDistributions.suggestUnionAll(mq, inputs);

        List<RelNode> newRels = new ArrayList<>(suggestions.size());

        for (Suggestion suggestion : suggestions) {
            RelTraitSet traits = rel.getTraitSet().replace(suggestion.out());
            List<RelNode> inputs0 = new ArrayList<>(inputs.size());

            for (RelNode input : inputs)
                inputs0.add(convert(input, suggestion.in()));

            newRels.add(new IgniteUnionAll(cluster, traits, inputs0));
        }

        RuleUtils.transformTo(call, newRels);
    }
}

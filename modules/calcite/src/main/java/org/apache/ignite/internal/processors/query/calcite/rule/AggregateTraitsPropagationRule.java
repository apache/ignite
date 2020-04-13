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
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregate;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class AggregateTraitsPropagationRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new AggregateTraitsPropagationRule();

    /** */
    public AggregateTraitsPropagationRule() {
        super(RuleUtils.traitPropagationOperand(IgniteAggregate.class));
    }

    /** */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteAggregate rel = call.rel(0);
        RelNode input = call.rel(1);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        ImmutableBitSet groupSet = rel.getGroupSet();
        List<ImmutableBitSet> groupSets = rel.getGroupSets();
        List<AggregateCall> aggCalls = rel.getAggCallList();

        IgniteDistribution inDistr = IgniteMdDistribution._distribution(input, mq);

        List<IgniteDistributions.Suggestion> suggestions =
            IgniteDistributions.suggestAggregate(F.asList(inDistr), groupSet, groupSets);

        List<RelNode> newRels = new ArrayList<>(suggestions.size());

        for (IgniteDistributions.Suggestion suggestion : suggestions) {
            RelTraitSet traits = rel.getTraitSet().replace(suggestion.out());
            RelNode input0 = RuleUtils.changeTraits(input, suggestion.in());

            if (isMapReduce(suggestion)) {
                RelTraitSet mapTraits = input0.getTraitSet()
                    .replace(IgniteDistributions.mapAggregate(mq, input0, groupSet, groupSets, aggCalls));

                input0 = new IgniteMapAggregate(cluster, mapTraits, input0, groupSet, groupSets, aggCalls);
                input0 = RuleUtils.changeTraits(input0, suggestion.out());

                newRels.add(new IgniteReduceAggregate(cluster, traits, input0, groupSet, groupSets, aggCalls, rel.getRowType()));
            }
            else
                newRels.add(new IgniteAggregate(cluster, traits, input0, groupSet, groupSets, aggCalls));
        }

        RuleUtils.transformTo(call, newRels);
    }

    /** */
    private boolean isMapReduce(IgniteDistributions.Suggestion suggestion) {
        return Objects.equals(suggestion.out(), IgniteDistributions.single())
            && suggestion.in().satisfies(IgniteDistributions.random());
    }
}

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;

/**
 *
 */
public class FilterTraitsPropagationRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new FilterTraitsPropagationRule();

    public FilterTraitsPropagationRule() {
        super(operand(IgniteFilter.class, operand(RelSubset.class, any())));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteFilter rel = call.rel(0);
        RelNode input = call.rel(1);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQueryEx mq = (RelMetadataQueryEx)cluster.getMetadataQuery();
        Set<RelTraitSet> allTraitSets = mq.deriveTraitSets(rel);
        Set<RelTraitSet> physicalTraitSets = new HashSet<>();
        for (RelTraitSet set : allTraitSets) {
            RelTraitSet physSet = set.replace(IgniteConvention.INSTANCE);
            physicalTraitSets.add(physSet);
        }

        List<RelNode> newRels = new ArrayList<>(physicalTraitSets.size());
        for (RelTraitSet traits : physicalTraitSets) {
            RelNode newInput = convert(input, traits);
            IgniteFilter newFilter = new IgniteFilter(cluster, traits, newInput, rel.getCondition());
            newRels.add(newFilter);
        }

        RuleUtils.transformTo(call, newRels);
    }
}

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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class IgniteHashJoinRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new IgniteHashJoinRule();

    public IgniteHashJoinRule() {
        super(Commons.any(LogicalJoin.class, RelNode.class), RelFactories.LOGICAL_BUILDER, "IgniteJoinRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RelTraitSet leftTraits = join.getLeft().getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION)
            .replace(IgniteDistributions.hash(join.analyzeCondition().leftKeys, IgniteDistributions.noOpFunction()));

        RelTraitSet rightTraits = join.getRight().getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION)
            .replace(IgniteDistributions.hash(join.analyzeCondition().rightKeys, IgniteDistributions.noOpFunction()));

        RelNode left = convert(join.getLeft(), leftTraits);
        RelNode right = convert(join.getRight(), rightTraits);

        RelMetadataQuery mq = call.getMetadataQuery();

        RelTraitSet traitSet = join.getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION)
            .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.join(mq, left, right, join.getCondition()));

        call.transformTo(new IgniteHashJoin(join.getCluster(), traitSet, left, right,
            join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone()));
    }
}

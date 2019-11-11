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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
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
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.DistributionType.BROADCAST;
import static org.apache.ignite.internal.processors.query.calcite.trait.DistributionType.HASH;
import static org.apache.ignite.internal.processors.query.calcite.trait.DistributionType.SINGLE;

/**
 *
 */
public class IgniteHashJoinRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new IgniteHashJoinRule();

    public IgniteHashJoinRule() {
        super(Commons.any(LogicalJoin.class, RelNode.class), RelFactories.LOGICAL_BUILDER, "IgniteHashJoinRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RelTraitSet leftTraits = join.getLeft().getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION);

        RelTraitSet rightTraits = join.getRight().getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION);

        RelNode left = convert(join.getLeft(), leftTraits);
        RelNode right = convert(join.getRight(), rightTraits);

        RelMetadataQuery mq = call.getMetadataQuery();

        List<DistributionTrait> leftDerived;
        List<DistributionTrait> rightDerived;

        if ((leftDerived = IgniteDistributions.deriveDistributions(left, mq)).isEmpty()
            || (rightDerived = IgniteDistributions.deriveDistributions(right, mq)).isEmpty()) {
            call.transformTo(join.copy(join.getTraitSet(), ImmutableList.of(left, right)));

            return;
        }

        List<DistributionTrait> leftDists = Commons.concat(leftDerived,
            IgniteDistributions.hash(join.analyzeCondition().leftKeys, IgniteDistributions.hashFunction()));

        List<DistributionTrait> rightDists = Commons.concat(rightDerived,
            IgniteDistributions.hash(join.analyzeCondition().rightKeys, IgniteDistributions.hashFunction()));

        for (DistributionTrait leftDist0 : leftDists) {
            for (DistributionTrait rightDist0 : rightDists) {
                if (canTransform(join, leftDist0, rightDist0))
                    transform(call, join, mq, leftDist0, rightDist0);
            }
        }
    }

    private void transform(RelOptRuleCall call, LogicalJoin join, RelMetadataQuery mq, DistributionTrait leftDist, DistributionTrait rightDist) {
        RelTraitSet leftTraits = join.getLeft().getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION)
            .replace(leftDist);

        RelTraitSet rightTraits = join.getRight().getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION)
            .replace(rightDist);

        RelNode left = convert(join.getLeft(), leftTraits);
        RelNode right = convert(join.getRight(), rightTraits);

        RelTraitSet traitSet = join.getTraitSet()
            .replace(IgniteRel.IGNITE_CONVENTION)
            .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.join(mq, left, right, join.getCondition()));

        call.transformTo(new IgniteHashJoin(join.getCluster(), traitSet, left, right,
            join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone()));
    }

    private boolean canTransform(LogicalJoin join, DistributionTrait leftDist, DistributionTrait rightDist) {
        if (leftDist.type() == BROADCAST
            && rightDist.type() == BROADCAST)
            return true;

        if (rightDist.type() == SINGLE
            && leftDist.type() == SINGLE)
            return true;

        if (leftDist.type() == BROADCAST
            && rightDist.type() == HASH
            && Objects.equals(rightDist.keys(), join.analyzeCondition().rightKeys))
            return true;

        if (rightDist.type() == BROADCAST
            && leftDist.type() == HASH
            && Objects.equals(leftDist.keys(), join.analyzeCondition().leftKeys))
            return true;

        if (leftDist.type() == HASH
            && rightDist.type() == HASH
            && Objects.equals(leftDist.keys(), join.analyzeCondition().leftKeys)
            && Objects.equals(rightDist.keys(), join.analyzeCondition().rightKeys)
            && Objects.equals(rightDist.destinationFunctionFactory(), leftDist.destinationFunctionFactory()))
            return true;

        return false;
    }
}

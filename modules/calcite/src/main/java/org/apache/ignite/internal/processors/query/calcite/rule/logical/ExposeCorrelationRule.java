/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.Collections;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;

public class ExposeCorrelationRule extends RelRule<ExposeCorrelationRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    public ExposeCorrelationRule(ExposeCorrelationRule.Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalCorrelate rel = call.rel(0);
        final RelOptCluster cluster = rel.getCluster();

        JoinRelType joinType = rel.getJoinType();

        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);

        Set<CorrelationId> corrIds = Collections.singleton(rel.getCorrelationId());

        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(RewindabilityTrait.REWINDABLE)
            .replace(CorrelationTrait.correlations(corrIds))/*.replace(IgniteDistributions.single())*/;

        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        call.transformTo(
            new IgniteCorrelatedNestedLoopJoin(
                cluster,
                outTraits,
                left,
                right,
                cluster.getRexBuilder().makeLiteral(true),
                corrIds,
                joinType
            )
        );
    }

    public interface Config extends RelRule.Config {
        /** */
        ExposeCorrelationRule.Config DEFAULT = RelRule.Config.EMPTY
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .withDescription("CorrRule")
            .as(ExposeCorrelationRule.Config.class)
            .withOperandFor(LogicalCorrelate.class);

        /** Defines an operand tree for the given classes. */
        default ExposeCorrelationRule.Config withOperandFor(Class<? extends Correlate> corr) {
            return withOperandSupplier(
                o0 -> o0.operand(corr).anyInputs()
            ).as(ExposeCorrelationRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override default ExposeCorrelationRule toRule() {
            return new ExposeCorrelationRule(this);
        }
    }
}

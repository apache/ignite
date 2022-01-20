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

import java.util.Collections;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;

/**
 * Rule to convert LogicalCorrelate operator to IgniteCorrelatedNestedLoopJoin.
 *
 * LogicalCorrelate operators are created during sub-query rewriting. In most cases LogicalCorrelate can be further
 * converted to other logical operators if decorrelation is enabled, but in some cases query can't
 * be decorrelated (when table function is used for example), this rule is required to support such cases.
 */
public class CorrelateToNestedLoopRule extends AbstractIgniteConverterRule<LogicalCorrelate> {
    /** */
    public static final RelOptRule INSTANCE = new CorrelateToNestedLoopRule();

    /** */
    public CorrelateToNestedLoopRule() {
        super(LogicalCorrelate.class, "CorrelateToNestedLoopRule");
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalCorrelate rel) {
        final RelOptCluster cluster = rel.getCluster();

        final Set<CorrelationId> correlationIds = Collections.singleton(rel.getCorrelationId());
        CorrelationTrait corrTrait = CorrelationTrait.correlations(correlationIds);

        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(RewindabilityTrait.REWINDABLE)
            .replace(corrTrait);

        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        return
            new IgniteCorrelatedNestedLoopJoin(
                cluster,
                outTraits,
                left,
                right,
                cluster.getRexBuilder().makeLiteral(true),
                correlationIds,
                rel.getJoinType()
            );
    }

    /** {@inheritDoc} */
    @Override public boolean matches(RelOptRuleCall call) {
        Correlate corr = call.rel(0);
        // Only these join types are currently supported by IgniteCorrelatedNestedLoopJoin
        // and only these types are used to rewrite sub-query.
        return corr.getJoinType() == JoinRelType.INNER || corr.getJoinType() == JoinRelType.LEFT;
    }
}

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

import java.util.EnumSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoinInfo;

/** Hash join converter rule. */
public class HashJoinConverterRule extends AbstractIgniteJoinConverterRule {
    /** */
    public static final RelOptRule INSTANCE = new HashJoinConverterRule();

    /** */
    private static final EnumSet<JoinRelType> NON_EQ_CONDITIONS_SUPPORT = EnumSet.of(JoinRelType.INNER, JoinRelType.SEMI);

    /** Ctor. */
    private HashJoinConverterRule() {
        super("HashJoinConverter", HintDefinition.HASH_JOIN);
    }

    /** {@inheritDoc} */
    @Override public boolean matchesJoin(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        IgniteJoinInfo joinInfo = IgniteJoinInfo.of(join);

        if (joinInfo.pairs().isEmpty())
            return false;

        // IS NOT DISTINCT is currently not supported simultaneously with equi conditions.
        if (joinInfo.hasMatchingNulls() && joinInfo.matchingNullsCnt() != joinInfo.pairs().size())
            return false;

        // Current limitation: unmatched products on left or right part requires special handling of non-equi condition
        // on execution level.
        return joinInfo.isEqui() || NON_EQ_CONDITIONS_SUPPORT.contains(join.getJoinType());
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        return new IgniteHashJoin(cluster, outTraits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }
}

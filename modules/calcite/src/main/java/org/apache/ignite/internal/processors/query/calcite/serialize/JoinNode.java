/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;

/**
 *
 */
public class JoinNode extends RelGraphNode {
    private final LogicalExpression condition;
    private final int[] variables;
    private final JoinRelType joinType;
    private final boolean semiDone;

    private JoinNode(RelTraitSet traits, LogicalExpression condition, int[] variables, JoinRelType joinType, boolean semiDone) {
        super(traits);
        this.condition = condition;
        this.variables = variables;
        this.joinType = joinType;
        this.semiDone = semiDone;
    }

    public static JoinNode create(IgniteJoin rel, RexToExpTranslator expTranslator) {
        return new JoinNode(rel.getTraitSet(),
            expTranslator.translate(rel.getCondition()),
            rel.getVariablesSet().stream().mapToInt(CorrelationId::getId).toArray(),
            rel.getJoinType(),
            rel.isSemiJoinDone());
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        assert children.size() == 2;

        RelNode left = children.get(0);
        RelNode right = children.get(1);

        return new IgniteJoin(ctx.cluster(),
            traitSet.toTraitSet(ctx.cluster()),
            left,
            right,
            ctx.expressionTranslator().translate(condition),
            Arrays.stream(variables).mapToObj(CorrelationId::new).collect(Collectors.toSet()),
            joinType,
            semiDone);
    }
}
